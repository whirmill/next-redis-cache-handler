/**
 * @whirmill/next-redis-cache-handler 1.2.0
 * Redis cache handler for Next.js 14, 15 and 16 (App Router, with Pages Router support).
 *
 * Behaviour
 * - Build (`next build`): never connects to Redis. Prerendered output is
 *   written to `.next` by Next.js and kept in memory for the build only.
 * - Runtime: Redis is the shared cache, in two keyspaces:
 *   - pages and route handlers under `<namespace>:<buildId>:`: their HTML and
 *     RSC payloads reference the build's own assets, so builds never share them;
 *   - data (fetch cache, `kind: "FETCH"`) under `<namespace>:data:`: API
 *     responses do not depend on the build, so they stay warm across deploys.
 *   If Redis is unavailable the handler serves from an in-process memory cache
 *   and reconnects in the background.
 * - Startup: `registerInitialCache` (called from `instrumentation.ts`) copies
 *   the build output into Redis without overwriting entries that already exist
 *   for the same build.
 * - Every key and tag index expires after at most NEXT_CACHE_MAX_TTL_SECONDS,
 *   so entries of old builds and unused data disappear by themselves.
 *
 * Environment
 * - REDIS_PRIVATE_URL, then REDIS_URL: Redis connection URL.
 * - NEXT_CACHE_NAMESPACE: key namespace (default: slug of RAILWAY_SERVICE_NAME, else "next").
 * - NEXT_CACHE_MAX_TTL_SECONDS: maximum key lifetime (default 1209600, 14 days).
 * - NEXT_CACHE_DEFAULT_TTL_SECONDS: lifetime of entries without `revalidate`
 *   (default unset: they stay until the maximum lifetime, as in Next.js).
 * - NEXT_CACHE_REDIS_TIMEOUT_MS: timeout of each Redis command (default 1000).
 * - NEXT_CACHE_MEMORY_MAX_ENTRIES: size of the in-memory fallback (default 5000).
 * - NEXT_PRIVATE_DEBUG_CACHE: verbose logging when set.
 *
 * Usage: see README.md.
 */
import { promises as fsPromises } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { commandOptions, createClient } from "redis";

const PRERENDER_MANIFEST_VERSION = 4;
const CACHE_ONE_YEAR = 31536000;
const DEFAULT_STALE_AGE_SECONDS = CACHE_ONE_YEAR;
const MAX_INT32 = 2 ** 31 - 1;
const NEXT_CACHE_IMPLICIT_TAG_ID = "_N_T_";
// Next.js 15.5.27+ and 16.4+ scope response cache keys by their source route:
// `/route-cache/<kind>/<sha256 of the route>/$<page path>`.
const ROUTE_CACHE_KEY_PREFIX = "/route-cache/";
const ROUTE_CACHE_PATH_MARKER = "/$/";
const SHARED_TAGS_KEY = "__sharedTags__";
const REVALIDATED_TAGS_KEY = "__revalidated_tags__";
const INDEX_TTL_REFRESH_MS = 60_000;
const INITIAL_CACHE_CONCURRENCY = 16;
// Shared by every copy of this module loaded in the same process.
const CONFIGURED_COPIES = Symbol.for("next-redis-cache-handler.configured-copies");

const DEBUG_CACHE = typeof process.env.NEXT_PRIVATE_DEBUG_CACHE !== "undefined";

function readPositiveInt(name, fallback) {
  const parsed = Number.parseInt(process.env[name] ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const MAX_TTL_SECONDS = readPositiveInt("NEXT_CACHE_MAX_TTL_SECONDS", 60 * 60 * 24 * 14);
const DEFAULT_TTL_SECONDS = readPositiveInt("NEXT_CACHE_DEFAULT_TTL_SECONDS", 0);
const REDIS_TIMEOUT_MS = readPositiveInt("NEXT_CACHE_REDIS_TIMEOUT_MS", 1000);
const MEMORY_MAX_ENTRIES = readPositiveInt("NEXT_CACHE_MEMORY_MAX_ENTRIES", 5000);

function log(message) {
  console.info(`[cache-handler] ${message}`);
}

function debugWarn(...args) {
  if (DEBUG_CACHE) console.warn("[cache-handler]", ...args);
}

function isBuildPhase() {
  return process.env.NEXT_PHASE === "phase-production-build";
}

function slug(value) {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function cacheNamespace() {
  return slug(process.env.NEXT_CACHE_NAMESPACE) || slug(process.env.RAILWAY_SERVICE_NAME) || "next";
}

/**
 * Cache key and file name Next.js uses for a prerendered route
 * (normalizePagePath): "/" is stored and looked up as "/index".
 */
function normalizePagePath(route) {
  if (route === "/") return "/index";
  if (/^\/index(\/|$)/.test(route) && !/\[.+?\]/.test(route)) return `/index${route}`;
  return route;
}

function isImplicitTag(tag) {
  return typeof tag === "string" && tag.startsWith(NEXT_CACHE_IMPLICIT_TAG_ID);
}

function getTagsFromHeaders(headers) {
  const tagsHeader = headers?.["x-next-cache-tags"];

  if (Array.isArray(tagsHeader)) {
    return tagsHeader;
  }

  if (typeof tagsHeader === "string") {
    return tagsHeader.split(",");
  }

  return [];
}

/**
 * Page path of a response cache key: the key itself before Next.js 15.5.27,
 * the part after `/$` for route-scoped keys.
 */
function pagePathOfKey(key) {
  if (!key.startsWith(ROUTE_CACHE_KEY_PREFIX)) return key;
  const marker = key.indexOf(ROUTE_CACHE_PATH_MARKER);
  return marker === -1 ? key : key.slice(marker + 2);
}

/** Next.js 14 marks data cache reads with `kindHint: "fetch"`, Next.js 15 with `kind: "FETCH"`. */
function isFetchRead(meta) {
  return meta?.kind === "FETCH" || meta?.kindHint === "fetch";
}

const RESPONSE_KINDS = new Set(["PAGE", "ROUTE", "PAGES", "APP_PAGE", "APP_ROUTE"]);

/**
 * `revalidate` of an entry in seconds, or false when it never goes stale.
 * Next.js 14 passes it in `ctx.revalidate`; Next.js 15 passes `ctx.cacheControl`
 * for pages and route handlers and keeps it on the value for fetch entries.
 */
function revalidateOf(ctx, value) {
  const revalidate =
    ctx.revalidate ?? ctx.cacheControl?.revalidate ?? (value?.kind === "FETCH" ? value.revalidate : undefined);
  return revalidate === CACHE_ONE_YEAR ? false : revalidate;
}

/**
 * Makes a cache value JSON-safe: route bodies, RSC payloads and segment
 * prefetches are Buffers (a Map of Buffers for segments) in Next.js 15.
 */
function serializeValue(value) {
  switch (value?.kind) {
    case "ROUTE":
    case "APP_ROUTE":
      return { ...value, body: Buffer.from(value.body).toString("base64") };
    case "APP_PAGE":
      return {
        ...value,
        rscData: value.rscData === undefined ? undefined : Buffer.from(value.rscData).toString("base64"),
        segmentData:
          value.segmentData === undefined
            ? undefined
            : Object.fromEntries(
                Array.from(value.segmentData, ([segment, data]) => [segment, Buffer.from(data).toString("base64")])
              ),
      };
    default:
      return value;
  }
}

function deserializeValue(value) {
  switch (value?.kind) {
    case "ROUTE":
    case "APP_ROUTE":
      return { ...value, body: Buffer.from(value.body, "base64") };
    case "APP_PAGE":
      return {
        ...value,
        rscData: typeof value.rscData === "string" ? Buffer.from(value.rscData, "base64") : undefined,
        segmentData: value.segmentData
          ? new Map(Object.entries(value.segmentData).map(([segment, data]) => [segment, Buffer.from(data, "base64")]))
          : undefined,
      };
    default:
      return value;
  }
}

/** Major version of the `next` package the app resolves, or undefined. */
function nextMajorVersion() {
  try {
    const require = createRequire(path.join(process.cwd(), "package.json"));
    return Number.parseInt(require("next/package.json").version, 10) || undefined;
  } catch {
    return undefined;
  }
}

function getLifespanParameters(lastModified, revalidate, defaultTtlSeconds = DEFAULT_TTL_SECONDS) {
  const lastModifiedAt = Math.floor(lastModified / 1000);
  // Entries without `revalidate` expire after the configured default lifetime, if any.
  const useDefaultTtl = !revalidate && defaultTtlSeconds > 0;
  const staleAge = useDefaultTtl ? defaultTtlSeconds : revalidate || DEFAULT_STALE_AGE_SECONDS;
  const expireAge = useDefaultTtl ? defaultTtlSeconds : Math.min(Math.floor(staleAge * 1.5), MAX_INT32);

  return {
    lastModifiedAt,
    staleAt: lastModifiedAt + staleAge,
    expireAt: lastModifiedAt + expireAge,
    staleAge,
    expireAge,
    revalidate,
  };
}

/** Seconds a key may live: until its lifespan ends, never longer than the cap. */
function redisTtlSeconds(cacheValue, maxTtlSeconds) {
  const expireAt = cacheValue?.lifespan?.expireAt;
  if (typeof expireAt !== "number") return maxTtlSeconds;
  return Math.min(expireAt - Math.floor(Date.now() / 1000), maxTtlSeconds);
}

function timeout(timeoutMs) {
  return commandOptions({ signal: AbortSignal.timeout(timeoutMs) });
}

export { deserializeValue, getLifespanParameters, serializeValue };

export function createMemoryHandler({ maxEntries = MEMORY_MAX_ENTRIES } = {}) {
  const store = new Map();
  const tagsByKey = new Map();
  const keysByTag = new Map();
  const revalidatedTagsAt = new Map();

  function deleteKey(key) {
    store.delete(key);

    const tags = tagsByKey.get(key);
    if (!tags) return;

    for (const tag of tags) {
      const keys = keysByTag.get(tag);
      if (!keys) continue;
      keys.delete(key);
      if (keys.size === 0) keysByTag.delete(tag);
    }

    tagsByKey.delete(key);
  }

  return {
    name: "memory-lru",
    async get(key, { implicitTags = [] } = {}) {
      const entry = store.get(key);
      if (!entry) return null;

      // LRU bump
      store.delete(key);
      store.set(key, entry);

      for (const tag of new Set([...(entry.tags || []), ...implicitTags])) {
        const invalidatedAt = revalidatedTagsAt.get(tag);
        if (typeof invalidatedAt === "number" && invalidatedAt > entry.lastModified) {
          deleteKey(key);
          return null;
        }
      }

      return entry;
    },
    async set(key, cacheValue, { onlyIfAbsent = false } = {}) {
      if (onlyIfAbsent && store.has(key)) return false;

      deleteKey(key);
      store.set(key, cacheValue);

      const tags = Array.isArray(cacheValue?.tags) ? cacheValue.tags : [];
      tagsByKey.set(key, new Set(tags));

      for (const tag of tags) {
        const keys = keysByTag.get(tag) ?? new Set();
        keys.add(key);
        keysByTag.set(tag, keys);
      }

      while (store.size > maxEntries) {
        const oldest = store.keys().next();
        if (oldest.done) break;
        deleteKey(oldest.value);
      }

      return true;
    },
    async revalidateTag(tag) {
      revalidatedTagsAt.set(tag, Date.now());

      for (const key of keysByTag.get(tag) ?? []) {
        deleteKey(key);
      }
    },
    async delete(key) {
      deleteKey(key);
    },
  };
}

export function createRedisStringsHandler({
  client,
  keyPrefix,
  maxTtlSeconds = MAX_TTL_SECONDS,
  timeoutMs = REDIS_TIMEOUT_MS,
  revalidateTagQuerySize = 100,
}) {
  const sharedTagsKey = `${keyPrefix}${SHARED_TAGS_KEY}`;
  const revalidatedTagsKey = `${keyPrefix}${REVALIDATED_TAGS_KEY}`;
  let indexTtlRefreshedAt = 0;

  function assertClientIsReady() {
    if (!client.isReady) {
      throw new Error("Redis client is not ready");
    }
  }

  // The tag indexes live as long as the build keeps writing to them.
  async function refreshIndexTtl() {
    if (Date.now() - indexTtlRefreshedAt < INDEX_TTL_REFRESH_MS) return;
    indexTtlRefreshedAt = Date.now();
    await Promise.all([
      client.expire(timeout(timeoutMs), sharedTagsKey, maxTtlSeconds),
      client.expire(timeout(timeoutMs), revalidatedTagsKey, maxTtlSeconds),
    ]);
  }

  return {
    name: "redis-strings",
    async get(key, { implicitTags = [] } = {}) {
      assertClientIsReady();

      const raw = await client.get(timeout(timeoutMs), `${keyPrefix}${key}`);
      if (!raw) return null;

      const cacheValue = JSON.parse(raw);
      if (!cacheValue) return null;

      const combinedTags = new Set([...(cacheValue.tags || []), ...implicitTags]);
      if (combinedTags.size === 0) return cacheValue;

      const revalidationTimes = await client.hmGet(
        timeout(timeoutMs),
        revalidatedTagsKey,
        Array.from(combinedTags)
      );

      for (const timestamp of revalidationTimes) {
        const revalidatedAt = Number.parseInt(timestamp ?? "", 10);
        if (Number.isFinite(revalidatedAt) && revalidatedAt > cacheValue.lastModified) {
          await client.unlink(timeout(timeoutMs), `${keyPrefix}${key}`);
          return null;
        }
      }

      return cacheValue;
    },
    async set(key, cacheValue, { onlyIfAbsent = false } = {}) {
      assertClientIsReady();

      const ttlSeconds = redisTtlSeconds(cacheValue, maxTtlSeconds);
      if (ttlSeconds <= 0) return false;

      const written = await client.set(
        timeout(timeoutMs),
        `${keyPrefix}${key}`,
        JSON.stringify(cacheValue),
        onlyIfAbsent ? { EX: ttlSeconds, NX: true } : { EX: ttlSeconds }
      );
      if (written === null) return false;

      if (Array.isArray(cacheValue?.tags) && cacheValue.tags.length > 0) {
        await client.hSet(timeout(timeoutMs), sharedTagsKey, key, JSON.stringify(cacheValue.tags));
      } else {
        await client.hDel(timeout(timeoutMs), sharedTagsKey, key);
      }

      await refreshIndexTtl();
      return true;
    },
    async revalidateTag(tag) {
      assertClientIsReady();

      // Kept for every tag, not only implicit ones: entries written again from
      // the build output after a restart are older than this and get rejected.
      await client.hSet(timeout(timeoutMs), revalidatedTagsKey, tag, String(Date.now()));
      await client.expire(timeout(timeoutMs), revalidatedTagsKey, maxTtlSeconds);

      const keysToDelete = [];
      const fieldsToDelete = [];
      let cursor = 0;

      do {
        const portion = await client.hScan(timeout(timeoutMs), sharedTagsKey, cursor, {
          COUNT: revalidateTagQuerySize,
        });

        for (const { field, value } of portion.tuples) {
          const tags = JSON.parse(value);
          if (Array.isArray(tags) && tags.includes(tag)) {
            keysToDelete.push(`${keyPrefix}${field}`);
            fieldsToDelete.push(field);
          }
        }

        cursor = portion.cursor;
      } while (cursor !== 0);

      if (keysToDelete.length === 0) return;

      await Promise.all([
        client.unlink(timeout(timeoutMs), keysToDelete),
        client.hDel(timeout(timeoutMs), sharedTagsKey, fieldsToDelete),
      ]);
    },
    async delete(key) {
      assertClientIsReady();
      await Promise.all([
        client.unlink(timeout(timeoutMs), `${keyPrefix}${key}`),
        client.hDel(timeout(timeoutMs), sharedTagsKey, key),
      ]);
    },
  };
}

/**
 * Sends data (fetch cache) entries to `dataHandler` and pages and route
 * handlers to `pagesHandler`. Next.js marks fetch-cache reads with
 * `kind: "FETCH"` (`kindHint: "fetch"` in Next.js 14) and stores them with
 * `kind: "FETCH"`; tag revalidation and deletes apply to both keyspaces.
 */
export function createKindRouter({ pagesHandler, dataHandler }) {
  const forRead = (meta) => (isFetchRead(meta) ? dataHandler : pagesHandler);
  const forValue = (cacheValue) => (cacheValue?.value?.kind === "FETCH" ? dataHandler : pagesHandler);

  return {
    name: "redis-pages-and-data",
    async get(key, meta) {
      return forRead(meta).get(key, meta);
    },
    async set(key, cacheValue, options) {
      return forValue(cacheValue).set(key, cacheValue, options);
    },
    async revalidateTag(tag) {
      await Promise.all([pagesHandler.revalidateTag(tag), dataHandler.revalidateTag(tag)]);
    },
    async delete(key) {
      await Promise.all([pagesHandler.delete(key), dataHandler.delete(key)]);
    },
  };
}

/**
 * Uses Redis while the client is connected and the in-memory handler while it
 * is not. Entries are never written to both, so another replica's
 * revalidation cannot leave a stale copy in this process's memory.
 */
export function createResilientHandler({ client, redisHandler, memoryHandler }) {
  const current = () => (client.isReady ? redisHandler : memoryHandler);

  return {
    name: "redis-with-memory-fallback",
    async get(key, meta) {
      return current().get(key, meta);
    },
    async set(key, cacheValue, options) {
      return current().set(key, cacheValue, options);
    },
    async revalidateTag(tag) {
      await memoryHandler.revalidateTag(tag);
      if (client.isReady) await redisHandler.revalidateTag(tag);
    },
    async delete(key) {
      await memoryHandler.delete(key);
      if (client.isReady) await redisHandler.delete(key);
    },
  };
}

async function connectRedis(url) {
  let host = "unknown host";
  try {
    host = new URL(url).host;
  } catch {
    // keep the placeholder; never log the URL, it carries the password
  }

  let wasReady = false;
  // Do not set disableOfflineQueue: together with the AbortSignal timeouts used
  // by every command, node-redis 4.7 throws in flushAll when the connection
  // drops and never reconnects. While disconnected the resilient handler does
  // not send commands to Redis anyway.
  const client = createClient({
    url,
    socket: {
      connectTimeout: 5000,
      reconnectStrategy: (retries) => Math.min(250 * 2 ** retries, 10_000),
    },
  });

  client.on("error", (error) => debugWarn("Redis client error:", error?.message || error));
  client.on("ready", () => {
    log(`Redis ready at ${host}`);
    wasReady = true;
  });
  client.on("end", () => log(`Redis connection to ${host} closed`));
  client.on("reconnecting", () => {
    if (wasReady) {
      log(`Redis connection to ${host} lost, reconnecting; using in-memory cache meanwhile`);
      wasReady = false;
    }
  });

  log(`connecting to Redis at ${host}`);
  const connecting = client.connect().catch((error) => {
    debugWarn("Redis connect failed:", error?.message || error);
  });
  await Promise.race([connecting, new Promise((resolve) => setTimeout(resolve, 5000).unref())]);

  if (!client.isReady) {
    log(`Redis at ${host} not reachable yet; using in-memory cache until it connects`);
  }

  return client;
}

async function removeEntryFromHandlers(handlers, key) {
  const operations = await Promise.allSettled(handlers.map((handler) => handler.delete?.(key)));

  operations.forEach((result, index) => {
    if (result.status === "rejected") {
      debugWarn(`[handler: ${handlers[index]?.name}] [method: delete] [key: ${key}]`, result.reason);
    }
  });
}

class CacheHandler {
  static #context;
  static #onCreationHook;
  static #configuring;
  static #mergedHandler;
  static #fallbackFalseRoutes = new Set();
  static #serverDistDir;

  static onCreation(onCreationHook) {
    CacheHandler.#onCreationHook = onCreationHook;
  }

  static async #readPagesRouterPage(pagePath, kind) {
    let pageHtmlHandle = null;

    try {
      const pageHtmlPath = path.join(CacheHandler.#serverDistDir, "pages", `${pagePath}.html`);
      const pageDataPath = path.join(CacheHandler.#serverDistDir, "pages", `${pagePath}.json`);

      pageHtmlHandle = await fsPromises.open(pageHtmlPath, "r");

      const [pageHtmlFile, { mtimeMs }, pageData] = await Promise.all([
        pageHtmlHandle.readFile("utf-8"),
        pageHtmlHandle.stat(),
        fsPromises.readFile(pageDataPath, "utf-8").then((data) => JSON.parse(data)),
      ]);

      return {
        lastModified: mtimeMs,
        lifespan: null,
        tags: [],
        value: {
          kind,
          html: pageHtmlFile,
          pageData,
          postponed: undefined,
          headers: undefined,
          status: undefined,
        },
      };
    } catch {
      return null;
    } finally {
      await pageHtmlHandle?.close();
    }
  }

  static async #writePagesRouterPage(pagePath, pageData) {
    try {
      const pageHtmlPath = path.join(CacheHandler.#serverDistDir, "pages", `${pagePath}.html`);
      const pageDataPath = path.join(CacheHandler.#serverDistDir, "pages", `${pagePath}.json`);

      await fsPromises.mkdir(path.dirname(pageHtmlPath), { recursive: true });
      await Promise.all([
        fsPromises.writeFile(pageHtmlPath, pageData.html),
        fsPromises.writeFile(pageDataPath, JSON.stringify(pageData.pageData)),
      ]);
    } catch (error) {
      debugWarn("Failed writing fallback:false page", error);
    }
  }

  static #configureCacheHandler() {
    CacheHandler.#configuring ??= CacheHandler.#doConfigure();
    return CacheHandler.#configuring;
  }

  static async #doConfigure() {
    const { serverDistDir, dev } = CacheHandler.#context;

    let buildId;
    try {
      buildId = (await fsPromises.readFile(path.join(serverDistDir, "..", "BUILD_ID"), "utf-8")).trim();
    } catch {
      buildId = undefined;
    }

    const config = await CacheHandler.#onCreationHook?.({ serverDistDir, dev, buildId });
    const handlers = (config?.handlers ?? []).filter(Boolean);

    if (handlers.length === 0) {
      handlers.push(createMemoryHandler());
    }

    CacheHandler.#serverDistDir = serverDistDir;

    try {
      const prerenderManifest = JSON.parse(
        await fsPromises.readFile(path.join(serverDistDir, "..", "prerender-manifest.json"), "utf-8")
      );

      if (prerenderManifest.version !== PRERENDER_MANIFEST_VERSION) {
        throw new Error(`Invalid prerender manifest version: ${prerenderManifest.version}`);
      }

      for (const [route, { srcRoute, dataRoute }] of Object.entries(prerenderManifest.routes)) {
        const isPagesRouter = dataRoute?.endsWith(".json");
        if (isPagesRouter && prerenderManifest.dynamicRoutes[srcRoute || ""]?.fallback === false) {
          CacheHandler.#fallbackFalseRoutes.add(normalizePagePath(route));
        }
      }
    } catch (error) {
      debugWarn("Failed to read prerender-manifest.json", error);
    }

    CacheHandler.#mergedHandler = {
      async get(key, meta) {
        for (const handler of handlers) {
          try {
            let cacheValue = await handler.get(key, meta);

            if (
              typeof cacheValue?.lifespan?.expireAt === "number" &&
              cacheValue.lifespan.expireAt < Math.floor(Date.now() / 1000)
            ) {
              cacheValue = null;
              await removeEntryFromHandlers(handlers, key);
            }

            if (cacheValue) return cacheValue;
          } catch (error) {
            debugWarn(`[handler: ${handler.name}] [method: get] [key: ${key}]`, error?.message || error);
          }
        }

        return null;
      },
      async set(key, cacheValue, options) {
        const results = await Promise.allSettled(
          handlers.map((handler) => handler.set(key, { ...cacheValue }, options))
        );
        results.forEach((result, index) => {
          if (result.status === "rejected") {
            debugWarn(`[handler: ${handlers[index].name}] [method: set] [key: ${key}]`, result.reason);
          }
        });
        if (results.every((result) => result.status === "rejected")) return null;
        return results.some((result) => result.status === "fulfilled" && result.value === true);
      },
      async revalidateTag(tag) {
        const results = await Promise.allSettled(handlers.map((handler) => handler.revalidateTag(tag)));
        results.forEach((result, index) => {
          if (result.status === "rejected") {
            debugWarn(`[handler: ${handlers[index].name}] [method: revalidateTag] [tag: ${tag}]`, result.reason);
          }
        });
      },
    };
  }

  constructor(context) {
    CacheHandler.#context ??= context;
  }

  /**
   * `ctx` is what Next.js passes: `kind` ("FETCH", "APP_PAGE", "APP_ROUTE",
   * "PAGES") and, for fetch reads, `tags` and `softTags` in Next.js 15;
   * `kindHint` and `softTags` in Next.js 14.
   */
  async get(cacheKey, ctx = {}) {
    await CacheHandler.#configureCacheHandler();

    const { softTags = [], kindHint, kind } = ctx;
    // Tags passed at read time, not only those stored with the entry, can invalidate it.
    const implicitTags = isFetchRead(ctx) ? [...(ctx.tags ?? []), ...softTags] : softTags;

    let cachedData = await CacheHandler.#mergedHandler.get(cacheKey, { implicitTags, kindHint, kind });

    const pagePath = pagePathOfKey(cacheKey);
    if (!cachedData && !isFetchRead(ctx) && CacheHandler.#fallbackFalseRoutes.has(pagePath)) {
      const pageKind = kind === "PAGES" ? "PAGES" : "PAGE";
      cachedData = await CacheHandler.#readPagesRouterPage(pagePath, pageKind);
      if (cachedData) {
        await CacheHandler.#mergedHandler.set(cacheKey, cachedData);
      }
    }

    if (!cachedData) return null;

    return { ...cachedData, value: deserializeValue(cachedData.value) };
  }

  /**
   * `ctx.initialLastModified` and `ctx.onlyIfAbsent` are set only by
   * registerInitialCache; Next.js passes neither. Resolves to true when the
   * entry was stored, false when it was skipped (already present or expired)
   * and null when every handler failed. Next.js ignores the result.
   */
  async set(cacheKey, incrementalCacheValue, ctx = {}) {
    await CacheHandler.#configureCacheHandler();

    const { tags = [], cacheControl, initialLastModified, onlyIfAbsent = false } = ctx;
    const value = incrementalCacheValue;
    const lastModified = Math.round(initialLastModified ?? Date.now());

    const pagePath = pagePathOfKey(cacheKey);
    const isPagesRouterPage = value?.kind === "PAGE" || value?.kind === "PAGES";
    const hasFallbackFalse = isPagesRouterPage && CacheHandler.#fallbackFalseRoutes.has(pagePath);
    const lifespan = hasFallbackFalse ? null : getLifespanParameters(lastModified, revalidateOf(ctx, value));

    if (lifespan !== null && Date.now() > lifespan.expireAt * 1000) {
      return false;
    }

    // Pages and route handlers carry their tags, implicit ones included, in a header.
    const cacheTags = RESPONSE_KINDS.has(value?.kind)
      ? Array.from(new Set([...tags, ...getTagsFromHeaders(value.headers ?? {})]))
      : tags;

    const cacheValue = {
      lastModified,
      lifespan,
      tags: Object.freeze(cacheTags),
      value: serializeValue(value),
      // Returned by get: Next.js 16 serves the entry with the lifetime it was stored with.
      ...(cacheControl ? { cacheControl } : {}),
    };

    const stored = await CacheHandler.#mergedHandler.set(cacheKey, cacheValue, { onlyIfAbsent });

    if (hasFallbackFalse) {
      await CacheHandler.#writePagesRouterPage(pagePath, value);
    }

    return stored;
  }

  /**
   * Accepts one tag or a list. The second argument of Next.js 16
   * (`{ expire }`, stale-while-revalidate) is ignored: tagged entries are
   * always dropped at once, so the next request renders them again.
   */
  async revalidateTag(tags, _durations) {
    await CacheHandler.#configureCacheHandler();

    for (const currentTag of typeof tags === "string" ? [tags] : tags ?? []) {
      await CacheHandler.#mergedHandler.revalidateTag(currentTag);
    }
  }

  resetRequestCache() {}
}

CacheHandler.onCreation(async ({ buildId }) => {
  if (isBuildPhase()) {
    log("build phase: Redis disabled, using in-memory cache");
    return { handlers: [createMemoryHandler()] };
  }

  const memoryHandler = createMemoryHandler();
  const url = process.env.REDIS_PRIVATE_URL || process.env.REDIS_URL;

  if (!url) {
    log("no REDIS_PRIVATE_URL or REDIS_URL set, using in-memory cache");
    return { handlers: [memoryHandler] };
  }

  const namespace = cacheNamespace();
  const pagesPrefix = `${namespace}:${buildId ?? "unknown-build"}:`;
  const dataPrefix = `${namespace}:data:`;
  log(`key prefixes ${pagesPrefix} (pages, routes) and ${dataPrefix} (data)`);

  const copies = (globalThis[CONFIGURED_COPIES] = (globalThis[CONFIGURED_COPIES] ?? 0) + 1);
  if (copies > 1) {
    console.warn(
      `[cache-handler] ${copies} copies of this module are configured in this process, each with its own ` +
        "Redis connection: import it in instrumentation.ts by file URL with webpackIgnore, as Next.js does"
    );
  }

  const client = await connectRedis(url);
  const redisHandler = createKindRouter({
    pagesHandler: createRedisStringsHandler({ client, keyPrefix: pagesPrefix }),
    dataHandler: createRedisStringsHandler({ client, keyPrefix: dataPrefix }),
  });

  return { handlers: [createResilientHandler({ client, redisHandler, memoryHandler })] };
});

async function runWithConcurrency(items, limit, worker) {
  let next = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      await worker(items[next++]);
    }
  });
  await Promise.all(runners);
}

/**
 * Copies the build output (prerendered pages, route handlers and fetch cache)
 * into the cache handler at server startup. Entries that already exist for
 * this build are left as they are. Call it from `instrumentation.ts`.
 *
 * Entries are stored in the shape the installed Next.js major reads (`PAGE`
 * and `ROUTE` in 14, `APP_PAGE`, `PAGES` and `APP_ROUTE` from 15), under the
 * route-scoped key Next.js 15.5.27+ records in each entry's `.meta` file when
 * there is one. From Next.js 15, partial prerendering shells and fallback
 * shells are left to Next.js. `options.nextMajorVersion` overrides the
 * detected major (default: the `next` package resolved from the app, else 15).
 */
export async function registerInitialCache(Handler = CacheHandler, { distDir = ".next", nextMajorVersion: major } = {}) {
  if (process.env.NODE_ENV !== "production" || isBuildPhase()) return;

  const nextDir = path.join(process.cwd(), distDir);
  const serverDistDir = path.join(nextDir, "server");
  const fetchCacheDir = path.join(nextDir, "cache", "fetch-cache");
  const startedAt = Date.now();
  const legacyKinds = (major ?? nextMajorVersion() ?? 15) < 15;

  let prerenderManifest;
  try {
    prerenderManifest = JSON.parse(await fsPromises.readFile(path.join(nextDir, "prerender-manifest.json"), "utf-8"));
    if (prerenderManifest.version !== PRERENDER_MANIFEST_VERSION) {
      throw new Error(`Invalid prerender manifest version: ${prerenderManifest.version}`);
    }
  } catch (error) {
    log(`initial cache skipped: ${error?.message || error}`);
    return;
  }

  const cacheHandler = new Handler({ serverDistDir, dev: false });
  const counts = { pages: 0, routes: 0, fetch: 0, skipped: 0, ignored: 0, failed: 0 };

  async function setEntry(kind, key, value, ctx) {
    try {
      const stored = await cacheHandler.set(key, value, { ...ctx, onlyIfAbsent: true });
      counts[stored === null ? "failed" : stored ? kind : "skipped"] += 1;
    } catch (error) {
      counts.failed += 1;
      debugWarn(`initial cache: failed to set ${kind} ${key}`, error?.message || error);
    }
  }

  const readMeta = (file) => fsPromises.readFile(file, "utf-8").then((data) => JSON.parse(data));

  function lifetime(route) {
    return {
      revalidate: route.initialRevalidateSeconds,
      cacheControl: legacyKinds
        ? undefined
        : { revalidate: route.initialRevalidateSeconds, expire: route.initialExpireSeconds },
    };
  }

  async function readSegments(base, segmentPaths) {
    if (!Array.isArray(segmentPaths)) return undefined;
    const segmentData = new Map();
    await Promise.all(
      segmentPaths.map(async (segmentPath) => {
        try {
          segmentData.set(segmentPath, await fsPromises.readFile(`${base}.segments${segmentPath}.segment.rsc`));
        } catch {
          // Next.js treats a missing segment as dynamic too.
        }
      })
    );
    return segmentData;
  }

  async function setPage(route, router, manifestRoute) {
    const cachePath = normalizePagePath(route);
    const base = path.join(serverDistDir, router, cachePath);
    const isAppRouter = router === "app";

    try {
      const [{ mtimeMs }, html, data, meta] = await Promise.all([
        fsPromises.stat(`${base}.html`),
        fsPromises.readFile(`${base}.html`, "utf-8"),
        isAppRouter && !legacyKinds
          ? fsPromises.readFile(`${base}.rsc`)
          : fsPromises
              .readFile(`${base}.${isAppRouter ? "rsc" : "json"}`, "utf-8")
              .then((text) => (isAppRouter ? text : JSON.parse(text))),
        // Pages Router entries have a .meta file only from Next.js 15.5.27.
        isAppRouter ? readMeta(`${base}.meta`) : readMeta(`${base}.meta`).catch(() => undefined),
      ]);

      // Next.js 15 reads these through its own resume logic: let it render them.
      if (!legacyKinds && (meta?.routeCache?.isFallback || meta?.postponed !== undefined || manifestRoute.experimentalPPR)) {
        counts.ignored += 1;
        return;
      }

      let value;
      if (legacyKinds) {
        value = {
          kind: "PAGE",
          html,
          pageData: data,
          postponed: meta?.postponed,
          headers: meta?.headers,
          status: meta?.status,
        };
      } else if (isAppRouter) {
        value = {
          kind: "APP_PAGE",
          html,
          rscData: data,
          headers: meta?.headers,
          postponed: undefined,
          status: meta?.status,
          segmentData: await readSegments(base, meta?.segmentPaths),
        };
      } else {
        value = { kind: "PAGES", html, pageData: data, headers: meta?.headers, status: meta?.status };
      }

      await setEntry("pages", meta?.routeCache?.key ?? cachePath, value, {
        ...lifetime(manifestRoute),
        initialLastModified: meta?.routeCacheLastModified ?? mtimeMs,
      });
    } catch (error) {
      counts.failed += 1;
      debugWarn(`initial cache: cannot read page ${cachePath}`, error?.message || error);
    }
  }

  async function setRoute(route, manifestRoute) {
    const cachePath = normalizePagePath(route);
    const base = path.join(serverDistDir, "app", cachePath);

    try {
      const [{ mtimeMs }, body, meta] = await Promise.all([
        fsPromises.stat(`${base}.body`),
        fsPromises.readFile(`${base}.body`),
        readMeta(`${base}.meta`),
      ]);

      if (!(meta.headers && meta.status)) {
        throw new Error("route metadata without headers or status");
      }

      await setEntry(
        "routes",
        meta.routeCache?.key ?? cachePath,
        { kind: legacyKinds ? "ROUTE" : "APP_ROUTE", body, headers: meta.headers, status: meta.status },
        {
          ...lifetime(manifestRoute),
          initialLastModified: meta.routeCacheLastModified ?? mtimeMs,
          tags: getTagsFromHeaders(meta.headers),
        }
      );
    } catch (error) {
      counts.failed += 1;
      debugWarn(`initial cache: cannot read route ${cachePath}`, error?.message || error);
    }
  }

  async function setFetch(fetchCacheKey) {
    const filePath = path.join(fetchCacheDir, fetchCacheKey);

    try {
      const [{ mtimeMs }, fetchCache] = await Promise.all([
        fsPromises.stat(filePath),
        fsPromises.readFile(filePath, "utf-8").then((data) => JSON.parse(data)),
      ]);

      await setEntry("fetch", fetchCacheKey, fetchCache, {
        revalidate: fetchCache.revalidate === CACHE_ONE_YEAR ? false : fetchCache.revalidate,
        initialLastModified: mtimeMs,
        tags: fetchCache.tags,
      });
    } catch (error) {
      counts.failed += 1;
      debugWarn(`initial cache: cannot read fetch cache ${fetchCacheKey}`, error?.message || error);
    }
  }

  const routes = Object.entries(prerenderManifest.routes);
  await runWithConcurrency(routes, INITIAL_CACHE_CONCURRENCY, async ([cachePath, manifestRoute]) => {
    const { dataRoute } = manifestRoute;
    if (dataRoute?.endsWith(".json")) {
      await setPage(cachePath, "pages", manifestRoute);
    } else if (dataRoute?.endsWith(".rsc")) {
      await setPage(cachePath, "app", manifestRoute);
    } else if (dataRoute === null) {
      await setRoute(cachePath, manifestRoute);
    }
  });

  let fetchFiles = [];
  try {
    fetchFiles = await fsPromises.readdir(fetchCacheDir);
  } catch {
    // no fetch cache in this build output
  }
  await runWithConcurrency(fetchFiles, INITIAL_CACHE_CONCURRENCY, setFetch);

  log(
    `initial cache registered in ${Date.now() - startedAt} ms: ` +
      `${counts.pages} pages, ${counts.routes} routes, ${counts.fetch} fetch entries stored, ` +
      `${counts.skipped} already present or expired, ${counts.ignored} left to Next.js, ${counts.failed} failed`
  );
}

export default CacheHandler;
