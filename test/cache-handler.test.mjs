// Hermetic tests for src/cache-handler.mjs: run with `npm test`.
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createKindRouter,
  createMemoryHandler,
  createRedisStringsHandler,
  createResilientHandler,
  getLifespanParameters,
} from "../src/cache-handler.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));

/** In-memory stand-in for the subset of the node-redis v4 API the handler uses. */
function createFakeRedis() {
  const strings = new Map();
  const hashes = new Map();
  const ttls = new Map();
  // Commands may receive commandOptions(...) as first argument.
  const args = (list) => (typeof list[0] === "string" ? list : list.slice(1));
  const hash = (key) => {
    if (!hashes.has(key)) hashes.set(key, new Map());
    return hashes.get(key);
  };

  return {
    isReady: true,
    strings,
    hashes,
    ttls,
    async get(...a) {
      const [key] = args(a);
      return strings.get(key) ?? null;
    },
    async set(...a) {
      const [key, value, options = {}] = args(a);
      if (options.NX && strings.has(key)) return null;
      strings.set(key, value);
      if (options.EX) ttls.set(key, options.EX);
      return "OK";
    },
    async hmGet(...a) {
      const [key, fields] = args(a);
      return fields.map((field) => hashes.get(key)?.get(field) ?? null);
    },
    async hSet(...a) {
      const [key, field, value] = args(a);
      hash(key).set(field, value);
      return 1;
    },
    async hDel(...a) {
      const [key, fields] = args(a);
      for (const field of [].concat(fields)) hashes.get(key)?.delete(field);
      return 1;
    },
    async hScan(...a) {
      const [key] = args(a);
      const tuples = [...(hashes.get(key) ?? new Map())].map(([field, value]) => ({ field, value }));
      return { cursor: 0, tuples };
    },
    async unlink(...a) {
      const [keys] = args(a);
      for (const key of [].concat(keys)) strings.delete(key);
      return 1;
    },
    async expire(...a) {
      const [key, seconds] = args(a);
      ttls.set(key, seconds);
      return 1;
    },
  };
}

const nowSeconds = () => Math.floor(Date.now() / 1000);

function entry({ lastModified = Date.now(), expireIn = 3600, tags = [] } = {}) {
  return {
    lastModified,
    lifespan: { expireAt: nowSeconds() + expireIn },
    tags,
    value: { kind: "FETCH", data: { body: "x" } },
  };
}

test("redis handler prefixes keys, caps TTL and indexes tags", async () => {
  const client = createFakeRedis();
  const handler = createRedisStringsHandler({ client, keyPrefix: "website:build1:", maxTtlSeconds: 600 });

  assert.equal(await handler.set("page", entry({ expireIn: 10 * 86400, tags: ["all"] })), true);

  assert.ok(client.strings.has("website:build1:page"));
  assert.equal(client.ttls.get("website:build1:page"), 600);
  assert.equal(client.hashes.get("website:build1:__sharedTags__").get("page"), '["all"]');
  assert.equal(client.ttls.get("website:build1:__sharedTags__"), 600);
  assert.deepEqual((await handler.get("page")).tags, ["all"]);
});

test("redis handler keeps the lifespan TTL when it is below the cap and skips expired entries", async () => {
  const client = createFakeRedis();
  const handler = createRedisStringsHandler({ client, keyPrefix: "p:", maxTtlSeconds: 600 });

  await handler.set("short", entry({ expireIn: 120 }));
  assert.ok(client.ttls.get("p:short") <= 120 && client.ttls.get("p:short") >= 119);

  assert.equal(await handler.set("expired", entry({ expireIn: -1 })), false);
  assert.equal(client.strings.has("p:expired"), false);
});

test("onlyIfAbsent never overwrites an existing entry", async () => {
  const client = createFakeRedis();
  const handler = createRedisStringsHandler({ client, keyPrefix: "p:" });

  await handler.set("page", { ...entry(), value: { kind: "FETCH", data: { body: "runtime" } } });
  assert.equal(await handler.set("page", entry(), { onlyIfAbsent: true }), false);
  assert.equal((await handler.get("page")).value.data.body, "runtime");
});

test("revalidateTag deletes tagged entries and rejects older copies written again later", async () => {
  const client = createFakeRedis();
  const handler = createRedisStringsHandler({ client, keyPrefix: "p:" });
  const buildTime = Date.now() - 60_000;

  await handler.set("tagged", entry({ lastModified: buildTime, tags: ["all"] }));
  await handler.set("other", entry({ lastModified: buildTime, tags: ["news"] }));
  await handler.revalidateTag("all");

  assert.equal(await handler.get("tagged"), null);
  assert.notEqual(await handler.get("other"), null);

  // Same entry restored from the build output after a restart (onlyIfAbsent).
  await handler.set("tagged", entry({ lastModified: buildTime, tags: ["all"] }), { onlyIfAbsent: true });
  assert.equal(await handler.get("tagged"), null);

  // A newer render is accepted.
  await handler.set("tagged", entry({ lastModified: Date.now() + 1, tags: ["all"] }));
  assert.notEqual(await handler.get("tagged"), null);
});

test("implicit tags passed at read time also invalidate entries", async () => {
  const client = createFakeRedis();
  const handler = createRedisStringsHandler({ client, keyPrefix: "p:" });

  await handler.set("page", entry({ lastModified: Date.now() - 1000 }));
  await handler.revalidateTag("_N_T_/it/prodotti");

  assert.equal(await handler.get("page", { implicitTags: ["_N_T_/it/prodotti"] }), null);
});

test("resilient handler uses Redis while connected and memory only while disconnected", async () => {
  const client = createFakeRedis();
  const redisHandler = createRedisStringsHandler({ client, keyPrefix: "p:" });
  const memoryHandler = createMemoryHandler();
  const handler = createResilientHandler({ client, redisHandler, memoryHandler });

  await handler.set("a", entry());
  assert.ok(client.strings.has("p:a"));
  assert.equal(await memoryHandler.get("a"), null, "memory must not hold copies while Redis is up");

  client.isReady = false;
  assert.equal(await handler.get("a"), null);
  await handler.set("b", entry());
  assert.notEqual(await memoryHandler.get("b"), null);
  assert.equal(client.strings.has("p:b"), false);

  client.isReady = true;
  assert.notEqual(await handler.get("a"), null);
});

test("during next build the handler never contacts Redis", async () => {
  const dir = await mkdtemp(path.join(tmpdir(), "cache-handler-"));
  try {
    const script = `
      import CacheHandler, { registerInitialCache } from ${JSON.stringify(path.join(HERE, "..", "src", "cache-handler.mjs"))};
      const handler = new CacheHandler({ serverDistDir: ${JSON.stringify(path.join(dir, "server"))}, dev: false });
      const started = Date.now();
      await handler.set("k", { kind: "FETCH", data: { body: "b" } }, { revalidate: 60, tags: [] });
      const hit = await handler.get("k", {});
      await registerInitialCache(CacheHandler);
      console.log(JSON.stringify({ hit: hit?.value?.data?.body, ms: Date.now() - started }));
    `;
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
      cwd: dir,
      encoding: "utf-8",
      timeout: 20_000,
      env: {
        ...process.env,
        NODE_ENV: "production",
        NEXT_PHASE: "phase-production-build",
        // Unroutable address: any connection attempt would hang well past the assertion below.
        REDIS_URL: "redis://default:secret@10.255.255.1:6379",
        REDIS_PRIVATE_URL: "",
      },
    });

    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /build phase: Redis disabled/);
    assert.doesNotMatch(result.stdout + result.stderr, /connecting to Redis|secret/);
    const outcome = JSON.parse(result.stdout.trim().split("\n").at(-1));
    assert.equal(outcome.hit, "b");
    assert.ok(outcome.ms < 1000, `took ${outcome.ms} ms`);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("warns when more than one copy of the module is configured in one process", async () => {
  const dir = await mkdtemp(path.join(tmpdir(), "cache-handler-"));
  try {
    const moduleUrl = new URL("../src/cache-handler.mjs", import.meta.url).href;
    const run = (copies) =>
      spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
          const copies = ${JSON.stringify(copies)};
          await Promise.all(copies.map(async (copy) => {
            const { default: CacheHandler } = await import(${JSON.stringify(moduleUrl)} + copy);
            await new CacheHandler({ serverDistDir: ${JSON.stringify(path.join(dir, "server"))}, dev: false }).get("k", {});
          }));
          process.exit(0);
          `,
        ],
        {
          cwd: dir,
          encoding: "utf-8",
          timeout: 30_000,
          // Refused connection: the handler falls back to memory after its connect timeout.
          env: { ...process.env, NODE_ENV: "production", NEXT_PHASE: "", REDIS_PRIVATE_URL: "", REDIS_URL: "redis://127.0.0.1:1" },
        }
      );

    const single = run(["?copy=1"]);
    assert.equal(single.status, 0, single.stderr);
    assert.doesNotMatch(single.stderr, /copies of this module/);

    const double = run(["?copy=1", "?copy=2"]);
    assert.equal(double.status, 0, double.stderr);
    assert.match(double.stderr, /2 copies of this module are configured/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("registerInitialCache stores prerendered output under the keys Next.js reads, including / as /index", async () => {
  const { mkdir, writeFile } = await import("node:fs/promises");
  const dir = await mkdtemp(path.join(tmpdir(), "cache-handler-"));
  try {
    const app = path.join(dir, ".next", "server", "app");
    await mkdir(app, { recursive: true });
    await writeFile(
      path.join(dir, ".next", "prerender-manifest.json"),
      JSON.stringify({
        version: 4,
        routes: {
          "/": { srcRoute: "/", dataRoute: "/index.rsc", initialRevalidateSeconds: false },
          "/about": { srcRoute: "/about", dataRoute: "/about.rsc", initialRevalidateSeconds: 60 },
          "/robots.txt": { srcRoute: "/robots.txt", dataRoute: null, initialRevalidateSeconds: false },
        },
        dynamicRoutes: {},
      })
    );
    for (const name of ["index", "about"]) {
      await writeFile(path.join(app, `${name}.html`), `<html>${name}</html>`);
      await writeFile(path.join(app, `${name}.rsc`), `rsc:${name}`);
      await writeFile(path.join(app, `${name}.meta`), JSON.stringify({ headers: { "x-next-cache-tags": "all" }, status: 200 }));
    }
    await writeFile(path.join(app, "robots.txt.body"), "User-agent: *");
    await writeFile(path.join(app, "robots.txt.meta"), JSON.stringify({ headers: { "content-type": "text/plain" }, status: 200 }));

    const script = `
      import CacheHandler, { registerInitialCache } from ${JSON.stringify(path.join(HERE, "..", "src", "cache-handler.mjs"))};
      await registerInitialCache(CacheHandler);
      const handler = new CacheHandler({ serverDistDir: ${JSON.stringify(path.join(dir, ".next", "server"))}, dev: false });
      const read = async (key) => (await handler.get(key, {}))?.value ?? null;
      console.log(JSON.stringify({
        index: (await read("/index"))?.html,
        about: (await read("/about"))?.html,
        robots: (await read("/robots.txt"))?.body?.toString(),
        slash: await read("/"),
      }));
    `;
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
      cwd: dir,
      encoding: "utf-8",
      timeout: 20_000,
      env: { ...process.env, NODE_ENV: "production", NEXT_PHASE: "", REDIS_URL: "", REDIS_PRIVATE_URL: "" },
    });

    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /2 pages, 1 routes, 0 fetch entries stored, 0 already present or expired, 0 failed/);
    const values = JSON.parse(result.stdout.trim().split("\n").at(-1));
    assert.equal(values.index, "<html>index</html>");
    assert.equal(values.about, "<html>about</html>");
    assert.equal(values.robots, "User-agent: *");
    assert.equal(values.slash, null);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("data entries are shared across builds, pages are not, and revalidation reaches both", async () => {
  const client = createFakeRedis();
  const router = (buildId) =>
    createKindRouter({
      pagesHandler: createRedisStringsHandler({ client, keyPrefix: `storefront:${buildId}:` }),
      dataHandler: createRedisStringsHandler({ client, keyPrefix: "storefront:data:" }),
    });
  const build1 = router("build1");
  const build2 = router("build2");
  const page = { ...entry({ tags: ["products"] }), value: { kind: "PAGE", html: "<html/>" } };
  const data = { ...entry({ tags: ["products"] }), value: { kind: "FETCH", data: { body: "{}" } } };

  await build1.set("/it/home", page);
  await build1.set("fetch-key", data);

  assert.ok(client.strings.has("storefront:build1:/it/home"));
  assert.ok(client.strings.has("storefront:data:fetch-key"));
  assert.equal(await build2.get("/it/home", {}), null, "pages stay with their build");
  assert.notEqual(await build2.get("fetch-key", { kindHint: "fetch" }), null, "data survives a new build");
  assert.equal(await build2.get("fetch-key", {}), null, "page lookups never read the data keyspace");

  await build2.revalidateTag("products");
  assert.equal(await build2.get("fetch-key", { kindHint: "fetch" }), null);
  assert.equal(client.hashes.get("storefront:data:__revalidated_tags__").has("products"), true);
  assert.equal(client.hashes.get("storefront:build2:__revalidated_tags__").has("products"), true);
});

test("a default TTL applies only to entries without revalidate", () => {
  const now = Date.now();
  const nowSeconds = Math.floor(now / 1000);

  // Without a default TTL (v1.1.0 behaviour): one year of stale age, expiry at 1.5x.
  const unbounded = getLifespanParameters(now, false, 0);
  assert.equal(unbounded.expireAt - nowSeconds, Math.floor(31536000 * 1.5));

  // With a default TTL: entries without revalidate expire exactly after it.
  const bounded = getLifespanParameters(now, false, 300);
  assert.equal(bounded.staleAge, 300);
  assert.equal(bounded.expireAt - nowSeconds, 300);

  // Entries with revalidate are unaffected by the default TTL.
  const revalidated = getLifespanParameters(now, 60, 300);
  assert.equal(revalidated.staleAge, 60);
  assert.equal(revalidated.expireAt - nowSeconds, 90);
});
