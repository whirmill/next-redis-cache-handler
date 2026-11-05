# @whirmill/next-redis-cache-handler

Redis cache handler for Next.js 14.

- **No Redis during `next build`.** Builds do not need network access to Redis.
- **Pages are scoped to their build.** HTML and RSC payloads reference the build's own assets, so a new build never serves the previous one's pages, nor the other way round.
- **The data cache survives deploys.** `fetch` cache entries are build-independent and stay warm after a deploy.
- **Startup registration.** Prerendered pages and route handlers are copied from `.next` into Redis when the server starts, without overwriting newer entries.
- **Bounded memory.** Every key expires after at most 14 days by default, so entries of old builds disappear even with `maxmemory-policy noeviction`.
- **Tag revalidation**, including the implicit tags that `revalidatePath` uses.
- **Resilient.** If Redis goes away the handler serves from memory and reconnects in the background. Credentials are never logged.

## Requirements

- Next.js 14.2 (App Router; Pages Router routes with `fallback: false` are supported)
- [`redis`](https://www.npmjs.com/package/redis) (node-redis) 4.x
- Node.js 20 or later

## Install

```sh
npm install @whirmill/next-redis-cache-handler redis@4
```

## Setup

### 1. `next.config.js`

```js
module.exports = {
  experimental: {
    // Required in Next.js 14 for instrumentation.ts
    instrumentationHook: true,
  },
  cacheHandler:
    process.env.NODE_ENV === "production"
      ? require.resolve("@whirmill/next-redis-cache-handler")
      : undefined,
  cacheMaxMemorySize: 0,
};
```

In an ES module config (`next.config.mjs`):

```js
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

export default {
  experimental: { instrumentationHook: true },
  cacheHandler:
    process.env.NODE_ENV === "production"
      ? require.resolve("@whirmill/next-redis-cache-handler")
      : undefined,
  cacheMaxMemorySize: 0,
};
```

### 2. `instrumentation.ts`

Place it next to `app/` (in `src/` if the app uses one):

```ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;

  // node:module exports a function: in the webpack bundle its named exports
  // are only reachable through `default`.
  const nodeModule = await import("node:module");
  const createRequire = nodeModule.createRequire ?? nodeModule.default.createRequire;
  const { realpathSync } = await import("node:fs");
  const path = await import("node:path");
  const { pathToFileURL } = await import("node:url");

  // Load the handler natively, by the same file URL Next.js uses for
  // `cacheHandler`, so both share one module instance and one Redis connection.
  const resolveFromApp = createRequire(path.join(process.cwd(), "package.json"));
  const handlerUrl = pathToFileURL(
    realpathSync(resolveFromApp.resolve("@whirmill/next-redis-cache-handler"))
  ).href;
  const { default: CacheHandler, registerInitialCache } = await import(
    /* webpackIgnore: true */ handlerUrl
  );

  await registerInitialCache(CacheHandler);
}
```

A plain `import("@whirmill/next-redis-cache-handler")` would be bundled by webpack into a second copy of the module, with its own Redis connection and state. The handler warns at startup (`N copies of this module are configured`) if that happens.

### 3. Point it at Redis

Set `REDIS_PRIVATE_URL` or `REDIS_URL`. Without either, the handler uses an in-memory cache.

## How it works

| | Where | Lifetime |
|---|---|---|
| Pages and route handlers | `<namespace>:<buildId>:<key>` | until revalidated or expired, at most `NEXT_CACHE_MAX_TTL_SECONDS` |
| Data cache (`fetch`) | `<namespace>:data:<key>` | `revalidate` × 1.5, entries without `revalidate` use `NEXT_CACHE_DEFAULT_TTL_SECONDS` if set, at most `NEXT_CACHE_MAX_TTL_SECONDS` |
| Tag indexes | `<prefix>__sharedTags__`, `<prefix>__revalidated_tags__` | refreshed on writes, at most `NEXT_CACHE_MAX_TTL_SECONDS` |

- **Build** (`NEXT_PHASE=phase-production-build`): memory only.
- **Startup**: `registerInitialCache` writes the prerender manifest's pages and routes with `SET NX`, so a restart or a second replica never overwrites newer entries. With a custom cache handler Next.js does not write `.next/cache/fetch-cache`, so the data cache fills on the first requests.
- **Revalidation**: `revalidateTag` deletes the tagged entries in both keyspaces and records a timestamp for the tag. Entries written before that timestamp, such as build output restored after a restart, are treated as missing. Implicit tags (`revalidatePath`) are checked the same way.
- **Redis outage**: while the client is disconnected, reads and writes use an in-process memory cache, never both at once, so another replica's revalidation cannot leave a stale copy behind. The client reconnects with backoff.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_PRIVATE_URL`, `REDIS_URL` | unset | Redis URL, the first one set wins |
| `NEXT_CACHE_NAMESPACE` | slug of `RAILWAY_SERVICE_NAME`, else `next` | key namespace |
| `NEXT_CACHE_MAX_TTL_SECONDS` | `1209600` (14 days) | maximum lifetime of any key |
| `NEXT_CACHE_DEFAULT_TTL_SECONDS` | unset | lifetime of entries without `revalidate`; unset keeps them until the maximum lifetime |
| `NEXT_CACHE_REDIS_TIMEOUT_MS` | `1000` | timeout of each Redis command |
| `NEXT_CACHE_MEMORY_MAX_ENTRIES` | `5000` | size of the in-memory fallback |
| `NEXT_PRIVATE_DEBUG_CACHE` | unset | verbose logging |

## Extending

App-specific behaviour goes in a subclass. Point `cacheHandler` and `instrumentation.ts` at the subclass module, and re-export `registerInitialCache` from it:

```js
// cache-handler.mjs in the app
const base = await import("@whirmill/next-redis-cache-handler");

export default class AppCacheHandler extends base.default {
  async get(key, ctx = {}) {
    const entry = await super.get(key, ctx);
    // ...
    return entry;
  }
}

export function registerInitialCache(Handler = AppCacheHandler, options) {
  return base.registerInitialCache(Handler, options);
}
```

Settings read from the environment are evaluated when the package is first imported, so an adapter that maps its own variable names must set `process.env` before the dynamic `import()`.

## Notes

- Do not pass `disableOfflineQueue` to a node-redis 4 client used with per-command abort signals: when the connection drops it throws in `flushAll` and never reconnects. The handler creates its own client and keeps the offline queue.
- The handler targets the Next.js 14 cache handler interface (prerender manifest version 4). Next.js 15 and later are not supported.

## Development

```sh
npm install
npm test
```

## Releasing

1. Bump `version` in `package.json` and commit on `main`.
2. Tag the commit `v<version>` and push the tag: the `Release` workflow tests and publishes it to npm with provenance, through npm trusted publishing.

## License

[MIT](LICENSE). See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
