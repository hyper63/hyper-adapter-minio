export * as R from 'https://cdn.skypack.dev/ramda@0.29.0'
export { default as crocks } from 'https://cdn.skypack.dev/crocks@0.12.4'

export * as Minio from 'npm:minio@7.1.1'
/**
 * readable-stream https://www.npmjs.com/package/readable-stream
 * which is a mirror of Node's streams impls, which now has conversion apis ie. "fromWeb"
 * does not seem to work in Deno for some reason
 *
 * This module seems to get the job done for now though
 */
export { ReadableWebToNodeStream } from 'npm:readable-web-to-node-stream@3.0.2'

export { join } from 'https://deno.land/std@0.199.0/path/mod.ts'
export { z } from 'https://deno.land/x/zod@v3.20.5/mod.ts'

export {
  HyperErr,
  isHyperErr,
} from 'https://raw.githubusercontent.com/hyper63/hyper/hyper-utils%40v0.1.1/packages/utils/hyper-err.js'
