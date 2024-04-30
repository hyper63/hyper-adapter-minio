import { crocks, HyperErr, R } from './deps.js'

import { Multi } from './lib/multi.js'
import { Namespaced } from './lib/namespaced.js'
import { checkName, handleHyperErr, minioClientSchema } from './lib/utils.js'

const { Async } = crocks
const { prop, map, always, identity } = R

/**
 * @typedef {Object} PutObjectArgs
 * @property {string} bucket
 * @property {string} object
 * @property {any} stream
 *
 * @typedef {Object} ObjectArgs
 * @property {string} bucket
 * @property {string} object
 *
 * @typedef {Object} ListObjectsArgs
 * @property {string} bucket
 * @property {string} [prefix]
 *
 * @typedef {Object} Msg
 * @property {string} [msg]
 *
 * @typedef {Object} Buckets
 * @property {string[]} buckets
 *
 * @typedef {Object} Objects
 * @property {string[]} objects
 *
 * @typedef {Object} ResponseOk
 * @property {boolean} ok
 *
 * @typedef {Msg & ResponseOk} ResponseMsg
 * @typedef {Buckets & ResponseOk} ResponseBuckets
 * @typedef {Objects & ResponseOk} ResponseObjects
 */

/**
 * @param {{ minio: any, bucketPrefix: string, region?: string, useNamespacedBucket: boolean }} config
 * @returns hyper storage terminating adapter impl
 */
export default function (config) {
  const { bucketPrefix, region, useNamespacedBucket, minio } = config

  const lib = useNamespacedBucket
    ? Namespaced({ bucketPrefix, region })
    : Multi({ bucketPrefix, region })

  const client = minioClientSchema.parse({
    makeBucket: lib.makeBucket(minio),
    removeBucket: lib.removeBucket(minio),
    bucketExists: lib.bucketExists(minio),
    listBuckets: lib.listBuckets(minio),
    putObject: lib.putObject(minio),
    removeObject: lib.removeObject(minio),
    removeObjects: lib.removeObjects(minio),
    getObject: lib.getObject(minio),
    getSignedUrl: lib.getSignedUrl(minio),
    listObjects: lib.listObjects(minio),
  })

  /**
   * Check the name of the bucket is valid and whether it exists or not
   */
  const checkBucket = (name) =>
    checkName(name)
      .chain(() =>
        client.bucketExists(name)
          .chain((exists) =>
            exists ? Async.Resolved(name) : Async.Rejected(
              HyperErr({ status: 404, msg: 'bucket does not exist' }),
            )
          )
      )

  /**
   * @param {string} name
   * @returns {Promise<ResponseMsg>}
   */
  function makeBucket(name) {
    return checkName(name)
      .chain(() => client.makeBucket(name))
      .bichain(
        handleHyperErr,
        always(Async.Resolved({ ok: true })),
      ).toPromise()
  }

  /**
   * @param {string} name
   * @returns {Promise<ResponseMsg>}
   */
  function removeBucket(name) {
    return checkBucket(name)
      .chain(() => client.listObjects({ bucket: name, prefix: '' }))
      .chain((objects) => {
        if (!objects.length) return Async.Resolved()
        return client.removeObjects({ bucket: name, keys: objects.map((o) => o.name) })
      })
      .chain(() => client.removeBucket(name))
      .bichain(
        handleHyperErr,
        always(Async.Resolved({ ok: true })),
      ).toPromise()
  }

  /**
   * @returns {Promise<ResponseBuckets>}
   */
  function listBuckets() {
    return client.listBuckets()
      .bichain(
        handleHyperErr,
        (bucketNamesArr) => Async.Resolved({ ok: true, buckets: bucketNamesArr }),
      ).toPromise()
  }

  /**
   * @param {PutObjectArgs}
   * @returns {Promise<ResponseOk>}
   */
  function putObject({ bucket, object, stream, useSignedUrl }) {
    return Async.all([checkBucket(bucket), checkName(object)])
      .chain(() => Async.of({ bucket, object, stream, useSignedUrl }))
      .chain(({ bucket, object, stream, useSignedUrl }) => {
        if (!useSignedUrl) {
          return Async.of(stream)
            .chain((stream) => client.putObject({ bucket, key: object, body: stream }))
            .map(always({ ok: true }))
        }

        return Async.of()
          .chain(() => client.getSignedUrl({ bucket, key: object, method: 'PUT' }))
          .map((url) => ({ ok: true, url }))
      })
      .bichain(
        handleHyperErr,
        Async.Resolved,
      ).toPromise()
  }

  /**
   * @param {ObjectArgs}
   * @returns {Promise<ResponseOk>}
   */
  function removeObject({ bucket, object }) {
    return checkBucket(bucket)
      .chain(() => client.removeObject({ bucket, key: object }))
      .bichain(
        handleHyperErr,
        always(Async.Resolved({ ok: true })),
      ).toPromise()
  }

  /**
   * @param {ObjectArgs}
   * @returns {Promise<{ ok: false, msg?: string, status?: number } | ReadableStream>}
   */
  function getObject({ bucket, object, useSignedUrl }) {
    return Async.all([checkBucket(bucket), checkName(object)])
      .chain(() => Async.of({ bucket, object, useSignedUrl }))
      .chain(({ bucket, object, useSignedUrl }) => {
        if (!useSignedUrl) {
          return client.getObject({ bucket, key: object }) // WebReadableStream, so no need to convert
        }

        /**
         * Generating a signedUrl has no way of knowing whether or not
         * the object actually exists.
         *
         * Since signedUrls already sort of break of boundary,
         * we are deferring this responsibility for checking the signed url to the consumer
         */
        return Async.of()
          // expiration is 1 hour
          .chain(() => client.getSignedUrl({ bucket, key: object, method: 'GET', expires: 10000 }))
          .map((url) => ({ ok: true, url }))
      })
      .bichain(
        handleHyperErr,
        Async.Resolved,
      ).toPromise()
  }

  /**
   * @param {ListObjectsArgs}
   * @returns {Promise<ResponseObjects>}
   */
  function listObjects({ bucket, prefix }) {
    return Async.all([checkBucket(bucket), checkName(prefix)])
      .chain(() => client.listObjects({ bucket, prefix }))
      .bimap(
        identity,
        map(prop('name')),
      ).bichain(
        handleHyperErr,
        (objectNamesArr) => Async.Resolved({ ok: true, objects: objectNamesArr }),
      ).toPromise()
  }

  return Object.freeze({
    makeBucket,
    removeBucket,
    listBuckets,
    putObject,
    removeObject,
    getObject,
    listObjects,
  })
}
