import { crocks, HyperErr, join, R } from '../deps.js'

import { Multi } from './multi.js'
import { isNoSuchKeyErr } from './utils.js'

const { Async } = crocks
const {
  assoc,
  dissoc,
  keys,
  always,
  filter,
  compose,
  prop,
  identity,
  ifElse,
  isNil,
  defaultTo,
} = R

const notHas = (prop) => complement(has(prop))
const createPrefix = (bucket, name) => join(bucket, name)

const [META_OBJECT, CREATED_AT, DELETED_AT] = [
  'meta.json',
  'createdAt',
  'deletedAt',
]
/**
 * Given the metadata object and name
 *
 * @param {Object} meta
 * @param {string} name
 * @returns Async.Resolved if the namespace exists
 * and Async.Rejected otherwise
 */
const checkNamespaceExists = (meta, name) => {
  return Async.of(meta)
    .map(
      compose(
        ifElse(
          isNil,
          always(false), // no namespace key
          notHas(DELETED_AT), // set deletedAt means namespace was deleted, so does not exist
        ),
        prop(name),
        defaultTo({}),
      ),
    )
    .chain(ifElse(
      identity,
      Async.Resolved,
      () =>
        Async.Rejected(
          HyperErr({ status: 404, msg: 'bucket does not exist' }),
        ),
    ))
}

export const HYPER_BUCKET_PREFIX = 'hyper-storage-namespaced'

export const Namespaced = (bucketPrefix) => {
  if (!bucketPrefix) throw new Error('bucketPrefix is required')

  const multi = Multi()
  // The single bucket used for all objects
  const namespacedBucket = `${HYPER_BUCKET_PREFIX}-${bucketPrefix}`

  const getMeta = (minio) => {
    const client = {
      getObject: multi.getObject(minio),
      saveMeta: saveMeta(minio),
      /**
       * Check if the namespaced bucket exists, and create if not
       */
      findOrCreateSingleBucket: () => {
        return client.bucketExists(namespacedBucket)
          .chain((exists) => exists ? Async.Resolved() : client.makeBucket(namespacedBucket))
      },
    }

    /**
     * Get the meta.json for the namespaced s3 bucket
     * which holds information like namespace names and when they were created
     *
     * If meta object does not exist, it will be created.
     * Otherwise, will reject if an unhandled error is received.
     */
    return () => {
      return client.findOrCreateSingleBucket()
        .chain(() =>
          client.getObject({ bucket: namespacedBucket, key: META_OBJECT })
            /**
             * Find or create the meta.json object
             */
            .bichain(
              (err) => {
                return isNoSuchKeyErr(err)
                  // Create
                  ? Async.of({ [CREATED_AT]: new Date().toISOString() })
                    .chain((meta) => client.saveMeta(meta).map(() => meta))
                  : Async.Rejected(err) // Some other error
              },
              // Found
              (r) =>
                Async.of(r)
                  /**
                   * Body is a ReadableStream, so we use Response to
                   * buffer the stream and then parse it as json using json()
                   */
                  .chain(Async.fromPromise((r) => new Response(r.Body).json())),
            )
        )
    }
  }

  const saveMeta = (minio) => {
    const client = { putObject: multi.putObject(minio) }

    /**
     * Save the meta object in the namespaced bucket
     * as meta.json
     *
     * @param {object} meta - the json to write to the metadata object
     */
    return (meta) => {
      return client.putObject({
        bucket: namespacedBucket,
        key: META_OBJECT,
        /**
         * use Response to get a ReadableStream of the JSON
         */
        body: new Response(JSON.stringify(meta)).body,
      })
    }
  }

  const makeNamespace = (minio) => {
    const client = { getMeta: getMeta(minio), saveMeta: saveMeta(minio) }

    /**
     * Create a namespace (prefix/folder) within the s3 bucket
     * recording it's existence in the meta file
     *
     * @param {string} name
     * @returns {Promise<ResponseMsg>}
     */
    return (name) => {
      return Async.of(name)
        .chain(client.getMeta)
        .chain((meta) =>
          checkNamespaceExists(meta, name)
            .bichain(
              /**
               * Set a key for the new namespace
               * NOTE: this also removes any deletedAt for the namespace
               */
              () => Async.Resolved(assoc(name, { [CREATED_AT]: new Date().toISOString() }, meta)),
              // The namespace already exists
              () => Async.Rejected(HyperErr({ status: 409, msg: 'bucket already exists' })),
            )
        )
        .chain(client.saveMeta)
    }
  }

  const removeNamespace = (minio) => {
    const client = {
      getMeta: getMeta(minio),
      saveMeta: saveMeta(minio),
      listObjects: multi.listObjects(minio),
      removeObjects: multi.removeObjects(minio),
    }

    /**
     * Remove a namespace aka. folder/prefix in the bucket
     * This is done by simply querying for all of the objects with
     * the prefix and deleting them.
     *
     * If the result isTruncated, then we recurse, until all objects with
     * the prefix are deleted, effectively deleting the namespace.
     *
     * Finally, we remove the bucket from the meta file
     *
     * @param {string} name
     */
    return (name) => {
      return Async.of(name)
        .chain(client.getMeta)
        .chain((meta) =>
          checkNamespaceExists(meta, name)
            .bichain(
              () => Async.Rejected(HyperErr({ status: 404, msg: 'bucket does not exist' })),
              /**
               * grab a list of objects at the prefix
               * and remove them.
               *
               * Once all objects under prefix have been removed
               * the namespace is effectively 'removed'
               */
              () =>
                client.listObjects({
                  bucket: namespacedBucket,
                  prefix: name,
                })
                  .chain(
                    (objects) =>
                      Async.of(objects)
                        .map(prop('name'))
                        .chain((keys) =>
                          keys.length
                            ? client.removeObjects({
                              bucket: namespacedBucket,
                              keys: keys.map((key) => createPrefix(bucket, key)),
                            })
                            : Async.Resolved()
                        ),
                  ),
            )
            .chain(
              () =>
                Async.of(assocPath([name, DELETED_AT], new Date().toISOString(), meta))
                  .chain(client.saveMeta),
            )
        )
    }
  }

  /**
   * The majority of the namespaced bucket client simply maps arguments to
   * the multi-bucket client.
   *
   * Usually, this just means replacing the provided 'bucket' with the single namespaced bucket name,
   * and the provided 'key' with a joined prefix of the provided 'bucket' name and 'key'
   * (since buckets in namespaced are just prefixed paths within the single namespaced bucket)
   */
  return {
    makeBucket: makeNamespace,
    removeBucket: removeNamespace,
    bucketExists: (minio) => {
      const client = { getMeta: getMeta(minio) }

      return (name) => client.getMeta().chain((meta) => checkNamespaceExists(meta, name))
    },
    listBuckets: (minio) => {
      const client = { getMeta: getMeta(minio) }

      return () => {
        return client.getMeta()
          .map(dissoc(CREATED_AT))
          .map(filter(notHas(DELETED_AT)))
          .map(keys) // string[]
      }
    },
    putObject: (minio) => {
      const client = { putObject: multi.putObject(minio) }

      return ({ bucket, key, body }) =>
        client.putObject({ bucket: namespacedBucket, key: createPrefix(bucket, key), body })
    },
    removeObject: (minio) => {
      const client = { removeObject: multi.removeObject(minio) }

      return ({ bucket, key }) =>
        client.removeObject({ bucket: namespacedBucket, key: createPrefix(bucket, key) })
    },
    removeObjects: (minio) => {
      const client = { removeObjects: multi.removeObjects(minio) }

      return ({ bucket, keys }) =>
        client.removeObjects({
          bucket: namespacedBucket,
          keys: keys.map((key) => createPrefix(bucket, key)),
        })
    },
    getObject: (minio) => {
      const client = { getObject: multi.getObject(minio) }

      return ({ bucket, key }) =>
        client.getObject({ bucket: namespacedBucket, key: createPrefix(bucket, key) })
    },
    getSignedUrl: (minio) => {
      const client = { getSignedUrl: multi.getSignedUrl(minio) }

      return ({ bucket, key, method, expires = 60 * 5 }) =>
        client.getSignedUrl({
          bucket: namespacedBucket,
          key: createPrefix(bucket, key),
          method,
          expires,
        })
    },
    listObjects: (minio) => {
      const client = { listObjects: multi.listObjects(minio) }

      return ({ bucket, prefix }) =>
        client.listObjects({ bucket: namespacedBucket, prefix: createPrefix(bucket, prefix) })
    },
  }
}
