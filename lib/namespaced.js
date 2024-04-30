import { crocks, HyperErr, isHyperErr, join, R } from '../deps.js'

import { Multi } from './multi.js'

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
  complement,
  has,
  map,
  assocPath,
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

export const Namespaced = ({ bucketPrefix, region }) => {
  if (!bucketPrefix) throw new Error('bucketPrefix is required')

  const multi = Multi({ bucketPrefix, region })
  /**
   * The single bucket used for all objects
   *
   * Since this mostly maps to the Multi client, the name of the single bucket
   * will be `${bucketPrefix}-namespaced`
   */
  const namespacedBucket = `namespaced`

  const getMeta = (minio) => {
    const client = {
      makeBucket: multi.makeBucket(minio),
      getObject: multi.getObject(minio),
      bucketExists: multi.bucketExists(minio),
      saveMeta: saveMeta(minio),
    }

    /**
     * Check if the namespaced bucket exists, and create if not
     */
    client.findOrCreateSingleBucket = () => {
      return client.bucketExists(namespacedBucket)
        .chain((exists) => exists ? Async.Resolved() : client.makeBucket(namespacedBucket))
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
                return isHyperErr(err) && err.status === 404
                  // Create
                  ? Async.of({ [CREATED_AT]: new Date().toISOString() })
                    .chain((meta) => client.saveMeta(meta).map(() => meta))
                  : Async.Rejected(err) // Some other error
              },
              // Found
              (nodeStream) =>
                Async.of(nodeStream)
                  .map((nodeStream) => ReadableStream.from(nodeStream))
                  /**
                   * Body is a ReadableStream, so we use Response to
                   * buffer the stream and then parse it as json using json()
                   */
                  .chain(Async.fromPromise((stream) => new Response(stream).json())),
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
         *
         * multi takes care of converting to node stream
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
                  prefix: createPrefix(name, ''),
                })
                  .chain(
                    (objects) => {
                      return Async.of(objects)
                        .map(map(prop('name')))
                        .chain((keys) =>
                          keys.length
                            ? client.removeObjects({
                              bucket: namespacedBucket,
                              keys: keys.map((key) => createPrefix(name, key)),
                            })
                            : Async.Resolved()
                        )
                    },
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

      return (name) =>
        client.getMeta()
          .chain((meta) => checkNamespaceExists(meta, name))
          .bichain(
            (err) => {
              if (isHyperErr(err) && err.status === 404) return Async.Resolved(false)
              return Async.Rejected(err)
            },
            Async.Resolved,
          )
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
          /**
           * trim the namespace off of the object name
           */
          .map((objects) =>
            objects.map((o) => ({
              ...o,
              name: o.name.substring(createPrefix(bucket, prefix).length + 1),
            }))
          )
    },
  }
}
