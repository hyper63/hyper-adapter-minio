import { crocks, HyperErr, R, readableStreamFromIterable } from '../deps.js'
import { asyncifyHandle, isBucketExistsErr, isNoSuchKeyErr } from './utils'

const { Async } = crocks
const { prop, map } = R

export const Multi = () => {
  const makeBucket = (minio) => {
    return asyncifyHandle((name, region) =>
      minio.makeBucket(name, region).bichain(
        (err) => {
          return isBucketExistsErr(err)
            ? Async.Resolve(HyperErr({ status: 409, msg: 'bucket already exists' }))
            : Async.Rejected(err) // Some other error
        },
        Async.Resolved,
      )
    )
  }

  const removeBucket = (minio) =>
    asyncifyHandle((name) => {
      return minio.removeBucket(name)
    })

  const bucketExists = (minio) =>
    asyncifyHandle((name) => {
      return minio.bucketExists(name)
    })

  const listBuckets = (minio) =>
    asyncifyHandle(() => {
      return minio.listBuckets()
        .map(map(prop('name'))) // string[]
    })

  const putObject = (minio) =>
    asyncifyHandle(({ bucket, key, body }) => {
      // TODO Need to convert INTO a node stream?
      return minio.putObject(bucket, key, body)
    })

  const removeObject = (minio) =>
    asyncifyHandle(({ bucket, key }) => {
      return minio.moveObject(bucket, key)
    })

  const removeObjects = (minio) =>
    asyncifyHandle(({ bucket, keys }) => {
      return minio.removeObjects(bucket, keys)
    })

  const getObject = (minio) =>
    asyncifyHandle(({ bucket, key }) => {
      return minio.getObject(bucket, key).bichain(
        (err) => {
          return isNoSuchKeyErr(err)
            ? Async.Rejected(HyperErr({ status: 404, msg: 'object not found' }))
            : Async.Rejected(err) // Some other error
        },
        /**
         * Found object
         *
         * Convert the Node ReadableStream received from MinIO client
         * into a Web ReadableStream.
         *
         * This works because a Node ReadableStream is itself an AsyncIterator.
         *
         * https://nodejs.org/api/stream.html#class-streamreadable
         * which implements https://nodejs.org/api/stream.html#readablesymbolasynciterator
         */
        (nodeStream) => Async.of(readableStreamFromIterable(nodeStream)),
      )
    })

  const getSignedUrl = (minio) =>
    // expires in 5 min by default
    asyncifyHandle(({ bucket, key, method, expires = 60 * 5 }) => {
      /**
       * minio bucket names must be lowercase.
       * Furthermore, case is ignored in the bucket name portion of an minio url.
       *
       * The one exception to this is with presigned urls.
       * If the url used has uppercase parts in the bucket name portion of the url,
       * this will cause the signature to not match the signature used to sign
       * the request.
       *
       * So we lowercase the bucket name when generating a presigned url,
       * to prevent a signature mismatch.
       *
       * TODO: should this be done in the library we are consuming?
       */
      return minio.presignedUrl(method, bucket.toLowerCase(), key, expires)
    })

  const listObjects = (minio) =>
    asyncifyHandle(({ bucket, prefix }) => {
      // Not a promise, but a stream instead
      const stream = minio.listObjects(bucket, prefix)

      return new Promise((resolve, reject) => {
        const objects = []
        /**
         * { name, prefix, size, etag }
         *
         * https://min.io/docs/minio/linux/developers/javascript/API.html#listobjects-bucketname-prefix-recursive-listopts
         */
        stream.on('data', (obj) => objects.push(obj))
        stream.on('end', resolve)
        stream.on('err', reject)
      })
    })

  return Object.freeze({
    makeBucket,
    removeBucket,
    bucketExists,
    listBuckets,
    putObject,
    removeObject,
    removeObjects,
    getObject,
    getSignedUrl,
    listObjects,
  })
}
