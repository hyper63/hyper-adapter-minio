import { HyperErr, R, ReadableWebToNodeStream } from '../deps.js'
import { asyncifyHandle, isBucketExistsErr, isNoSuchKeyErr } from './utils.js'

const { prop, map } = R

export const createPrefix = (prefix) => (str) => `${prefix}-${str}`

export const Multi = ({ bucketPrefix, region }) => {
  if (!bucketPrefix) throw new Error('bucketPrefix is required')

  const bucketWithPrefix = createPrefix(bucketPrefix)

  const makeBucket = (minio) => {
    return asyncifyHandle((name) =>
      /**
       * When using with AWS s3 and coupled with the fact that LocationConstraint
       * is not required _only_ for us-east-1 (https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucketConfiguration.html),
       * this can cause some hair-pulling gotchas when creating a bucket and not passing region -- the bucket will automatically
       * be attempted to be created in us-east-1, REGARDLESS of whichever region used to instantiate the MinIO Client.
       *
       * So if you're using something like IAM to securely access s3, or a VPC Endpoint for s3 in a region that is _not_ us-east-1,
       * you will simply get an opaque S3 AccessDenied error when creating the bucket -- your IAM Role might be constrained to only
       * access, say us-east-2, or your VPC Endpoint is for s3.us-east-2.amazonaws.com, and accessing us-east-1 out of the blue
       * will simply produce a seemingly incoherent "AccessDenied". -_______-
       *
       * SO, we MUST pass region here that is provided to the adapter, to ensure the bucket is created in the desired region,
       * and any credentials imbued by IAM or the VPC are used.
       */
      minio.makeBucket(bucketWithPrefix(name), region).catch((err) => {
        if (isBucketExistsErr(err)) throw HyperErr({ status: 409, msg: 'bucket already exists' })
        throw err // some other err
      })
    )
  }

  const removeBucket = (minio) =>
    asyncifyHandle((name) => {
      return minio.removeBucket(bucketWithPrefix(name))
    })

  const bucketExists = (minio) =>
    asyncifyHandle((name) => {
      return minio.bucketExists(bucketWithPrefix(name))
    })

  const listBuckets = (minio) =>
    asyncifyHandle(() => {
      return minio.listBuckets()
        .then(map(prop('name'))) // string[]
    })

  const putObject = (minio) =>
    asyncifyHandle(({ bucket, key, body }) => {
      return minio.putObject(bucketWithPrefix(bucket), key, new ReadableWebToNodeStream(body))
    })

  const removeObject = (minio) =>
    asyncifyHandle(({ bucket, key }) => {
      return minio.removeObject(bucketWithPrefix(bucket), key)
    })

  const removeObjects = (minio) =>
    asyncifyHandle(({ bucket, keys }) => {
      return minio.removeObjects(bucketWithPrefix(bucket), keys)
    })

  const getObject = (minio) =>
    asyncifyHandle(({ bucket, key }) => {
      return minio.getObject(bucketWithPrefix(bucket), key)
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
        .then((nodeStream) => ReadableStream.from(nodeStream))
        .catch((err) => {
          if (isNoSuchKeyErr(err)) throw HyperErr({ status: 404, msg: 'object not found' })
          throw err // Some other error
        })
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
      return minio.presignedUrl(method, bucketWithPrefix(bucket).toLowerCase(), key, expires)
    })

  const listObjects = (minio) =>
    asyncifyHandle(({ bucket, prefix }) => {
      return new Promise((resolve, reject) => {
        /**
         * Not a promise, but a stream instead
         *
         * Note: third argument is required to traverse "sub directories" recursively
         */
        const stream = minio.listObjects(bucketWithPrefix(bucket), prefix, true)
        const objects = []
        /**
         * { name, prefix, size, etag }
         *
         * https://min.io/docs/minio/linux/developers/javascript/API.html#listobjects-bucketname-prefix-recursive-listopts
         */
        stream.on('data', (obj) => objects.push(obj))
        stream.on('end', () => resolve(objects))
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
