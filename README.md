<h1 align="center">hyper-adapter-minio</h1>
<p align="center">A Storage port adapter that can use S3 or MinIO for object storage in the <a href="https://hyper.io/">hyper</a> service framework</p>
</p>
<p align="center">
  <a href="https://github.com/hyper63/hyper-adapter-minio/actions/workflows/test-and-publish.yml"><img src="https://github.com/hyper63/hyper-adapter-minio/actions/workflows/test-and-publish.yml/badge.svg" alt="Test" /></a>
  <a href="https://github.com/hyper63/hyper-adapter-minio/tags/"><img src="https://img.shields.io/github/tag/hyper63/hyper-adapter-minio" alt="Current Version" /></a>
</p>

---

<!-- toc -->

- [Getting Started](#getting-started)
  - [Credentials](#credentials)
    - [From the URL](#from-the-url)
  - [from ENV VARS](#from-env-vars)
- [Multiple Buckets or Namespaced Single Bucket](#multiple-buckets-or-namespaced-single-bucket)
- [Features](#features)
- [Methods](#methods)
- [Contributing](#contributing)
- [Testing](#testing)
- [License](#license)

<!-- tocstop -->

## Getting Started

`hyper.config.js`

```js
import { default as minio } from 'https://raw.githubusercontent.com/hyper63/hyper-adapter-minio/main/mod.js'

export default {
  app,
  adapter: [
    {
      port: 'storage',
      plugins: [
        minio({ url: 'https://minioadmin:minioadmin@play.minio.io', bucketPrefix: 'uniquePrefix' }),
      ],
    },
  ],
}
```

When you configure the hyper service with this adapter, you must provide a unique bucket prefix.
This helps ensure your bucket name is globally unique

> The unique name is an alphanumeric string that contains identifing information, this will enable
> you to identify the buckets created by this adapter.

### Credentials

There are two credentials needed in order for this adapter to interact with the underlying S3 or
MinIO resource: an `accessKey` and a `secretKey`. These credentials can be provided to this adapter
in a couple ways.

#### From the URL

The first is simply in the `url` as the `username` and `password`:

```js
minio({ url: 'https://accessKey:secretKey@play.minio.io', bucketPrefix: 'uniquePrefix' })
```

### from ENV VARS

You can also set the environment variables `MINIO_ROOT_USER` to your `accessKey` and
`MINIO_ROOT_PASSWORD` to your `secretKey`.

> Credentials provided in the `url` will supercede any credentials pulled from environment
> variables. In other words, if credentials are provided in both ways, the credentials derived from
> the url will be used.

## Multiple Buckets or Namespaced Single Bucket

This adapter can be configured to either create a bucket, in the underying S3 or MinIO, per hyper
Storage Service, or instead to use a _single namespaced_ bucket to store all objects across all
hyper Storage services. In the latter configuration, each hyper Storage service is represented as a
_prefix_ in the single namespaced bucket.

> Among other reasons, using a _single namespaced_ bucket is a great option, if you're concerned
> with surpassing AWS'
> [S3 Bucket Count Restriction](https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html)

**By default, the adapter uses the bucket per hyper Storage Service implementation**. To enable the
_single namespaced_ bucket approach, pass the `useNamespacedBucket` flag into the adapter:

```js
minio({
  url: 'https://accessKey:secretKey@play.minio.io',
  bucketPrefix: 'uniquePrefix',
  useNamespacedBucket: true,
})
```

This will make the adapter create only a single bucket called `uniquePrefix-namespaced`. Each hyper
Storage Service is then implemented as a private prefix within the bucket. For example, if you had
hyper Storage services `foo` and `bar`, the structure of the bucket would look like:

```
- uniquePrefix-namespaced 
--|/foo
---| foo.png
---| .... # all objects in the 'foo' Storage Service here
--| /bar
---| bar.png
---| .... # all objects in the 'bar' Storage Service here
```

## Features

- Create an `s3` bucket
- Remove an `s3` bucket
- List `s3` buckets
- Put an object into an `s3` bucket
- Remove an object from an `s3` bucket
- Get an object from an `s3` bucket
- List objects in an `s3` bucket

## Methods

This adapter fully implements the Storage port and can be used as the
[hyper Storage service](https://docs.hyper.io/storage-api) adapter

See the full port [here](https://github.com/hyper63/hyper/tree/main/packages/port-storage)

## Contributing

Contributions are welcome! See the hyper
[contribution guide](https://docs.hyper.io/oss/contributing-to-hyper)

## Testing

```
deno task test
```

To lint, check formatting, and run unit tests

To run the test harness, run `deno task test:harness`. a `hyper` server with the adapter installed
will be running on port `6363`

## License

Apache-2.0
