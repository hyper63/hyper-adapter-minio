import { crocks, ReadableWebToNodeStream } from '../deps.js'
import { assert, assertEquals, assertObjectMatch } from '../dev_deps.js'

import { Multi } from './multi.js'

const { Async } = crocks

Deno.test('Multi bucket minio client', async (t) => {
  const multi = Multi({ bucketPrefix: 'test', region: 'us-east-2' })

  await t.step('makeBucket', async (t) => {
    await t.step('it should resolve if the bucket is created successfully', () => {
      return multi.makeBucket({ makeBucket: () => Promise.resolve() })('foo')
        .map(() => assert(true))
        .toPromise()
    })

    await t.step('it should prefix the bucket name', () => {
      return multi.makeBucket({
        makeBucket: (name) => {
          assertEquals('test-foo', name)
          return Promise.resolve()
        },
      })('foo')
        .toPromise()
    })

    await t.step('it should make the bucket in the region', () => {
      return multi.makeBucket({
        makeBucket: (_name, region) => {
          assertEquals(region, 'us-east-2')
          return Promise.resolve()
        },
      })('foo')
        .toPromise()
    })

    await t.step('it should throw a HyperErr if the bucket exists', () => {
      return multi.makeBucket({
        makeBucket: () => {
          return Promise.reject({ code: 'BucketAlreadyOwnedByYou' })
        },
      })('foo')
        .bichain(
          (err) => {
            assertObjectMatch(err, { status: 409, msg: 'bucket already exists' })
            return Async.Resolved()
          },
          Async.Rejected,
        )
        .toPromise()
    })

    await t.step('it should bubble unknown errors', () => {
      return multi.makeBucket({
        makeBucket: () => {
          return Promise.reject({ code: 'Unknown' })
        },
      })('foo')
        .bichain(
          (err) => {
            assert(err)
            return Async.Resolved()
          },
          Async.Rejected,
        )
        .toPromise()
    })
  })

  await t.step('removeBucket', async (t) => {
    await t.step('it should resolve if the bucket is removed successfully', () => {
      return multi.removeBucket({ removeBucket: () => Promise.resolve() })('foo')
        .map(() => assert(true))
        .toPromise()
    })

    await t.step('it should prefix the bucket name', () => {
      return multi.removeBucket({
        removeBucket: (name) => {
          assertEquals('test-foo', name)
          return Promise.resolve()
        },
      })('foo')
        .toPromise()
    })
  })

  await t.step('bucketExists', async (t) => {
    await t.step('it should return whatever the client returns', () => {
      return multi.bucketExists({ bucketExists: () => Promise.resolve(true) })('foo')
        .map(assert)
        .toPromise()
    })

    await t.step('it should prefix the bucket name', () => {
      return multi.bucketExists({
        bucketExists: (name) => {
          assertEquals('test-foo', name)
          return Promise.resolve()
        },
      })('foo')
        .toPromise()
    })
  })

  await t.step('listBuckets', async (t) => {
    await t.step('it should return the bucket names', () => {
      return multi.listBuckets({
        listBuckets: () => Promise.resolve(['foo', 'bar', 'fizz'].map((name) => ({ name }))),
      })('foo')
        .map((res) => {
          assertObjectMatch(res, ['foo', 'bar', 'fizz'])
        })
        .toPromise()
    })
  })

  await t.step('putObject', async (t) => {
    await t.step('it should pass the values to the client', () => {
      return multi.putObject({
        putObject: (bucket, key, body) => {
          assertEquals(bucket, 'test-foo')
          assertEquals(key, 'bar.png')
          assert(body)
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        key: 'bar.png',
        body: new Response(JSON.stringify({ foo: 'bar' })).body,
      })
        .toPromise()
    })
  })

  await t.step('removeObject', async (t) => {
    await t.step('it should pass the values to the client', () => {
      return multi.removeObject({
        removeObject: (bucket, key) => {
          assertEquals(bucket, 'test-foo')
          assertEquals(key, 'bar.png')
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        key: 'bar.png',
      })
        .toPromise()
    })
  })

  await t.step('removeObjects', async (t) => {
    await t.step('it should pass the values to the client', () => {
      return multi.removeObjects({
        removeObjects: (bucket, keys) => {
          assertEquals(bucket, 'test-foo')
          assertEquals(keys, ['bar.png'])
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        keys: ['bar.png'],
      })
        .toPromise()
    })
  })

  await t.step('getObject', async (t) => {
    await t.step('it should pass the values to the client', () => {
      return multi.getObject({
        getObject: (bucket, key) => {
          assertEquals(bucket, 'test-foo')
          assertEquals(key, 'bar.png')
          return Promise.resolve(new ReadableWebToNodeStream(new Response('foo').body))
        },
      })({
        bucket: 'foo',
        key: 'bar.png',
      })
        .toPromise()
    })

    await t.step('it throw a HyperErr if no object is found', () => {
      return multi.getObject({
        getObject: () => {
          return Promise.reject({ code: 'NoSuchKey' })
        },
      })({
        bucket: 'foo',
        key: 'bar.png',
      })
        .bichain(
          (err) => {
            assertObjectMatch(err, { status: 404, msg: 'object not found' })
            return Async.Resolved()
          },
          Async.Rejected,
        )
        .toPromise()
    })
  })

  await t.step('getSignedUrl', async (t) => {
    await t.step(
      'it should pass the values to the client, lowercase the bucket and default expires',
      () => {
        return multi.getSignedUrl({
          presignedUrl: (method, bucket, key, expires) => {
            assertEquals(method, 'GET')
            // lowercase
            assertEquals(bucket, 'test-foo')
            assertEquals(key, 'bar.png')
            assertEquals(expires, 60 * 5)
            return Promise.resolve('http://presigned')
          },
        })({
          bucket: 'FOO',
          key: 'bar.png',
          method: 'GET',
        })
          .toPromise()
      },
    )
  })

  await t.step('listObjects', async (t) => {
    await t.step('should gather all of the objects', () => {
      return multi.listObjects({
        listObjects: (bucket, prefix) => {
          assertEquals(bucket, 'test-foo')
          assertEquals(prefix, '/crap')

          // Mock stream
          return {
            on: (event, fn) => {
              switch (event) {
                case 'data': {
                  fn({ name: 'foo' })
                  fn({ name: 'bar' })
                  break
                }
                case 'end': {
                  return fn('foo')
                }
              }
            },
          }
        },
      })({
        bucket: 'foo',
        prefix: '/crap',
      })
        .map((res) => {
          assertObjectMatch(res, [{ name: 'foo' }, { name: 'bar' }])
        })
        .toPromise()
    })
  })
})
