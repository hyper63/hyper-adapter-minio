import { crocks, ReadableWebToNodeStream } from '../deps.js'
import { assert, assertEquals, assertObjectMatch } from '../dev_deps.js'

import { Namespaced } from './namespaced.js'

const { Async } = crocks

Deno.test('Namespaced bucket minio client', async (t) => {
  const namespaced = Namespaced({ bucketPrefix: 'test', region: 'us-east-2' })

  const META = {
    createdAt: new Date().toJSON(),
    foo: {
      createdAt: new Date().toJSON(),
    },
    deleted: {
      createdAt: new Date().toJSON(),
      deletedAt: new Date().toJSON(),
    },
  }
  const happyMinio = {
    makeBucket: () => Promise.resolve(),
    getObject: (_bucket, key) => {
      if (key === 'meta.json') {
        return Promise.resolve(
          new ReadableWebToNodeStream(
            new Response(JSON.stringify(META)).body,
          ),
        )
      }

      return Promise.resolve(
        new ReadableWebToNodeStream(new Response(JSON.stringify({ fizz: 'buzz' })).body),
      )
    },
    bucketExists: () => Promise.resolve(true),
    putObject: () => Promise.resolve(),
  }

  await t.step('makeBucket', async (t) => {
    await t.step('it should resolve if the namespace is created successfully', () => {
      return namespaced.makeBucket(happyMinio)('new_bucket')
        .map(() => assert(true))
        .toPromise()
    })

    await t.step(
      'it should create a single bucket to namespace if it does not already exist',
      () => {
        return namespaced.makeBucket({
          ...happyMinio,
          makeBucket: (name) => {
            assertEquals(name, 'test-namespaced')
            return Promise.resolve()
          },
          bucketExists: (name) => {
            assertEquals(name, 'test-namespaced')
            return Promise.resolve(false)
          },
        })('new_bucket')
          .toPromise()
      },
    )

    await t.step(
      'it should create a metadata file if it does not already exist',
      () => {
        let putObjectCount = 0
        return namespaced.makeBucket({
          ...happyMinio,
          getObject: (_bucket, key) => {
            if (key === 'meta.json') {
              return Promise.reject({ ok: false, status: 404 })
            }

            return Promise.resolve(
              new ReadableWebToNodeStream(new Response(JSON.stringify({ fizz: 'buzz' })).body),
            )
          },
          putObject: async (_bucket, _key, body) => {
            putObjectCount++
            const meta = await (new Response(ReadableStream.from(body)).json())
            // Have to create the meta object, then persist the new bucket to it. separate calls
            if (putObjectCount > 1) assert(meta.new_bucket.createdAt)
          },
        })('new_bucket')
          .toPromise()
      },
    )

    await t.step('it should update the metadata file with the new bucket metadata', () => {
      return namespaced.makeBucket({
        ...happyMinio,
        putObject: async (_bucket, _key, body) => {
          const meta = await (new Response(ReadableStream.from(body)).json())
          assertObjectMatch(meta, META)
          assert(meta.new_bucket.createdAt)
        },
      })('new_bucket')
        .toPromise()
    })

    await t.step('it should throw a HyperErr if the namespace already exists', () => {
      return namespaced.makeBucket(happyMinio)('foo')
        .bichain(
          (err) => {
            assertObjectMatch(err, { status: 409, msg: 'bucket already exists' })
            return Async.Resolved()
          },
          Async.Rejected,
        )
        .toPromise()
    })
  })

  await t.step('removeBucket', async (t) => {
    const _happyMinio = {
      ...happyMinio,
      listObjects: (bucket, prefix) => {
        assertEquals(bucket, 'test-namespaced')
        assertEquals(prefix, 'foo')

        // Mock stream
        return {
          on: (event, fn) => {
            switch (event) {
              case 'data': {
                fn({ name: 'crap/fizz.png' })
                fn({ name: 'crap/bar.png' })
                break
              }
              case 'end': {
                return fn('foo')
              }
            }
          },
        }
      },
      removeObjects: () => Promise.resolve(),
    }

    await t.step('should remove the namespace', () => {
      return namespaced.removeBucket({
        ..._happyMinio,
      })('foo')
        .map(() => assert(true))
        .toPromise()
    })

    await t.step('should update the metadata file', () => {
      return namespaced.removeBucket({
        ..._happyMinio,
        listObjects: () => {
          // Mock stream
          return {
            on: (event, fn) => {
              switch (event) {
                case 'data': {
                  break
                }
                case 'end': {
                  return fn('foo')
                }
              }
            },
          }
        },
        putObject: async (_bucket, _key, body) => {
          const meta = await (new Response(ReadableStream.from(body)).json())
          assertObjectMatch(meta, META)
          assert(meta.foo.deletedAt)
        },
      })('foo')
        .toPromise()
    })

    await t.step('should remove all objects in the namespace', () => {
      return namespaced.removeBucket({
        ..._happyMinio,
        removeObjects: (bucket, keys) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(keys, ['foo/crap/fizz.png', 'foo/crap/bar.png'])

          return Promise.resolve()
        },
      })('foo')
        .map(() => assert(true))
        .toPromise()
    })
  })

  await t.step('bucketExists', async (t) => {
    await t.step('should return true if the namespace exists', async () => {
      await namespaced.bucketExists(happyMinio)('foo')
        .map(assert)
        .toPromise()
    })

    await t.step('should return false if the namespace does not exist', async () => {
      await namespaced.bucketExists(happyMinio)('new_bucket')
        .map((exists) => assert(!exists))
        .toPromise()
    })
  })

  await t.step('listBuckets', async (t) => {
    await t.step('should return the bucket names', () => {
      return namespaced.listBuckets(happyMinio)()
        .map((res) => assertObjectMatch(res, ['foo']))
        .toPromise()
    })
  })

  await t.step('putObject', async (t) => {
    await t.step('should put the object in the namespace', () => {
      return namespaced.putObject({
        ...happyMinio,
        putObject: (bucket, key, body) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(key, 'foo/bar/fizz.png')
          assert(body)
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        key: '/bar/fizz.png',
        body: new Response(JSON.stringify({ foo: 'bar' })).body,
      })
        .toPromise()
    })
  })

  await t.step('removeObject', async (t) => {
    await t.step('should remove the object in the namespace', () => {
      return namespaced.removeObject({
        ...happyMinio,
        removeObject: (bucket, key) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(key, 'foo/bar/fizz.png')
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        key: '/bar/fizz.png',
      })
        .toPromise()
    })
  })

  await t.step('removeObjects', async (t) => {
    await t.step('should remove the objects in the namespace', () => {
      return namespaced.removeObjects({
        ...happyMinio,
        removeObjects: (bucket, keys) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(keys, ['foo/bar/fizz.png', 'foo/fuzz.png'])
          return Promise.resolve()
        },
      })({
        bucket: 'foo',
        keys: ['bar/fizz.png', 'fuzz.png'],
      })
        .toPromise()
    })
  })

  await t.step('getObject', async (t) => {
    await t.step('should get the object in the namespace', () => {
      return namespaced.getObject({
        ...happyMinio,
        getObject: (bucket, key) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(key, 'foo/bar/fizz.png')
          return Promise.resolve(new ReadableWebToNodeStream(new Response('foo').body))
        },
      })({
        bucket: 'foo',
        key: 'bar/fizz.png',
      })
        .toPromise()
    })
  })

  await t.step('getSignedUrl', async (t) => {
    await t.step('should get a signed url in the namespace', () => {
      return namespaced.getSignedUrl({
        ...happyMinio,
        presignedUrl: (method, bucket, key, expires) => {
          assertEquals(method, 'GET')
          // lowercase
          assertEquals(bucket, 'test-namespaced')
          assertEquals(key, 'FOO/bar.png')
          assertEquals(expires, 60 * 5)
          return Promise.resolve('http://presigned')
        },
      })({
        bucket: 'FOO',
        key: 'bar.png',
        method: 'GET',
      })
        .toPromise()
    })
  })

  await t.step('listObjects', async (t) => {
    await t.step('should gather all of the objects in the namespace', () => {
      return namespaced.listObjects({
        listObjects: (bucket, prefix) => {
          assertEquals(bucket, 'test-namespaced')
          assertEquals(prefix, 'foo/crap')

          // Mock stream
          return {
            on: (event, fn) => {
              switch (event) {
                case 'data': {
                  fn({ name: 'foo/crap/fizz.png' })
                  fn({ name: 'foo/crap/bar.png' })
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
        prefix: 'crap',
      })
        .map((res) => {
          assertObjectMatch(res, [{ name: 'fizz.png' }, { name: 'bar.png' }])
        })
        .toPromise()
    })
  })
})
