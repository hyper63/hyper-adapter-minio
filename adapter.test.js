import { assertEquals, assertObjectMatch } from './dev_deps.js'
import { ReadableWebToNodeStream } from './deps.js'

import adapter from './adapter.js'

Deno.test('adapter', async (t) => {
  const happyMinio = {
    makeBucket: (name) => {
      if (name === 'test-exists') return Promise.reject({ code: 'BucketAlreadyExists' })
      return Promise.resolve()
    },
    removeBucket: () => Promise.resolve(),
    bucketExists: () => Promise.resolve(true),
    listBuckets: () =>
      Promise.resolve([
        { name: 'test-foo' },
        { name: 'test-bar' },
      ]),
    getObject: () => {
      return Promise.resolve(
        new ReadableWebToNodeStream(new Response(JSON.stringify({ fizz: 'buzz' })).body),
      )
    },
    putObject: () => Promise.resolve(),
    removeObject: () => {
      return Promise.resolve()
    },
    removeObjects: (bucket, keys) => {
      if (bucket === 'test-removeBucketTest') {
        assertEquals(keys, ['foo', 'bar'])
      }
      return Promise.resolve()
    },
    listObjects: () => {
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
    presignedUrl: () => {
      return Promise.resolve('http://presigned')
    },
  }

  const a = adapter({
    minio: happyMinio,
    bucketPrefix: 'test',
    region: 'us-east-2',
    useNamespacedBucket: false,
  })

  await t.step('makeBucket', async (t) => {
    await t.step('should return whether the bucket was created successfully', () => {
      return a.makeBucket('foo')
        .then((res) => assertEquals(res, { ok: true }))
    })

    await t.step('should return a HyperErr if the bucket already exists', () => {
      return a.makeBucket('exists')
        .then((res) => assertObjectMatch(res, { ok: false, status: 409 }))
    })
  })

  await t.step('removeBucket', async (t) => {
    await t.step('should return whether the bucket was removed successfully', () => {
      return a.removeBucket('foo')
        .then((res) => assertEquals(res, { ok: true }))
    })

    await t.step('should remove all objects in the bucket before removing', () => {
      return a.removeBucket('removeBucketTest')
        .then((res) => assertEquals(res, { ok: true }))
    })
  })

  await t.step('listBuckets', async (t) => {
    await t.step('should return the names of the buckets', () => {
      return a.listBuckets()
        .then((res) =>
          assertObjectMatch(res, {
            ok: true,
            buckets: [
              'test-foo',
              'test-bar',
            ],
          })
        )
    })
  })

  await t.step('putObject', async (t) => {
    await t.step('should return that the object was put successfully', () => {
      return a.putObject({
        bucket: 'foo',
        object: 'bar.png',
        stream: new Response(JSON.stringify({ foo: 'bar' })).body,
      })
        .then((res) => assertEquals(res, { ok: true }))
    })

    await t.step('should return the presigned url', () => {
      return a.putObject({
        bucket: 'foo',
        object: 'bar.png',
        useSignedUrl: true,
      })
        .then((res) => assertEquals(res, { ok: true, url: 'http://presigned' }))
    })
  })

  await t.step('removeObject', async (t) => {
    await t.step('should return that the object was removed successfully', () => {
      return a.removeObject({
        bucket: 'foo',
        object: 'bar.png',
      })
        .then((res) => assertEquals(res, { ok: true }))
    })
  })

  await t.step('getObject', async (t) => {
    await t.step('should return the stream of the object', () => {
      return a.getObject({
        bucket: 'foo',
        object: 'bar.png',
      })
        .then(async (res) => {
          const object = await (new Response(res).json())
          assertEquals(object, { fizz: 'buzz' })
        })
    })

    await t.step('should return the presigned url', () => {
      return a.getObject({
        bucket: 'foo',
        object: 'bar.png',
        useSignedUrl: true,
      })
        .then((res) => assertEquals(res, { ok: true, url: 'http://presigned' }))
    })
  })

  await t.step('listObjects', async (t) => {
    await t.step('should return names of objects in the bucket', () => {
      return a.listObjects({
        bucket: 'foo',
        prefix: 'fizz',
      })
        .then((res) => {
          assertEquals(res, { ok: true, objects: ['foo', 'bar'] })
        })
    })
  })
})
