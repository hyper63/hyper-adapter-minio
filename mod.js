import { crocks, Minio, R } from './deps.js'

import createAdapter from './adapter.js'
import PORT_NAME from './port_name.js'

const { Either } = crocks
const { Left, Right, of } = Either
const { identity, defaultTo, over, lensProp, mergeRight } = R

export default ({ url, bucketPrefix, useNamespacedBucket }) => {
  const checkBucketPrefix = (config) =>
    config.bucketPrefix && config.bucketPrefix.length <= 32 ? Right(config) : Left({
      message: 'Prefix name: must be a string 1-32 alphanumeric characters',
    })

  const setFromEnv = (config) => mergeRight({ url: Deno.env.get('MINIO_URL') }, config)

  const setUseNamespacedBucket = (config) => mergeRight({ useNamespacedBucket: false }, config)

  const setClient = ({ url }) =>
    over(lensProp('minio'), () => {
      const config = new URL(url)
      return new Minio.Client({
        endPoint: config.hostname,
        accessKey: config.username,
        secretKey: config.password,
        useSSL: config.protocol === 'https:',
        port: Number(config.port),
      })
    })

  return Object.freeze({
    id: 'minio',
    port: PORT_NAME,
    load: (prevLoad) =>
      of(prevLoad) // credentials can be received from a composed plugin
        .map(defaultTo({}))
        .map((prevLoad) => mergeRight(prevLoad, { url, bucketPrefix, useNamespacedBucket }))
        .chain(checkBucketPrefix)
        .map(setFromEnv)
        .map(setUseNamespacedBucket)
        .map(setClient)
        .either(
          (e) => console.log('Error: In Load Method', e.message),
          identity,
        ),
    link: (config) => (_) =>
      createAdapter({
        minio: config.minio,
        bucketPrefix: config.bucketPrefix,
        useNamespacedBucket: config.useNamespacedBucket,
      }),
  })
}
