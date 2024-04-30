import { crocks, Minio, R } from './deps.js'

import createAdapter from './adapter.js'
import PORT_NAME from './port_name.js'

const { Either } = crocks
const { Left, Right, of } = Either
const { identity, defaultTo, over, lensProp, mergeRight } = R

export default ({ url, region, bucketPrefix, useNamespacedBucket }) => {
  const checkBucketPrefix = (config) =>
    config.bucketPrefix && config.bucketPrefix.length <= 32 ? Right(config) : Left({
      message: 'Prefix name: must be a string 1-32 alphanumeric characters',
    })

  const setFromEnv = (config) =>
    mergeRight({
      url: Deno.env.get('MINIO_URL'),
      region: Deno.env.get('MINIO_REGION'),
      // optional. Credentials in MINIO_URL take precedent
      accessKey: Deno.env.get('MINIO_ROOT_USER'),
      secretKey: Deno.env.get('MINIO_ROOT_PASSWORD'),
    }, config)

  const setUseNamespacedBucket = (config) => mergeRight({ useNamespacedBucket: false }, config)

  const setClient = (config) =>
    over(
      lensProp('minio'),
      () => {
        const minioConfig = new URL(config.url)
        return new Minio.Client({
          endPoint: minioConfig.hostname,
          region: config.region,
          // Fallback to credentials pulled from env, if none in MINIO_URL
          accessKey: minioConfig.username || config.accessKey,
          secretKey: minioConfig.password || config.secretKey,
          useSSL: minioConfig.protocol === 'https:',
          port: Number(minioConfig.port) || minioConfig.protocol === 'https:' ? 443 : 80,
        })
      },
      config,
    )

  return Object.freeze({
    id: 'minio',
    port: PORT_NAME,
    load: (prevLoad) =>
      of(prevLoad) // credentials can be received from a composed plugin
        .map(defaultTo({}))
        .map((prevLoad) => mergeRight(prevLoad, { url, region, bucketPrefix, useNamespacedBucket }))
        .chain(checkBucketPrefix)
        .map(setFromEnv)
        .map(setUseNamespacedBucket)
        .map(setClient)
        .either(
          (e) => console.log('Error: In Load Method', e.message),
          identity,
        ),
    link: (config) => (_) => {
      return createAdapter({
        minio: config.minio,
        region: config.region,
        bucketPrefix: config.bucketPrefix,
        useNamespacedBucket: config.useNamespacedBucket,
      })
    },
  })
}
