import { crocks, HyperErr, isHyperErr, R } from '../deps.js'

const { Async, Result, resultToAsync } = crocks
const { includes, ifElse, identity, find, __, propSatisfies, allPass, has } = R

export const isTokenErr = allPass([
  has('code'),
  propSatisfies(
    (s) =>
      find(includes(__, s), [
        'InvalidAccessKeyId',
        'InvalidToken',
        'ExpiredToken',
        'SignatureDoesNotMatch',
        'Http400',
      ]),
    'code',
  ),
])

export const isBucketExistsErr = allPass([
  has('code'),
  propSatisfies(
    (s) =>
      find(includes(__, s), [
        'BucketAlreadyExists',
        'BucketAlreadyOwnedByYou',
      ]),
    'code',
  ),
])

export const isBucketDneErr = allPass([
  has('code'),
  propSatisfies(
    (s) =>
      find(includes(__, s), [
        'BucketAlreadyExists',
        'BucketAlreadyOwnedByYou',
      ]),
    'code',
  ),
])

export const isNoSuchKeyErr = allPass([
  has('code'),
  propSatisfies(
    (s) =>
      find(includes(__, s), [
        'NoSuchKey',
      ]),
    'code',
  ),
])

export const handleHyperErr = ifElse(
  isHyperErr,
  Async.Resolved,
  Async.Rejected,
)

export const checkName = (name) => {
  return resultToAsync(
    Result.Err([])
      .alt(
        includes('..', name) ? Result.Err(['name cannot contain \'..\'']) : Result.Err([]),
      )
      .bichain(
        (errs) => errs.length ? Result.Err(errs) : Result.Ok(name),
        Result.Ok,
      )
      .bimap(
        (errs) => HyperErr(errs.join(', ')), // combine errs into string
        identity,
      ),
  )
}

export const asyncifyHandle = (fn) =>
  Async.fromPromise(
    (...args) =>
      Promise.resolve(fn(...args))
        .catch((err) => {
          if (isTokenErr(err)) throw HyperErr({ status: 500, msg: 'credentials are invalid' })
          throw err
        }),
  )
