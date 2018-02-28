package ch.epfl.bluebrain.nexus.sourcing.akka.cache

import cats.{MonadError, Show}
import cats.syntax.functor._

/**
  * A cache definition
  *
  * @tparam F the monadic effect type
  * @tparam K the generic type of the allowed keys to be stored on this cache
  * @tparam V the generic type of the allowed values to be stored on this cache
  */
abstract class Cache[F[_], K: Show, V](implicit F: MonadError[F, Throwable]) {

  /**
    * Fetches the stored value of the provided ''key''.
    *
    * @param key the key from where to obtain the value
    * @return an optional [[V]] instance wrapped in the
    *         abstract ''F[_]'' type if successful, or a [[None]] wrapped within
    *         ''F[_]'' otherwise
    */
  def get(key: K): F[Option[V]]

  /**
    * Fetches the stored value of the provided ''key'' or return the provided ''default''.
    *
    * @param key     the key from where to obtain the value
    * @param default the value to return when ''key'' does not exist
    * @return an [[V]] instance wrapped in the abstract ''F[_]'' type
    */
  def getOrElse(key: K, default: => V): F[V] = get(key).map(_.getOrElse(default))

  /**
    * Stores the provided ''value'' on the ''key'' index.
    *
    * @param key   the key location where to store the ''value''
    * @param value the value to store
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def put(key: K, value: V): F[Unit]

  /**
    * Removes the value of the provided ''key''.
    *
    * @param key the key from where to obtain the value to be removed
    * @return an optional [[Unit]] instance wrapped in the abstract ''F[_]'' type
    */
  def remove(key: K): F[Unit]

}
