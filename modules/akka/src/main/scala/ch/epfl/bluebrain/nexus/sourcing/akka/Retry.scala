package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.MonadError
import cats.effect.Timer
import cats.implicits._

import scala.concurrent.duration._

/**
  * A strategy to be applied for retrying computations of values in a <strong>lazy</strong> __F__ effect type.
  *
  * @tparam F the type of the effect
  */
abstract class Retry[F[_], E](implicit F: MonadError[F, E]) {

  /**
    * Returns a new value in the same context type, but with a preconfigured retry mechanism.
    *
    * @param fa the computation value in __F__
    * @tparam A the type of the value
    * @return a new value in the same context type, but with a preconfigured retry mechanism
    */
  def apply[A](fa: F[A]): F[A]

  /**
    * Returns a new value computed from the ''pf''. Retries with a preconfigured retry mechanism
    * if an error [[E]] occurs or if ''pf'' was not defined.
    *
    * @param fa           the computation value in __F__
    * @param pf           a partial function to transform A into B
    * @param onMapFailure an error to fail the computation with when the partial function is not defined
    *                     for the resulting A value
    * @tparam B the type of the output value
    * @return a new value computed from the ''pf'' in the same context type
    */
  def apply[A, B](fa: F[A], pf: PartialFunction[A, B], onMapFailure: => E): F[B] =
    apply(fa.flatMap { a =>
      pf.lift(a) match {
        case Some(b) => F.pure(b)
        case _       => F.raiseError(onMapFailure)
      }
    })
}

object Retry {

  /**
    * Constructs a [[Retry]] from a given strategy
    *
    * @param strategy the strategy to retry
    */
  def apply[F[_], E](strategy: RetryStrategy)(implicit F: MonadError[F, E], T: Timer[F]): Retry[F, E] =
    new Retry[F, E] {
      override def apply[A](fa: F[A]): F[A] = {
        def inner(previousDelay: FiniteDuration, currentRetries: Int): F[A] =
          fa.handleErrorWith { error =>
            strategy.next(previousDelay, currentRetries) match {
              case Some(newDelay) if newDelay.toMillis == 0L => inner(newDelay, currentRetries + 1)
              case Some(newDelay)                            => T.sleep(newDelay) *> inner(newDelay, currentRetries + 1)
              case _                                         => F.raiseError(error)
            }
          }
        inner(previousDelay = 0 millis, currentRetries = 0)
      }
    }
}
