package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.MonadError
import cats.arrow.FunctionK
import cats.effect.Timer
import cats.implicits._

import scala.concurrent.duration._

/**
  * A strategy to be applied for retrying computations of values in a <strong>lazy</strong> __F__ effect type.
  *
  * @tparam F the type of the effect
  */
trait Retryer[F[_]] {

  /**
    * Returns a new value in the same context type, but with a preconfigured retry mechanism.
    *
    * @param fa the computation value in __F__
    * @tparam A the type of the value
    * @return a new value in the same context type, but with a preconfigured retry mechanism
    */
  def apply[A](fa: F[A]): F[A]

}

object Retryer {

  /**
    * Lifts a polymorphic function into a RetryStrategy.
    *
    * @param f the polymorphic function to lift into a RetryStrategy.
    * @see [[cats.arrow.FunctionK.lift]]
    */
  def apply[F[_]](f: FunctionK[F, F]): Retryer[F] = new Retryer[F] {
    override def apply[A](fa: F[A]): F[A] = f(fa)
  }

  /**
    * Constructs a [[Retryer]] from a given strategy
    *
    * @param strategy the strategy to retry
    */
  def apply[F[_], E](strategy: RetryStrategy)(implicit F: MonadError[F, E], T: Timer[F]): Retryer[F] =
    new Retryer[F] {

      override def apply[A](fa: F[A]) = {
        def inner(previousDelay: FiniteDuration, currentRetries: Int): F[A] =
          fa.handleErrorWith { error =>
            strategy.next(previousDelay, currentRetries) match {
              case Some(newDelay) => T.sleep(newDelay) *> inner(newDelay, currentRetries + 1)
              case _              => F.raiseError(error)
            }
          }

        inner(previousDelay = 0 millis, currentRetries = 0)
      }
    }
}

abstract class RetryerMap[F[_], E] extends Retryer[F] {

  /**
    * Returns a new value computed from the ''pf''. Retries with a preconfigured retry mechanism
    * if an error [[E]] occurs or if ''pf'' was not defined.
    *
    * @param fa the computation value in __F__
    * @param pf a partial function to transform A into B
    * @tparam A the type of the value
    * @tparam B the type of the output value
    * @return a new value computed from the ''pf'' in the same context type
    */
  def apply[A, B](fa: F[A], pf: PartialFunction[A, B], onMapFailure: => E): F[B]
}
object RetryerMap {

  /**
    * Constructs a [[RetryerMap]] from a given strategy
    *
    * @param strategy the strategy to retry
    */
  def apply[F[_], E](strategy: RetryStrategy)(implicit F: MonadError[F, E], T: Timer[F]): RetryerMap[F, E] =
    new RetryerMap[F, E] {

      private val underlying = Retryer[F, E](strategy)

      override def apply[A](fa: F[A]) = underlying.apply(fa)

      override def apply[A, B](fa: F[A], pf: PartialFunction[A, B], onMapFailure: => E): F[B] = {
        def inner(previousDelay: FiniteDuration, currentRetries: Int): F[B] = {
          val mapped: F[B] = fa.flatMap { a =>
            pf.lift(a) match {
              case Some(b) => F.pure(b)
              case _       => F.raiseError(onMapFailure)
            }
          }
          mapped.handleErrorWith { error =>
            strategy.next(previousDelay, currentRetries) match {
              case Some(newDelay) => T.sleep(newDelay) *> inner(newDelay, currentRetries + 1)
              case _              => F.raiseError(error)
            }
          }
        }

        inner(previousDelay = 0 millis, currentRetries = 0)
      }
    }
}
