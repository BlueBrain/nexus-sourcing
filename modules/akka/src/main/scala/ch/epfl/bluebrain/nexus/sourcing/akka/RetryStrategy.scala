package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.ApplicativeError
import cats.arrow.FunctionK
import cats.effect.Timer
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

/**
  * A strategy to be applied for retrying computations of values in a <strong>lazy</strong> __F__ effect type.
  *
  * @tparam F the type of the effect
  */
trait RetryStrategy[F[_]] {

  /**
    * Returns a new value in the same context type, but with a preconfigured retry mechanism.
    *
    * @param fa the computation value in __F__
    * @tparam A the type of the value
    * @return a new value in the same context type, but with a preconfigured retry mechanism
    */
  def apply[A](fa: F[A]): F[A]

}

object RetryStrategy {

  /**
    * Lifts a polymorphic function into a RetryStrategy.
    *
    * @param f the polymorphic function to lift into a RetryStrategy.
    * @see [[cats.arrow.FunctionK.lift]]
    */
  def apply[F[_]](f: FunctionK[F, F]): RetryStrategy[F] = new RetryStrategy[F] {
    override def apply[A](fa: F[A]): F[A] = f(fa)
  }

  /**
    * No retry strategy.
    */
  def never[F[_]]: RetryStrategy[F] = new RetryStrategy[F] {
    override def apply[A](fa: F[A]): F[A] = fa
  }

  /**
    * A strategy that retries the computation once.
    *
    * @param F  evidence that __F__ has an [[ApplicativeError]]
    * @tparam F the effect type
    * @tparam E the error type
    * @return a strategy that retries the computation once
    */
  def once[F[_], E](implicit F: ApplicativeError[F, E]): RetryStrategy[F] = new RetryStrategy[F] {
    override def apply[A](fa: F[A]): F[A] =
      fa.handleErrorWith { _ =>
        fa
      }
  }

  /**
    * A retry strategy with exponential backoff.
    *
    * @param initialDelay the initial delay to apply before retrying the computation
    * @param maxRetries   the maximum number of tries
    * @param factor       the delay increase factor in between retries
    * @param T            a timer for __F__
    * @param F            an applicative error for __F__
    * @tparam F           the effect type
    * @tparam E           the error type
    * @return a retry strategy with exponential backoff
    */
  def exponentialBackoff[F[_], E](
      initialDelay: FiniteDuration,
      maxRetries: Int,
      factor: Int = 2
  )(implicit T: Timer[F], F: ApplicativeError[F, E]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def retry[A](fa: F[A], delay: FiniteDuration, retries: Int): F[A] =
        fa.handleErrorWith { error =>
          if (retries > 0)
            T.sleep(delay) *> retry(fa, delay * factor.toLong, retries - 1)
          else
            F.raiseError(error)
        }

      override def apply[A](fa: F[A]): F[A] =
        retry(fa, initialDelay, maxRetries)
    }
}
