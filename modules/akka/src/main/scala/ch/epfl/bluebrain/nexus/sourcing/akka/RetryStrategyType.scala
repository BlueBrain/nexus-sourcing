package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.MonadError
import cats.effect.Timer

import scala.concurrent.duration._
import scala.util.Random

sealed trait RetryStrategyType extends Product with Serializable {

  /**
    * Given a current delay value provides the next delay
    * @param current the current delay
    * @param retries the current amount of retries
    */
  def next(current: FiniteDuration, retries: Int): Option[FiniteDuration]

  def retryOn[F[_]: Timer, E](implicit F: MonadError[F, E]): Retryer[F, E] = new Retryer[F, E](this)

}

object RetryStrategyType {

  /**
    * An exponential backoff delay increment strategy.
    *
    * @param init         the initial delay
    * @param maxDelay     the maximum delay accepted
    * @param maxRetries   the maximum number of retries
    * @param randomFactor the random variation on delay
    */
  final case class Backoff private (init: FiniteDuration,
                                    maxDelay: FiniteDuration,
                                    maxRetries: Int,
                                    randomFactor: Double)
      extends RetryStrategyType {
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = {
      if (retries == 0)
        Some(init)
      else if (retries >= maxRetries)
        None
      else {
        val cappedRandomFactor = randomFactor.min(1.0).max(0.0)
        val minJitter          = 1 - cappedRandomFactor
        val maxJitter          = 1 + cappedRandomFactor
        val nextDelay          = 2 * (minJitter + (maxJitter - minJitter) * Random.nextDouble) * current.max(1 millis)
        Some(nextDelay.min(maxDelay).toMillis millis)
      }
    }
  }

  /**
    * A linear delay increment strategy
    *
    * @param init       the initial delay
    * @param maxDelay   the maximum delay accepted
    * @param maxRetries the maximum number of retries
    * @param increment  the linear increment on delay
    */
  final case class Linear(init: FiniteDuration,
                          maxDelay: FiniteDuration,
                          maxRetries: Int = Int.MaxValue,
                          increment: FiniteDuration = 1 second)
      extends RetryStrategyType {
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = {
      if (retries == 0) Some(init)
      else if (retries >= maxRetries) None
      else Some((current + increment).min(maxDelay))
    }
  }

  /**
    * A once delay increment strategy
    *
    * @param init the initial delay
    */
  final case class Once(init: FiniteDuration) extends RetryStrategyType {

    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] =
      if (retries == 0) Some(init)
      else None
  }

  /**
    * A never strategy
    *
    */
  final case object Never extends RetryStrategyType {
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = None
  }
}
