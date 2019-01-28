package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.MonadError
import cats.effect.Timer

import scala.concurrent.duration._
import scala.util.Random

/**
  * Enumeration of retry strategy types
  */
sealed trait RetryStrategy extends Product with Serializable {

  /**
    * Given a current delay value provides the next delay
    * @param current the current delay
    * @param retries the current amount of retries
    */
  def next(current: FiniteDuration, retries: Int): Option[FiniteDuration]

  /**
    * Generates a retryer from the current strategy
    */
  def retryer[F[_]: Timer, E](implicit F: MonadError[F, E]): Retryer[F] = Retryer.apply[F, E](this)

  /**
    * Generates a map retryer from the current strategy
    */
  def retryerMap[F[_]: Timer, E](implicit F: MonadError[F, E]): RetryerMap[F, E] = RetryerMap.apply[F, E](this)
}

object RetryStrategy {

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
      extends RetryStrategy {
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
      extends RetryStrategy {
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
  final case class Once(init: FiniteDuration) extends RetryStrategy {

    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] =
      if (retries == 0) Some(init)
      else None
  }

  /**
    * A never strategy
    *
    */
  final case object Never extends RetryStrategy {
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = None
  }
}
