package ch.epfl.bluebrain.nexus.sourcing.retry

import com.github.ghik.silencer.silent

import scala.concurrent.duration._
import scala.util.Random

/**
  * Enumeration of retry strategy types.
  */
sealed trait RetryStrategy extends Product with Serializable {

  /**
    * Given a current delay value provides the next delay.
    *
    * @param current the current delay
    * @param retries the current amount of retries
    */
  def next(current: FiniteDuration, retries: Int): Option[FiniteDuration]
}

object RetryStrategy {

  /**
    * An exponential backoff delay increment strategy.
    *
    * @param initialDelay the initial delay
    * @param maxDelay     the maximum delay accepted
    * @param randomFactor the random variation on delay
    * @param maxRetries   the maximum number of retries
    */
  final case class Backoff(
      initialDelay: FiniteDuration,
      maxDelay: FiniteDuration,
      randomFactor: Double,
      maxRetries: Int = Int.MaxValue
  ) extends RetryStrategy {
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = {
      if (retries == 0) Some(initialDelay)
      else if (retries >= maxRetries) None
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
    * A linear delay increment strategy.
    *
    * @param initialDelay the initial delay
    * @param maxDelay     the maximum delay accepted
    * @param increment    the linear increment on delay
    * @param maxRetries   the maximum number of retries
    */
  final case class Linear(
      initialDelay: FiniteDuration,
      maxDelay: FiniteDuration,
      increment: FiniteDuration = 1 second,
      maxRetries: Int = Int.MaxValue
  ) extends RetryStrategy {

    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = {
      if (retries == 0) Some(initialDelay)
      else if (retries >= maxRetries) None
      else Some((current + increment).min(maxDelay))
    }
  }

  /**
    * A once delay increment strategy.
    *
    * @param delay the duration to sleep before attempting again
    */
  final case class Once(delay: FiniteDuration = 0 millis) extends RetryStrategy {

    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] =
      if (retries == 0) Some(delay)
      else None
  }

  /**
    * A never strategy.
    */
  final case object Never extends RetryStrategy {
    @silent
    override def next(current: FiniteDuration, retries: Int): Option[FiniteDuration] = None
  }
}
