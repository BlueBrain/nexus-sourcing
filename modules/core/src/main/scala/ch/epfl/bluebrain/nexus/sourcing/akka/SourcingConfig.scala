package ch.epfl.bluebrain.nexus.sourcing.akka

import akka.actor.ActorSystem
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig._
import ch.epfl.bluebrain.nexus.sourcing.retry.RetryStrategy
import ch.epfl.bluebrain.nexus.sourcing.retry.RetryStrategy._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Sourcing configuration.
  *
  * @param askTimeout                        timeout for the message exchange with the aggregate actor
  * @param queryJournalPlugin                the query (read) plugin journal id
  * @param commandEvaluationTimeout          timeout for evaluating commands
  * @param commandEvaluationExecutionContext the execution context where commands are to be evaluated
  * @param shards                            the number of shards for the aggregate
  * @param passivation                       the passivation strategy configuration
  * @param retry                             the retry strategy configuration
  */
final case class SourcingConfig(
    askTimeout: FiniteDuration,
    queryJournalPlugin: String,
    commandEvaluationTimeout: FiniteDuration,
    commandEvaluationExecutionContext: String,
    shards: Int,
    passivation: PassivationStrategyConfig,
    retry: RetryStrategyConfig
) {

  /**
    * Computes an [[AkkaSourcingConfig]] using an implicitly available actor system.
    *
    * @param as the underlying actor system
    */
  def akkaSourcingConfig(implicit as: ActorSystem): AkkaSourcingConfig =
    AkkaSourcingConfig(
      askTimeout = Timeout(askTimeout),
      readJournalPluginId = queryJournalPlugin,
      commandEvaluationMaxDuration = commandEvaluationTimeout,
      commandEvaluationExecutionContext =
        if (commandEvaluationExecutionContext == "akka") as.dispatcher
        else ExecutionContext.global
    )

  /**
    * Computes a passivation strategy from the provided configuration and the passivation evaluation function.
    *
    * @param shouldPassivate whether aggregate should passivate after a message exchange
    * @tparam State   the type of the aggregate state
    * @tparam Command the type of the aggregate command
    */
  def passivationStrategy[State, Command](
      shouldPassivate: (String, String, State, Option[Command]) => Boolean =
        (_: String, _: String, _: State, _: Option[Command]) => false
  ): PassivationStrategy[State, Command] =
    PassivationStrategy(
      passivation.lapsedSinceLastInteraction,
      passivation.lapsedSinceRecoveryCompleted,
      shouldPassivate
    )
}

object SourcingConfig {

  /**
    * Partial configuration for aggregate passivation strategy.
    *
    * @param lapsedSinceLastInteraction   duration since last interaction with the aggregate after which the passivation
    *                                     should occur
    * @param lapsedSinceRecoveryCompleted duration since the aggregate recovered after which the passivation should
    *                                     occur
    */
  final case class PassivationStrategyConfig(
      lapsedSinceLastInteraction: Option[FiniteDuration],
      lapsedSinceRecoveryCompleted: Option[FiniteDuration]
  )

  /**
    * Retry strategy configuration.
    *
    * @param strategy     the type of strategy; possible options are "never", "once" and "exponential"
    * @param initialDelay the initial delay before retrying that will be multiplied with the 'factor' for each attempt
    *                     (applicable for strategy "exponential" and "linear")
    * @param maxDelay     the maximum delay (applicable for strategy "exponential" and "linear")
    * @param maxRetries   maximum number of retries in case of failure (applicable only for strategy "exponential")
    * @param factor       the exponential factor (applicable only for strategy "exponential")
    * @param increment    the linear increment (applicable only for strategy "linear")
    */
  final case class RetryStrategyConfig(
      strategy: String,
      initialDelay: FiniteDuration,
      maxDelay: FiniteDuration,
      maxRetries: Int,
      factor: Double,
      increment: FiniteDuration
  ) {

    /**
      * Computes a retry strategy from the provided configuration.
      */
    def retryStrategy: RetryStrategy =
      strategy match {
        case "exponential" => Backoff(initialDelay, maxDelay, factor, maxRetries)
        case "linear"      => Linear(initialDelay, maxDelay, increment, maxRetries)
        case "once"        => Once(initialDelay)
        case _             => Never
      }
  }

}
