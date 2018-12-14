package ch.epfl.bluebrain.nexus.sourcing.akka

import akka.actor.ActorSystem
import akka.util.Timeout
import cats.ApplicativeError
import cats.effect.Timer
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig._

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
    retry: RetryStrategyConfig,
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
      lapsedSinceRecoveryCompleted: Option[FiniteDuration],
  )

  /**
    * Retry strategy configuration.
    *
    * @param strategy     the type of strategy; possible options are "never", "once" and "exponential"
    * @param initialDelay the initial delay before retrying that will be multiplied with the 'factor' for each attempt
    *                     (applicable only for strategy "exponential")
    * @param maxRetries   maximum number of retries in case of failure (applicable only for strategy "exponential")
    * @param factor       the exponential factor (applicable only for strategy "exponential")
    */
  final case class RetryStrategyConfig(
      strategy: String,
      initialDelay: FiniteDuration,
      maxRetries: Int,
      factor: Int
  ) {

    /**
      * Computes a retry strategy from the provided configuration.
      */
    def retryStrategy[F[_]: Timer, E](implicit F: ApplicativeError[F, E]): RetryStrategy[F] =
      strategy match {
        case "exponential" =>
          RetryStrategy.exponentialBackoff(initialDelay, maxRetries, factor)
        case "once" =>
          RetryStrategy.once
        case _ =>
          RetryStrategy.never
      }
  }

  /**
    * Configuration for the akka sourcing implementation.
    *
    * @param askTimeout                        maximum duration to wait for a reply when communicating with an aggregate actor
    * @param readJournalPluginId               the id of the read journal for querying across entity event logs
    * @param commandEvaluationMaxDuration      the maximum amount of time allowed for a command to evaluate before cancelled
    * @param commandEvaluationExecutionContext the execution context where command evaluation is executed
    */
  final case class AkkaSourcingConfig(askTimeout: Timeout,
                                      readJournalPluginId: String,
                                      commandEvaluationMaxDuration: FiniteDuration,
                                      commandEvaluationExecutionContext: ExecutionContext)
}
