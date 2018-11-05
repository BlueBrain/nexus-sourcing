package ch.epfl.bluebrain.nexus.sourcing.akka

import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Configuration for the akka sourcing implementation.
  *
  * @param askTimeout                   maximum duration to wait for a reply when communicating with an aggregate actor
  * @param readJournalPluginId          the id of the read journal for querying across entity event logs
  * @param commandEvaluationMaxDuration the maximum amount of time allowed for a command to evaluate before cancelled
  * @param commandEvalExecutionContext  the execution context where command evaluation is executed
  */
final case class AkkaSourcingConfig(askTimeout: Timeout,
                                    readJournalPluginId: String,
                                    commandEvaluationMaxDuration: FiniteDuration,
                                    commandEvalExecutionContext: ExecutionContext)
