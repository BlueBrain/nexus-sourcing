package ch.epfl.bluebrain.nexus.sourcing.projections

import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig

import scala.concurrent.duration.FiniteDuration

/**
  * Indexing configuration
  *
  * @param batch        the maximum number of events taken on each batch
  * @param batchTimeout the maximum amount of time to wait for the number of events to be taken on each batch
  * @param retry        the retry configuration when indexing failures
  */
final case class IndexingConfig(batch: Int, batchTimeout: FiniteDuration, retry: RetryStrategyConfig)
