package ch.epfl.bluebrain.nexus.sourcing.projections

import akka.actor.ActorSystem
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage.{Persist, Volatile}
import ch.epfl.bluebrain.nexus.sourcing.projections.StreamByTag.{PersistentStreamByTag, VolatileStreamByTag}
import shapeless.Typeable

/**
  * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
  * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
  * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
  * the events are skipped.
  */
object SequentialTagIndexer {

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are NOT persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  // $COVERAGE-OFF$
  final def start[F[_]: Effect, Event: Typeable, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Volatile])(
      implicit as: ActorSystem,
      sourcingConfig: SourcingConfig): StreamCoordinator[F, ProjectionProgress] = {
    val streamByTag: StreamByTag[F, ProjectionProgress] = new VolatileStreamByTag(config)
    StreamCoordinator.start(streamByTag.fetchInit, streamByTag.source, config.name)
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  final def start[F[_]: Effect, Event: Typeable, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Persist])(
      implicit projections: Projections[F, Event],
      as: ActorSystem,
      sourcingConfig: SourcingConfig): StreamCoordinator[F, ProjectionProgress] = {
    val streamByTag: StreamByTag[F, ProjectionProgress] = new PersistentStreamByTag(config)
    StreamCoordinator.start(streamByTag.fetchInit, streamByTag.source, config.name)
  }
  // $COVERAGE-ON$
}
