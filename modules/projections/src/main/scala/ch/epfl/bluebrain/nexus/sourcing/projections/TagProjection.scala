package ch.epfl.bluebrain.nexus.sourcing.projections

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.scaladsl.Source
import cats.effect.Effect
import cats.effect.syntax.all._
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Generic tag projection with an initialization function; it provides a source of values of type A.
  */
sealed trait TagProjection[F[_], A] {

  /**
    * @return an initialization function that fetches the value required to initialize the source
    */
  def fetchInit: F[A]

  /**
    * A source generated from the  provided init value
    *
    * @param init initialization value
    */
  def source(init: A): Source[A, _]
}

/**
  * An event with its persistenceId
  *
  * @param persistenceId the event persistenceId
  * @param value         the event value
  * @tparam T the event type
  */
final case class WrappedEvt[T](persistenceId: String, value: T)

/**
  * A sequence of events with an offset
  *
  * @param progress the offset value
  * @param events the sequence of events
  * @tparam T the event type
  */
final case class OffsetEvtBatch[T](progress: ProjectionProgress, events: List[WrappedEvt[T]])

object OffsetEvtBatch {
  def empty[T]: OffsetEvtBatch[T] = OffsetEvtBatch(NoProgress, List.empty[WrappedEvt[T]])
}

object TagProjection {

  private def defaultActorOf(implicit as: ActorSystem): ((Props, String) => ActorRef) = as.actorOf(_, _)

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are NOT persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  // $COVERAGE-OFF$
  final def start[F[_], Event: ClassTag, MappedEvt, Err](config: ProjectionConfig[F, Event, MappedEvt, Err, Volatile])(
      implicit as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {
    val tagProjection: TagProjection[F, ProjectionProgress] = new VolatileTagProjection(config)
    val actorOf                                             = config.actorOf.getOrElse(defaultActorOf)
    StreamSupervisor.start(tagProjection.fetchInit, tagProjection.source, config.name, actorOf)
  }

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are NOT persisted once computed the index function.
    * The projection runs only once on all the cluster
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  // $COVERAGE-OFF$
  final def startSingleton[F[_], Event: ClassTag, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Volatile]
  )(
      implicit as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {
    val tagProjection: TagProjection[F, ProjectionProgress] = new VolatileTagProjection(config)
    val actorOf                                             = config.actorOf.getOrElse(defaultActorOf)
    StreamSupervisor.startSingleton(tagProjection.fetchInit, tagProjection.source, config.name, actorOf)
  }

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are NOT persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  // $COVERAGE-OFF$
  final def delay[F[_], Event: ClassTag, MappedEvt, Err](config: ProjectionConfig[F, Event, MappedEvt, Err, Volatile])(
      implicit as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): F[StreamSupervisor[F, ProjectionProgress]] =
    F.delay(start(config))

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  final def start[F[_], Event: ClassTag, MappedEvt, Err](config: ProjectionConfig[F, Event, MappedEvt, Err, Persist])(
      implicit projections: Projections[F, Event],
      as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {
    val tagProjection: TagProjection[F, ProjectionProgress] = new PersistentTagProjection(config)
    val actorOf                                             = config.actorOf.getOrElse(defaultActorOf)
    StreamSupervisor.start(tagProjection.fetchInit, tagProjection.source, config.name, actorOf)
  }

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function.
    * The projection runs only once on all the cluster
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  final def startSingleton[F[_], Event: ClassTag, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Persist]
  )(
      implicit projections: Projections[F, Event],
      as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {
    val tagProjection: TagProjection[F, ProjectionProgress] = new PersistentTagProjection(config)
    val actorOf                                             = config.actorOf.getOrElse(defaultActorOf)
    StreamSupervisor.startSingleton(tagProjection.fetchInit, tagProjection.source, config.name, actorOf)
  }

  /**
    * Generic tag projection that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    */
  final def delay[F[_], Event: ClassTag, MappedEvt, Err](config: ProjectionConfig[F, Event, MappedEvt, Err, Persist])(
      implicit projections: Projections[F, Event],
      as: ActorSystem,
      sourcingConfig: SourcingConfig,
      F: Effect[F]
  ): F[StreamSupervisor[F, ProjectionProgress]] =
    F.delay(start(config))

  // $COVERAGE-ON$

  abstract class BatchedTagProjection[F[_], Event, MappedEvt, Err, O <: ProgressStorage](
      config: ProjectionConfig[F, Event, MappedEvt, Err, O]
  )(implicit F: Effect[F], T: ClassTag[Event], as: ActorSystem)
      extends TagProjection[F, ProjectionProgress] {

    private[sourcing] implicit val log: LoggingAdapter  = Logging(as, TagProjection.getClass)
    private[sourcing] implicit val retry: Retry[F, Err] = config.retry
    private[sourcing] type IdentifiedEvent = (String, Event, MappedEvt)

    private[sourcing] def batchedSource(
        initialProgress: ProjectionProgress
    ): Source[(ProjectionProgress, List[IdentifiedEvent]), NotUsed] = {
      config.mapInitialProgress(initialProgress).toIO.unsafeRunSync()
      PersistenceQuery(as)
        .readJournalFor[EventsByTagQuery](config.pluginId)
        .eventsByTag(config.tag, initialProgress.offset)
        .map(castEvent)
        .mapAsync(1) {
          case (id, Some(event)) => mapEvent(id, event)
          case (id, None)        => Future.successful((id, None, None))
        }
        .scan((initialProgress, None: Option[String], None: Option[Event], None: Option[MappedEvt])) {
          case ((progress, _, _, _), (envelope, Some(event), Some(mappedEvt))) =>
            (
              OffsetProgress(envelope.offset, progress.processedCount + 1, progress.discardedCount),
              Some(envelope.persistenceId),
              Some(event),
              Some(mappedEvt)
            )
          case ((progress, _, _, _), (envelope, eventOpt, mappedOpt)) =>
            (
              OffsetProgress(envelope.offset, progress.processedCount + 1, progress.discardedCount + 1),
              Some(envelope.persistenceId),
              eventOpt,
              mappedOpt
            )
        }
        .flatMapConcat {
          case (progress, Some(id), eventOpt, mappedOpt) => Source.single((progress, id, eventOpt, mappedOpt))
          case _                                         => Source.empty
        }
        .groupedWithin(config.batch, config.batchTo)
        .filter(_.nonEmpty)
        .map(mapBatchOffset)
    }

    private def castEvent(envelope: EventEnvelope): (EventEnvelope, Option[Event]) = {
      log.debug("Processing event for persistence id '{}', seqNr '{}'", envelope.persistenceId, envelope.sequenceNr)
      envelope.event match {
        case T(casted) => (envelope, Some(casted))
        case _ =>
          log.warning(
            "Some of the Events on the list are not compatible with type '{}', skipping...",
            T.runtimeClass.getSimpleName
          )
          (envelope, None)
      }
    }

    private def mapEvent(env: EventEnvelope, event: Event): Future[(EventEnvelope, Option[Event], Option[MappedEvt])] =
      config
        .mapping(event)
        .retry
        .map {
          case Some(evtMapped) => (env, Some(event): Option[Event], Some(evtMapped))
          case None =>
            log.debug(
              "Mapping event with id '{}' and value '{}' failed '{}'",
              env.persistenceId,
              event,
              "Mapping failed"
            )
            (env, Some(event), None)
        }
        .recoverWith(logError(env, event))
        .toIO
        .unsafeToFuture()

    private def mapBatchOffset(
        batched: Seq[(ProjectionProgress, String, Option[Event], Option[MappedEvt])]
    ): (ProjectionProgress, List[(String, Event, MappedEvt)]) = {
      val progress = batched.lastOption.map { case (off, _, _, _) => off }.getOrElse(NoProgress)
      (progress, batched.collect { case (_, id, Some(event), Some(mappedEvent)) => (id, event, mappedEvent) }.toList)
    }

    private def logError(
        env: EventEnvelope,
        event: Event
    ): PartialFunction[Throwable, F[(EventEnvelope, Option[Event], Option[MappedEvt])]] = {
      case err =>
        log.error(
          err,
          "Indexing event with id '{}' and value '{}' failed '{}'",
          env.persistenceId,
          event,
          err.getMessage
        )
        F.pure((env, Some(event), None))
    }

  }

  /**
    * Generates a source that reads from PersistenceQuery the events with the provided tag. The progress is stored and the errors logged. The different stages are represented below:
    *
    * +----------------------------+    +------------+    +-----------+    +-------------+    +-------------+    +-------------+    +--------------+
    * | eventsByTag(currentOffset) |--->| castEvents |--->| mapEvents |--->| batchEvents |--->| indexEvents |--->| mapProgress |--->|storeProgress |
    * +----------------------------+    +------------+    +-----------+    +-------------+    +-------------+    +-------------+    +--------------+
    *
    */
  final class PersistentTagProjection[F[_], Event, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Persist]
  )(implicit projections: Projections[F, Event], F: Effect[F], T: ClassTag[Event], as: ActorSystem)
      extends BatchedTagProjection(config) {

    def fetchInit: F[ProjectionProgress] =
      if (config.storage.restart)
        config.init.retry >> F.pure(NoProgress)
      else
        config.init.retry >> projections.progress(config.name).retry

    def source(initialProgress: ProjectionProgress): Source[ProjectionProgress, NotUsed] =
      batchedSource(initialProgress)
        .mapAsync(1) {
          case (progress, events) => indexEvents(progress, events)
        }
        .mapAsync(1) { progress =>
          config.mapProgress(progress).map(_ => progress).toIO.unsafeToFuture()
        }
        .mapAsync(1)(storeProgress)

    private def indexEvents(progress: ProjectionProgress, events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(recoverIndex(progress, events))
        .map(_ => progress)
        .toIO
        .unsafeToFuture()

    private def storeProgress(progress: ProjectionProgress): Future[ProjectionProgress] =
      projections.recordProgress(config.name, progress).retry.map(_ => progress).toIO.unsafeToFuture()

    private def recoverIndex(
        progress: ProjectionProgress,
        events: List[IdentifiedEvent]
    ): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.traverse {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed'{}'", id, event, err.getMessage)
            projections.recordFailure(config.name, id, progress, event)
        } >> F.unit
    }
  }

  /**
    * Generates a source that reads from PersistenceQuery the events with the provided tag. The different stages are represented below:
    *
    * +-----------------------+    +-----------+    +-----------+    +-------------+    +-------------+    +-------------+
    * | eventsByTag(NoOffset) |--->| castEvent |--->| mapEvents |--->| batchEvents |--->| mapProgress |--->| indexEvents |
    * +-----------------------+    +-----------+    +-----------+    +-------------+    +-------------+    +-------------+
    *
    */
  final class VolatileTagProjection[F[_], Event, MappedEvt, Err](
      config: ProjectionConfig[F, Event, MappedEvt, Err, Volatile]
  )(implicit F: Effect[F], T: ClassTag[Event], as: ActorSystem)
      extends BatchedTagProjection(config) {

    def fetchInit: F[ProjectionProgress] = config.init.retry >> F.pure(NoProgress)

    private def indexEvents(progress: ProjectionProgress, events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(logError(events))
        .map(_ => progress)
        .toIO
        .unsafeToFuture()

    def source(initialProgress: ProjectionProgress): Source[ProjectionProgress, NotUsed] = {
      batchedSource(initialProgress)
        .mapAsync(1) {
          case (progress, events) => indexEvents(progress, events)
        }
        .mapAsync(1) { progress =>
          config.mapProgress(progress).map(_ => progress).toIO.unsafeToFuture()
        }
    }

    private def logError(events: List[IdentifiedEvent]): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.foreach {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed '{}'", id, event, err.getMessage)
        }
        F.unit
    }
  }
}
