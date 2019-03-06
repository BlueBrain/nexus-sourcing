package ch.epfl.bluebrain.nexus.sourcing

import _root_.akka.NotUsed
import _root_.akka.actor.ActorSystem
import _root_.akka.event.Logging
import _root_.akka.persistence.query.scaladsl.EventsByTagQuery
import _root_.akka.persistence.query.{EventEnvelope, PersistenceQuery}
import _root_.akka.stream.scaladsl.Source
import cats.effect.Effect
import cats.effect.syntax.all._
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress._
import ch.epfl.bluebrain.nexus.sourcing.persistence._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Encoder
import shapeless.Typeable

import scala.concurrent.Future

/**
  *
  * Enumeration of stream by tag types.
  */
sealed trait StreamByTag[F[_], A] {

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
  * @param offset the offset value
  * @param events the sequence of events
  * @tparam T the event type
  */
final case class OffsetEvtBatch[T](offset: ProjectionProgress, events: List[WrappedEvt[T]])

object OffsetEvtBatch {
  def empty[T]: OffsetEvtBatch[T] = OffsetEvtBatch(NoProgress, List.empty[WrappedEvt[T]])
}

object StreamByTag {

  abstract class BatchedStreamByTag[F[_], Event, MappedEvt, Err, O <: OffsetStorage](
      config: IndexerConfig[F, Event, MappedEvt, Err, O])(implicit F: Effect[F], T: Typeable[Event], as: ActorSystem) {

    private[sourcing] implicit val log                  = Logging(as, SequentialTagIndexer.getClass)
    private[sourcing] implicit val retry: Retry[F, Err] = config.retry
    private[sourcing] type IdentifiedEvent = (String, Event, MappedEvt)

    private[sourcing] def batchedSource(
        initialProgress: ProjectionProgress): Source[(ProjectionProgress, List[IdentifiedEvent]), NotUsed] = {
      config.mapInitialProgress(initialProgress).toIO.unsafeRunSync()
      PersistenceQuery(as)
        .readJournalFor[EventsByTagQuery](config.pluginId)
        .eventsByTag(config.tag, initialProgress.offset)
        .scan((initialProgress, None: Option[EventEnvelope])) { (previous, envelope) =>
          previous match {
            case (progress, _) =>
              (OffsetProgress(envelope.offset, progress.processedCount + 1, progress.discardedCount), Some(envelope))
          }

        }
        .flatMapConcat {
          case (progress, Some(envelope)) => castEvent(progress, envelope)
          case (_, None)                  => Source.empty
        }
        .mapAsync(1) {
          case (offset, id, Some(event)) => mapEvent(offset, id, event)
          case (offset, id, None)        => Future.successful((offset, id, None, None))
        }
        .groupedWithin(config.batch, config.batchTo)
        .filter(_.nonEmpty)
        .map(mapBatchOffset)
    }

    private def castEvent(progress: ProjectionProgress,
                          envelope: EventEnvelope): Source[(ProjectionProgress, String, Option[Event]), NotUsed] = {
      log.debug("Processing event for persistence id '{}', seqNr '{}'", envelope.persistenceId, envelope.sequenceNr)
      T.cast(envelope.event) match {
        case Some(casted) => Source.single((progress, envelope.persistenceId, Some(casted)))
        case _ =>
          log.warning("Some of the Events on the list are not compatible with type '{}', skipping...", T.describe)
          Source.single((progress, envelope.persistenceId, None))
      }
    }

    private def mapEvent(offset: ProjectionProgress,
                         id: String,
                         event: Event): Future[(ProjectionProgress, String, Option[Event], Option[MappedEvt])] =
      config
        .mapping(event)
        .retry
        .map {
          case Some(evtMapped) => (offset, id, Some(event): Option[Event], Some(evtMapped))
          case None =>
            log.warning("Indexing event with id '{}' and value '{}' failed '{}'", id, event, "Mapping failed")
            (offset, id, Some(event), None)
        }
        .recoverWith(logError(offset, id, event))
        .toIO
        .unsafeToFuture()

    private def mapBatchOffset(batched: Seq[(ProjectionProgress, String, Option[Event], Option[MappedEvt])])
      : (ProjectionProgress, List[(String, Event, MappedEvt)]) = {
      val offset = batched.lastOption.map { case (off, _, _, _) => off }.getOrElse(NoProgress)
      val skipped = batched.count {
        case (_, _, None, _) => true
        case (_, _, _, None) => true
        case _               => false
      }
      (OffsetProgress(offset.offset, offset.processedCount, offset.discardedCount + skipped), batched.collect {
        case (_, id, Some(event), Some(mappedEvent)) => (id, event, mappedEvent)
      }.toList)
    }

    private def logError(
        offset: ProjectionProgress,
        id: String,
        event: Event): PartialFunction[Throwable, F[(ProjectionProgress, String, Option[Event], Option[MappedEvt])]] = {
      case err =>
        log.error(err, "Indexing event with id '{}' and value '{}' failed '{}'", id, event, err.getMessage)
        F.pure((offset, id, Some(event), None))
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
  final class PersistentStreamByTag[F[_], Event: Encoder, MappedEvt, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Persist])(implicit failureLog: IndexFailuresLog[F],
                                                                projection: ResumableProjection[F],
                                                                F: Effect[F],
                                                                T: Typeable[Event],
                                                                as: ActorSystem)
      extends BatchedStreamByTag(config)
      with StreamByTag[F, ProjectionProgress] {

    def fetchInit: F[ProjectionProgress] =
      if (config.storage.restart)
        config.init.retry *> F.pure(NoProgress)
      else
        config.init.retry.flatMap(_ => projection.fetchProgress.retry)

    def source(initialProgress: ProjectionProgress): Source[ProjectionProgress, NotUsed] =
      batchedSource(initialProgress)
        .mapAsync(1) {
          case (offset, events) => indexEvents(offset, events)
        }
        .mapAsync(1) { progress =>
          config.mapProgress(progress).map(_ => progress).toIO.unsafeToFuture()
        }
        .mapAsync(1)(storeProgress)

    private def indexEvents(offset: ProjectionProgress, events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(recoverIndex(offset, events))
        .map(_ => offset)
        .toIO
        .unsafeToFuture()

    private def storeProgress(offset: ProjectionProgress): Future[ProjectionProgress] =
      projection.storeProgress(offset).retry.map(_ => offset).toIO.unsafeToFuture()

    private def recoverIndex(offset: ProjectionProgress,
                             events: List[IdentifiedEvent]): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.traverse {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed'{}'", id, event, err.getMessage)
            failureLog.storeEvent(id, offset.offset, event)
        } *> F.unit
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
  final class VolatileStreamByTag[F[_], Event, MappedEvt, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Volatile])(implicit
                                                                 F: Effect[F],
                                                                 T: Typeable[Event],
                                                                 as: ActorSystem)
      extends BatchedStreamByTag(config)
      with StreamByTag[F, ProjectionProgress] {

    def fetchInit: F[ProjectionProgress] = config.init.retry *> F.pure(NoProgress)

    private def indexEvents(offset: ProjectionProgress, events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(logError(events))
        .map(_ => offset)
        .toIO
        .unsafeToFuture()

    def source(initialProgress: ProjectionProgress): Source[ProjectionProgress, NotUsed] = {
      batchedSource(initialProgress)
        .mapAsync(1) {
          case (offset, events) => indexEvents(offset, events)
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
