package ch.epfl.bluebrain.nexus.sourcing

import _root_.akka.NotUsed
import _root_.akka.actor.ActorSystem
import _root_.akka.event.Logging
import _root_.akka.persistence.query.scaladsl.EventsByTagQuery
import _root_.akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import _root_.akka.stream.scaladsl.Source
import cats.effect.Effect
import cats.implicits._
import cats.effect.syntax.all._
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
        initialProgress: ProjectionProgress): Source[(Offset, List[IdentifiedEvent]), NotUsed] = {
      config.mapInitialProgress(initialProgress).toIO.unsafeRunSync()
      PersistenceQuery(as)
        .readJournalFor[EventsByTagQuery](config.pluginId)
        .eventsByTag(config.tag, initialProgress.offset)
        .flatMapConcat(castEvent)
        .mapAsync(1) { case (offset, id, event) => mapEvent(offset, id, event) }
        .collect { case Some(value) => value }
        .groupedWithin(config.batch, config.batchTo)
        .filter(_.nonEmpty)
        .map(mapBatchOffset)
    }

    private def castEvent(envelope: EventEnvelope): Source[(Offset, String, Event), NotUsed] = {
      log.debug("Processing event for persistence id '{}', seqNr '{}'", envelope.persistenceId, envelope.sequenceNr)
      T.cast(envelope.event) match {
        case Some(casted) => Source.single((envelope.offset, envelope.persistenceId, casted))
        case _ =>
          log.warning("Some of the Events on the list are not compatible with type '{}', skipping...", T.describe)
          Source.empty
      }
    }

    private def mapEvent(offset: Offset, id: String, event: Event): Future[Option[(Offset, String, Event, MappedEvt)]] =
      config
        .mapping(event)
        .retry
        .map {
          case Some(evtMapped) => Some((offset, id, event, evtMapped))
          case None =>
            log.warning("Indexing event with id '{}' and value '{}' failed '{}'", id, event, "Mapping failed")
            None
        }
        .recoverWith(logError(id, event))
        .toIO
        .unsafeToFuture()

    private def mapBatchOffset(
        batched: Seq[(Offset, String, Event, MappedEvt)]): (Offset, List[(String, Event, MappedEvt)]) = {
      val offset = batched.lastOption.map { case (off, _, _, _) => off }.getOrElse(NoOffset)
      (offset, batched.map { case (_, id, event, mappedEvent) => (id, event, mappedEvent) }.toList)
    }

    private def logError(id: String,
                         event: Event): PartialFunction[Throwable, F[Option[(Offset, String, Event, MappedEvt)]]] = {
      case err =>
        log.error(err, "Indexing event with id '{}' and value '{}' failed '{}'", id, event, err.getMessage)
        F.pure(None)
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
        .scanAsync(initialProgress) {
          case (previousProgress, (offset, events)) => indexEvents(previousProgress, offset, events)
        }
        .mapAsync(1) { progress =>
          config.mapProgress(progress).map(_ => progress).toIO.unsafeToFuture()
        }
        .mapAsync(1)(storeProgress)

    private def indexEvents(previousProgress: ProjectionProgress,
                            offset: Offset,
                            events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(recoverIndex(offset, events))
        .map(_ => OffsetProgress(offset, previousProgress.processedCount + events.size))
        .toIO
        .unsafeToFuture()

    private def storeProgress(offset: ProjectionProgress): Future[ProjectionProgress] =
      projection.storeProgress(offset).retry.map(_ => offset).toIO.unsafeToFuture()

    private def recoverIndex(offset: Offset, events: List[IdentifiedEvent]): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.traverse {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed'{}'", id, event, err.getMessage)
            failureLog.storeEvent(id, offset, event)
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

    private def indexEvents(previousProgress: ProjectionProgress,
                            offset: Offset,
                            events: List[IdentifiedEvent]): Future[ProjectionProgress] =
      config
        .index(events.map { case (_, _, mapped) => mapped })
        .retry
        .recoverWith(logError(events))
        .map(_ => OffsetProgress(offset, previousProgress.processedCount + events.size))
        .toIO
        .unsafeToFuture()

    def source(initialProgress: ProjectionProgress): Source[ProjectionProgress, NotUsed] = {
      batchedSource(initialProgress)
        .scanAsync(initialProgress) {
          case (previousOffset, (offset, events)) => indexEvents(previousOffset, offset, events)
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
