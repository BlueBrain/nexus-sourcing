package ch.epfl.bluebrain.nexus.sourcing.persistence

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.{Sequence, TimeBasedUUID}
import akka.stream.scaladsl.Source
import cats.effect._
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress.{NoProgress, OffsetProgress}
import com.datastax.driver.core.Session
import com.google.common.util.concurrent.ListenableFuture
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, Encoder}

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

/**
  * A Projection represents the process to transforming an event stream into a format that's efficient for consumption.
  * In terms of CQRS, the events represents the format in which data is written to the primary store (the write
  * model) while the result of a projection represents the data in a consumable format (the read model).
  *
  * Projections replay an event stream
  */
trait Projections[F[_], E] {

  /**
    * Records progress against a projection identifier.
    *
    * @param id       the projection identifier
    * @param progress the offset to record
    * @return a future () value
    */
  def recordProgress(id: String, progress: ProjectionProgress): F[Unit]

  /**
    * Retrieves the progress for the specified projection projectionId. If there is no record of progress
    * the [[ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress.NoProgress]] is returned.
    *
    * @param id an unique projectionId for a projection
    * @return a future progress value for the specified projection projectionId
    */
  def progress(id: String): F[ProjectionProgress]

  /**
    * Record a specific event against a index failures log projectionId.
    *
    * @param id            the project identifier
    * @param persistenceId the persistenceId to record
    * @param progress      the progress to record
    * @param event         the event to be recorded
    */
  def recordFailure(id: String, persistenceId: String, progress: ProjectionProgress, event: E): F[Unit]

  /**
    * An event stream for all failures recorded for a projection.
    *
    * @param id the projection identifier
    * @return a source of the failed events
    */
  def failures(id: String): Source[(E, ProjectionProgress), _]
}

object Projections {

  private class Statements(as: ActorSystem) {
    private val journalConfig = as.settings.config.getConfig("cassandra-journal")
    private val cfg           = new CassandraPluginConfig(as, journalConfig)

    val keyspace: String      = cfg.keyspace
    val progressTable: String = journalConfig.getString("projection-progress-table")
    val failuresTable: String = journalConfig.getString("projection-failures-table")

    val createKeyspace: String =
      s"""CREATE KEYSPACE IF NOT EXISTS $keyspace
         |WITH REPLICATION = { 'class' : ${cfg.replicationStrategy} }""".stripMargin

    val createProgressTable: String =
      s"""CREATE TABLE IF NOT EXISTS $keyspace.$progressTable (
         |projectionId varchar primary key, progress text)""".stripMargin

    val createFailuresTable: String =
      s"""CREATE TABLE IF NOT EXISTS $keyspace.$failuresTable (
         |projectionId varchar, offset bigint, persistenceId text, progress text, event text,
         |PRIMARY KEY (projectionId, offset, persistenceId))
         |WITH CLUSTERING ORDER BY (offset ASC)""".stripMargin

    val recordProgressQuery: String =
      s"UPDATE $keyspace.$progressTable SET progress = ? WHERE projectionId = ?"

    val progressQuery: String =
      s"SELECT progress FROM $keyspace.$progressTable WHERE projectionId = ?"

    val recordFailureQuery: String =
      s"""INSERT INTO $keyspace.$failuresTable (projectionId, offset, persistenceId, progress, event)
         |VALUES (?, ?, ?, ?, ?) IF NOT EXISTS""".stripMargin

    val failuresQuery: String =
      s"SELECT progress, event from $keyspace.$failuresTable WHERE projectionId = ? ALLOW FILTERING"
  }

  private class CassandraProjections[F[_], E](
      session: CassandraSession,
      stmts: Statements
  )(implicit F: Async[F], Encoder: Encoder[E], Decoder: Decoder[E])
      extends Projections[F, E] {

    override def recordProgress(id: String, progress: ProjectionProgress): F[Unit] =
      wrapFuture(session.executeWrite(stmts.recordProgressQuery, progress.asJson.noSpaces, id)) >> F.unit

    override def progress(id: String): F[ProjectionProgress] =
      wrapFuture(session.selectOne(stmts.progressQuery, id)).flatMap {
        case Some(row) => F.fromTry(decode[ProjectionProgress](row.getString("progress")).toTry)
        case None      => F.pure(NoProgress)
      }

    override def recordFailure(id: String, persistenceId: String, progress: ProjectionProgress, event: E): F[Unit] =
      wrapFuture(
        session.executeWrite(stmts.recordFailureQuery,
                             id,
                             toOffset(progress),
                             persistenceId,
                             progress.asJson.noSpaces,
                             event.asJson.noSpaces)) >> F.unit

    override def failures(id: String): Source[(E, ProjectionProgress), _] =
      session
        .select(stmts.failuresQuery, id)
        .map { row =>
          (decode[E](row.getString("event")), decode[ProjectionProgress](row.getString("progress"))).tupled
        }
        .collect { case Right(value) => value }

    private def toOffset(progress: ProjectionProgress): java.lang.Long = progress match {
      case OffsetProgress(seq: Sequence, _, _)       => seq.value
      case OffsetProgress(uuid: TimeBasedUUID, _, _) => uuid.value.timestamp()
      case _                                         => 0L
    }
  }

  private def wrapFuture[F[_]: LiftIO, A](f: => Future[A]): F[A] =
    IO.fromFuture(IO(f)).to[F]

  private def wrapFuture[F[_]: LiftIO, A](f: => ListenableFuture[A])(implicit ec: ExecutionContextExecutor): F[A] =
    wrapFuture {
      val promise = Promise[A]
      f.addListener(() => promise.complete(Try(f.get())), ec)
      promise.future
    }

  private def lookupConfig(as: ActorSystem): CassandraPluginConfig = {
    val journalConfig = as.settings.config.getConfig("cassandra-journal")
    new CassandraPluginConfig(as, journalConfig)
  }

  private def createSession[F[_]](implicit as: ActorSystem, F: Sync[F]): F[CassandraSession] = {
    val cfg = lookupConfig(as)
    val log = Logging(as, Projections.getClass)
    F.delay {
      new CassandraSession(
        as,
        cfg.sessionProvider,
        cfg.sessionSettings,
        as.dispatcher,
        log,
        metricsCategory = "projections",
        init = _ => Future.successful(Done)
      )
    }
  }

  private def ensureInitialized[F[_]](session: CassandraSession, cfg: CassandraPluginConfig, stmts: Statements)(
      implicit as: ActorSystem,
      F: Async[F]): F[Unit] = {
    implicit val ec: ExecutionContextExecutor = as.dispatcher

    val underlying = wrapFuture(session.underlying())

    def keyspace(s: Session) =
      if (cfg.keyspaceAutoCreate) wrapFuture(s.executeAsync(stmts.createKeyspace)) >> F.unit
      else F.unit

    def progress(s: Session) =
      if (cfg.tablesAutoCreate) wrapFuture(s.executeAsync(stmts.createProgressTable)) >> F.unit
      else F.unit

    def failures(s: Session) =
      if (cfg.tablesAutoCreate) wrapFuture(s.executeAsync(stmts.createFailuresTable)) >> F.unit
      else F.unit

    for {
      s <- underlying
      _ <- keyspace(s)
      _ <- progress(s)
      _ <- failures(s)
    } yield ()
  }

  def apply[F[_], E: Encoder: Decoder](session: CassandraSession)(implicit as: ActorSystem,
                                                                  F: Async[F]): F[Projections[F, E]] = {
    val statements = new Statements(as)
    val cfg        = lookupConfig(as)

    ensureInitialized(session, cfg, statements) >> F.delay(new CassandraProjections(session, statements))
  }

  def apply[F[_], E: Encoder: Decoder](implicit as: ActorSystem, F: Async[F]): F[Projections[F, E]] =
    createSession[F].flatMap(session => apply(session))

}
