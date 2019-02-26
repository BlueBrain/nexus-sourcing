package ch.epfl.bluebrain.nexus.sourcing.persistence

import _root_.akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import _root_.akka.persistence.cassandra.session.scaladsl.CassandraSession
import cats.MonadError
import cats.effect.LiftIO
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress._
import monix.eval.Task

/**
  * Contract defining interface for the projection storage that allows storing the progress against a projection identifier
  * and querying the last known offset for an identifier.
  */
trait ProjectionProgressStorage[F[_]] {

  /**
    * Records progress against a projection identifier.
    *
    * @param identifier an unique identifier for a projection
    * @param progress   the offset to record
    * @return a future () value
    */
  def storeProgress(identifier: String, progress: ProjectionProgress): F[Unit]

  /**
    * Retrieves the progress for the specified projection identifier.  If there is no record of progress
    * the [[NoProgress]] is returned.
    *
    * @param identifier an unique identifier for a projection
    * @return a future progress value for the specified projection identifier
    */
  def fetchProgress(identifier: String): F[ProjectionProgress]
}

/**
  * Cassandra backed [[ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgressStorage]] implementation as an
  * Akka extension that piggybacks on Akka Persistence Cassandra for configuration and session management.
  *
  * @param session  a cassandra session
  * @param keyspace the keyspace under which the projection storage operates
  * @param table    the table where projection offsets are stored
  */
final class CassandraProjectionProgressStorage[F[_]: LiftIO](session: CassandraSession, keyspace: String, table: String)(
    implicit F: MonadError[F, Throwable])
    extends ProjectionProgressStorage[F]
    with Extension
    with ProjectionProgressCodec {
  import io.circe.parser._

  override def storeProgress(identifier: String, progress: ProjectionProgress): F[Unit] = {
    val stmt = s"update $keyspace.$table set progress = ? where identifier = ?"
    liftIO(session.executeWrite(stmt, projectionProgressEncoder(progress).noSpaces, identifier)).map(_ => ())
  }

  override def fetchProgress(identifier: String): F[ProjectionProgress] = {
    val stmt = s"select progress from $keyspace.$table where identifier = ?"
    liftIO(session.selectOne(stmt, identifier)).flatMap {
      case Some(row) => F.fromTry(decode[ProjectionProgress](row.getString("progress")).toTry)
      case None      => F.pure(NoProgress)
    }
  }
}

object ProjectionProgressStorage
    extends ExtensionId[CassandraProjectionProgressStorage[Task]]
    with ExtensionIdProvider
    with CassandraStorage {
  override def lookup(): ExtensionId[_ <: Extension] = ProjectionProgressStorage

  override def createExtension(system: ExtendedActorSystem): CassandraProjectionProgressStorage[Task] = {
    val (session, keyspace, table) =
      createSession("projection", "identifier varchar primary key, progress text", system)

    new CassandraProjectionProgressStorage[Task](session, keyspace, table)
  }
}
