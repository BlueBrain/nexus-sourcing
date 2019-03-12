package ch.epfl.bluebrain.nexus.sourcing.persistence

import _root_.akka.actor.ActorSystem
import monix.eval.Task

/**
  * A ResumableProjection allows storing the current projection progress based on an offset description such that it
  * can be resumed when interrupted (either intentionally or as a consequence for an arbitrary failure).
  *
  * Example use:
  * {{{
  *
  *   implicit val as: ActorSystem = ActorSystem()
  *   val proj = ResumableProjection("default")
  *   proj.fetchProgress // Future[ProjectionProgress]
  *
  * }}}
  */
trait ResumableProjection[F[_]] {

  /**
    * @return an unique identifier for this projection
    */
  def identifier: String

  /**
    * @return the latest known [[ProjectionProgress]]; an inexistent offset is represented by [[ProjectionProgress.NoProgress]]
    */
  def fetchProgress: F[ProjectionProgress]

  /**
    * Records the argument [[ProjectionProgress]] against this projection.
    *
    * @param progress the progress to record
    * @return a future () value upon success or a failure otherwise
    */
  def storeProgress(progress: ProjectionProgress): F[Unit]
}

object ResumableProjection {

  private[persistence] def apply[F[_]](id: String, storage: ProjectionProgressStorage[F]): ResumableProjection[F] =
    new ResumableProjection[F] {
      override val identifier: String = id

      override def storeProgress(progress: ProjectionProgress): F[Unit] =
        storage.storeProgress(identifier, progress)

      override def fetchProgress: F[ProjectionProgress] =
        storage.fetchProgress(identifier)
    }

  /**
    * Constructs a new `ResumableProjection` instance with the specified identifier.  Calls to store or query the
    * current offset are delegated to the underlying
    * [[ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgressStorage]] extension.
    *
    * @param id an identifier for the projection
    * @param as an implicitly available actor system
    * @return a new `ResumableProjection` instance with the specified identifier
    */
  // $COVERAGE-OFF$
  def apply(id: String)(implicit as: ActorSystem): ResumableProjection[Task] =
    apply[Task](id, ProjectionProgressStorage(as))
  // $COVERAGE-ON$
}
