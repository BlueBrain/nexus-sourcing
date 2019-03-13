package ch.epfl.bluebrain.nexus.sourcing.persistence
import akka.persistence.query.{NoOffset, Offset}

/**
  * Representation of projection progress.
  */
sealed trait ProjectionProgress extends Product with Serializable {

  /**
    * The last processed [[Offset]].
    */
  def offset: Offset

  /**
    * Count of processed events.
    */
  def processedCount: Long

  /**
    *
    * Count of discarded events.
    */
  def discardedCount: Long
}

object ProjectionProgress {

  /**
    * Representation of lack of recorded progress
    */
  final case object NoProgress extends ProjectionProgress {
    val offset: Offset       = NoOffset
    val processedCount: Long = 0
    val discardedCount: Long = 0

  }

  /**
    * Representation of [[Offset]] based projection progress.
    */
  final case class OffsetProgress(offset: Offset, processedCount: Long, discardedCount: Long) extends ProjectionProgress
}
