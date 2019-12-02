package ch.epfl.bluebrain.nexus.sourcing.projections

import java.util.UUID

import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.{ProgressStatus, SingleProgress}
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.syntax._

import scala.math.Ordering.Implicits._
import scala.reflect.ClassTag

/**
  * Representation of projection progress.
  */
sealed trait ProjectionProgress extends Product with Serializable {

  /**
    * The smallest single progress
    */
  def minProgress: SingleProgress

  /**
    * Adds a status
    *
    * @param id       the progress id
    * @param offset   the offset value
    * @param progress the progress status
    */
  def +(id: String, offset: Offset, progress: ProgressStatus): ProjectionProgress

  /**
    * Retrieves a single progress wth the passed id
    *
    * @param id the progress id
    */
  def progress(id: String): SingleProgress

}

object ProjectionProgress {

  private def toLong(b: Boolean) = if (b) 1L else 0L

  type OffsetProgressMap = Map[String, SingleProgress]

  /**
    * Enumeration of the possible progress status for a single message
    */
  sealed trait ProgressStatus extends Product with Serializable {
    def discarded: Boolean = false
    def failed: Boolean    = false
  }

  object ProgressStatus {

    /**
      * A discarded messaged
      */
    final case object Discarded extends ProgressStatus {
      override def discarded: Boolean = true
    }

    /**
      * A failed message
      *
      * @param error the failure
      */
    final case class Failed(error: String) extends ProgressStatus {
      override def failed: Boolean = true
    }

    final case object Passed extends ProgressStatus
  }

  /**
    * Representation of a single projection progress.
    */
  sealed trait SingleProgress extends ProjectionProgress {

    /**
      * The last processed [[Offset]].
      */
    def offset: Offset

    /**
      * Count of processed events.
      */
    def processed: Long

    /**
      * Count of discarded events.
      */
    def discarded: Long

    /**
      * Count of failed events.
      */
    def failed: Long

    def minProgress: SingleProgress = this

    def progress(id: String): SingleProgress = this

  }

  /**
    * Representation of lack of recorded progress
    */
  final case object NoProgress extends SingleProgress {
    val offset: Offset  = NoOffset
    val processed: Long = 0
    val discarded: Long = 0
    val failed: Long    = 0

    def +(id: String, offset: Offset, progress: ProgressStatus): ProjectionProgress =
      OffsetsProgress(
        Map(id -> OffsetProgress(offset, 1L, toLong(progress.discarded), toLong(progress.failed)))
      )
  }

  /**
    * Representation of [[Offset]] based projection progress.
    */
  final case class OffsetProgress(offset: Offset, processed: Long, discarded: Long, failed: Long)
      extends SingleProgress {
    def +(id: String, newOffset: Offset, progress: ProgressStatus): ProjectionProgress =
      OffsetsProgress(
        Map(
          id -> OffsetProgress(
            newOffset,
            processed + 1L,
            discarded + toLong(progress.discarded),
            failed + toLong(progress.failed)
          )
        )
      )
  }

  /**
    * Representation of multiple [[Offset]] based projection. Each projection has a unique identifier
    * and a [[SingleProgress]]
    */
  final case class OffsetsProgress(progress: OffsetProgressMap) extends ProjectionProgress {

    @SuppressWarnings(Array("UnsafeTraversableMethods"))
    lazy val minProgress: SingleProgress =
      if (progress.isEmpty) NoProgress else progress.values.minBy(_.offset)

    /**
      * Replaces the passed single projection
      */
    def replace(tuple: (String, SingleProgress)): OffsetsProgress =
      OffsetsProgress(progress + tuple)

    def +(id: String, offset: Offset, progress: ProgressStatus): ProjectionProgress = {
      val previous = this.progress.getOrElse(id, NoProgress)
      replace(
        id -> OffsetProgress(
          offset,
          previous.processed + 1L,
          previous.discarded + toLong(progress.discarded),
          previous.failed + toLong(progress.failed)
        )
      )
    }

    def progress(id: String): SingleProgress = progress.getOrElse(id, NoProgress)

  }

  object OffsetsProgress {
    val empty: OffsetsProgress = OffsetsProgress(Map.empty)
  }

  private[projections] implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  final implicit val sequenceEncoder: Encoder[Sequence] = Encoder.instance { seq =>
    Json.obj("value" -> Json.fromLong(seq.value))
  }

  final implicit val sequenceDecoder: Decoder[Sequence] = Decoder.instance { cursor =>
    cursor.get[Long]("value").map(value => Sequence(value))
  }

  final implicit val timeBasedUUIDEncoder: Encoder[TimeBasedUUID] = Encoder.instance { uuid =>
    Json.obj("value" -> Encoder.encodeUUID(uuid.value))
  }

  final implicit val timeBasedUUIDDecoder: Decoder[TimeBasedUUID] = Decoder.instance { cursor =>
    cursor.get[UUID]("value").map(uuid => TimeBasedUUID(uuid))
  }

  final implicit val noOffsetEncoder: Encoder[NoOffset.type] = Encoder.instance(_ => Json.obj())

  final implicit val noOffsetDecoder: Decoder[NoOffset.type] = Decoder.instance { cursor =>
    cursor.as[JsonObject].map(_ => NoOffset)
  }

  final implicit val offsetEncoder: Encoder[Offset] = Encoder.instance {
    case o: Sequence      => encodeDiscriminated(o)
    case o: TimeBasedUUID => encodeDiscriminated(o)
    case o: NoOffset.type => encodeDiscriminated(o)
  }

  final implicit def offsetDecoder(
      implicit S: ClassTag[Sequence],
      TBU: ClassTag[TimeBasedUUID],
      NO: ClassTag[NoOffset.type]
  ): Decoder[Offset] = {
    val sequence      = S.runtimeClass.getSimpleName
    val timeBasedUUID = TBU.runtimeClass.getSimpleName
    val noOffset      = NO.runtimeClass.getSimpleName

    Decoder.instance { cursor =>
      cursor.get[String]("type").flatMap {
        case `sequence`      => cursor.as[Sequence]
        case `timeBasedUUID` => cursor.as[TimeBasedUUID]
        case `noOffset`      => cursor.as[NoOffset.type]
        //       $COVERAGE-OFF$
        case other => Left(DecodingFailure(s"Unknown discriminator value '$other'", cursor.history))
        //       $COVERAGE-ON$
      }
    }
  }

  implicit val singleProgressEncoder: Encoder[SingleProgress] = deriveConfiguredEncoder[SingleProgress]
  implicit val singleProgressDecoder: Decoder[SingleProgress] = deriveConfiguredDecoder[SingleProgress]

  implicit val mapProgressEncoder: Encoder[OffsetProgressMap] = Encoder.instance { m =>
    val jsons = m.map { case (index, projection) => Json.obj("index" -> index.asJson, "value" -> projection.asJson) }
    Json.arr(jsons.toSeq: _*)
  }

  implicit val mapProgressDecoder: Decoder[OffsetProgressMap] =
    (hc: HCursor) =>
      hc.value.asArray.toRight(DecodingFailure("Expected array was not found", hc.history)).flatMap { arr =>
        arr.foldM[Decoder.Result, OffsetProgressMap](Map.empty[String, SingleProgress]) { (map, c) =>
          (c.hcursor.get[String]("index"), c.hcursor.get[SingleProgress]("value")).mapN {
            case (index, projection) =>
              map + (index -> projection)
          }
        }
      }

  implicit val projectionProgressEncoder: Encoder[ProjectionProgress] = deriveConfiguredEncoder[ProjectionProgress]
  implicit val projectionProgressDecoder: Decoder[ProjectionProgress] = deriveConfiguredDecoder[ProjectionProgress]

  private def encodeDiscriminated[A: Encoder](a: A)(implicit A: ClassTag[A]) =
    Encoder[A].apply(a).deepMerge(Json.obj("type" -> Json.fromString(A.runtimeClass.getSimpleName)))

  /**
    * Offset ordering
    */
  implicit val ordering: Ordering[Offset] = {
    case (x: Sequence, y: Sequence)           => x compare y
    case (x: TimeBasedUUID, y: TimeBasedUUID) => x compare y
    case (NoOffset, _)                        => -1
    case (_, NoOffset)                        => 1
    case _                                    => 0
  }

  /**
    * Syntactic sugar for [[Offset]]
    */
  implicit class OffsetSyntax(private val value: Offset) extends AnyVal {

    /**
      * Offset comparison
      *
      * @param offset the offset to compare against the ''value''
      * @return true when ''value'' is greater than the passed ''offset'' or when offset is ''NoOffset'', false otherwise
      **/
    def gt(offset: Offset): Boolean =
      offset == NoOffset || value > offset
  }
}
