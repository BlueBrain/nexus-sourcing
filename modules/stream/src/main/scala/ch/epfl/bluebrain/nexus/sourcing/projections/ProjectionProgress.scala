package ch.epfl.bluebrain.nexus.sourcing.projections

import java.util.UUID

import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveDecoder, deriveEncoder}
import shapeless.Typeable

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

  private implicit val config: Configuration = Configuration.default
    .withDiscriminator("type")

  final implicit val noOffsetTypeable: Typeable[NoOffset.type] =
    new Typeable[NoOffset.type] {
      override def cast(t: Any): Option[NoOffset.type] =
        if (NoOffset == t) Some(NoOffset) else None
      override def describe: String = "NoOffset"
    }

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

  final implicit val offsetDecoder: Decoder[Offset] = {
    val sequence      = Typeable[Sequence].describe
    val timeBasedUUID = Typeable[TimeBasedUUID].describe
    val noOffset      = Typeable[NoOffset.type].describe

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

  implicit val projectionProgressEncoder: Encoder[ProjectionProgress] = deriveEncoder[ProjectionProgress]
  implicit val projectionProgressDecoder: Decoder[ProjectionProgress] = deriveDecoder[ProjectionProgress]

  private def encodeDiscriminated[A: Encoder: Typeable](a: A) =
    Encoder[A].apply(a).deepMerge(Json.obj("type" -> Json.fromString(Typeable[A].describe)))
}
