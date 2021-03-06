package ch.epfl.bluebrain.nexus.sourcing.projections

import java.util.UUID

import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import io.circe.generic.extras.Configuration
import io.circe._

import scala.reflect.ClassTag

object instances extends AllInstances

trait AllInstances extends CirceInstances with OffsetOrderingInstances

trait CirceInstances {
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

  private def encodeDiscriminated[A: Encoder](a: A)(implicit A: ClassTag[A]) =
    Encoder[A].apply(a).deepMerge(Json.obj("type" -> Json.fromString(A.runtimeClass.getSimpleName)))
}

trait OffsetOrderingInstances {
  final implicit val offsetOrdering: Ordering[Offset] = {
    case (x: Sequence, y: Sequence)           => x compare y
    case (x: TimeBasedUUID, y: TimeBasedUUID) => x.value.timestamp() compareTo y.value.timestamp()
    case (NoOffset, _)                        => -1
    case (_, NoOffset)                        => 1
    case _                                    => 0
  }
}
