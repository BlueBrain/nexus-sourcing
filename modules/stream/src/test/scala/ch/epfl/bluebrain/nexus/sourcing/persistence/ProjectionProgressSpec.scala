package ch.epfl.bluebrain.nexus.sourcing.persistence

import java.util.UUID

import _root_.akka.persistence.query.{NoOffset, Sequence, TimeBasedUUID}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress._
import io.circe.{Encoder, Json}
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}
import shapeless.Typeable

class ProjectionProgressSpec extends WordSpecLike with Matchers with Inspectors with Resources with EitherValues {

  "A ProjectionProgress" should {
    val mapping = Map(
      OffsetProgress(Sequence(14L), 1, 0) -> jsonContentOf("/indexing/sequence-offset-progress.json"),
      OffsetProgress(TimeBasedUUID(UUID.fromString("ee7e4360-39ca-11e9-9ed5-dbdaa32f8986")), 32, 30) -> jsonContentOf(
        "/indexing/timebaseduuid-offset-progress.json"),
      NoProgress -> jsonContentOf("/indexing/no-offset-progress.json")
    )

    "properly encode progress values" in {
      forAll(mapping.toList) {
        case (prog, repr) =>
          Encoder[ProjectionProgress].apply(prog) shouldEqual repr
      }
    }

    "properly decode progress values" in {
      forAll(mapping.toList) {
        case (prog, repr) =>
          repr.as[ProjectionProgress].right.value shouldEqual prog
      }
    }
  }

  "A Typeable[NoOffset] instance" should {

    "cast a noOffset value" in {
      Typeable[NoOffset.type].cast(NoOffset) shouldEqual Some(NoOffset)
    }

    "not cast an arbitrary value" in {
      Typeable[NoOffset.type].cast(Json.obj()) shouldEqual None
    }

    "describe NoOffset by its object name" in {
      Typeable[NoOffset.type].describe shouldEqual "NoOffset"
    }
  }
}
