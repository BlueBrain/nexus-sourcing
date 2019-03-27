package ch.epfl.bluebrain.nexus.sourcing.projections

import java.util.UUID

import akka.persistence.query.{Sequence, TimeBasedUUID}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress._
import io.circe.Encoder
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

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
}
