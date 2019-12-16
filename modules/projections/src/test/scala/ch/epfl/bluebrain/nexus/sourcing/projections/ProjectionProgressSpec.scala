package ch.epfl.bluebrain.nexus.sourcing.projections

import java.util.UUID

import akka.persistence.query.{Sequence, TimeBasedUUID}
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress._
import io.circe.Encoder
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{EitherValues, Inspectors}

class ProjectionProgressSpec extends AnyWordSpecLike with Matchers with Inspectors with TestHelpers with EitherValues {

  "A ProjectionProgress" should {
    val mapping = Map(
      OffsetProgress(Sequence(14L), 2, 0, 1) ->
        jsonContentOf("/indexing/sequence-offset-progress.json"),
      OffsetProgress(TimeBasedUUID(UUID.fromString("ee7e4360-39ca-11e9-9ed5-dbdaa32f8986")), 32, 5, 10) ->
        jsonContentOf("/indexing/timebaseduuid-offset-progress.json"),
      NoProgress ->
        jsonContentOf("/indexing/no-offset-progress.json"),
      OffsetsProgress(Map("noOffset" -> NoProgress, "other" -> OffsetProgress(Sequence(2L), 10L, 2L, 0L))) ->
        jsonContentOf("/indexing/offsets-progress.json")
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
          repr.as[ProjectionProgress].rightValue shouldEqual prog
      }
    }

    "Add progress" in {
      val progress =
        OffsetsProgress(Map("noOffset" -> NoProgress, "other" -> OffsetProgress(Sequence(2L), 10L, 2L, 0L)))
      progress + ("noOffset", Sequence(1L), ProgressStatus.Failed("some error")) shouldEqual
        OffsetsProgress(
          Map(
            "noOffset" -> OffsetProgress(Sequence(1L), 1L, 0L, 1L),
            "other"    -> OffsetProgress(Sequence(2L), 10L, 2L, 0L)
          )
        )
      progress + ("other", Sequence(3L), ProgressStatus.Discarded) shouldEqual
        OffsetsProgress(Map("noOffset" -> NoProgress, "other" -> OffsetProgress(Sequence(3L), 11L, 3L, 0L)))
    }

    "fetch minimum progress" in {
      val progress = OffsetsProgress(
        Map(
          "one"   -> OffsetProgress(Sequence(1L), 2L, 1L, 0L),
          "other" -> OffsetProgress(Sequence(2L), 10L, 2L, 0L),
          "a"     -> OffsetProgress(Sequence(0L), 0L, 0L, 0L)
        )
      )
      progress.minProgressFilter(_.length > 1) shouldEqual OffsetProgress(Sequence(1L), 2L, 1L, 0L)
      progress.minProgress shouldEqual OffsetProgress(Sequence(0L), 0L, 0L, 0L)

    }
  }
}
