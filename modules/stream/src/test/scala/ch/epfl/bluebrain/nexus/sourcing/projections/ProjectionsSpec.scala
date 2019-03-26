package ch.epfl.bluebrain.nexus.sourcing.projections

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.{TestKit, TestKitBase}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.test.io.IOValues
import ch.epfl.bluebrain.nexus.sourcing.projections.Fixture.memoize
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionsSpec.SomeEvent
import io.circe.generic.auto._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}

import scala.concurrent.duration._

//noinspection TypeAnnotation
@DoNotDiscover
class ProjectionsSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with IOValues
    with Randomness
    with Eventually
    with BeforeAndAfterAll {

  override implicit lazy val system: ActorSystem = SystemBuilder.persistence("ProjectionsSpec")
  private implicit val mt: ActorMaterializer     = ActorMaterializer()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A Projection" should {
    val id            = genString()
    val persistenceId = s"/some/${genString()}"
    val projections   = memoize(Projections[IO, SomeEvent]).unsafeRunSync()
    val progress      = OffsetProgress(Offset.sequence(42), 42, 42)

    "store progress" in {
      projections.ioValue.recordProgress(id, progress).ioValue
    }

    "retrieve stored progress" in {
      projections.ioValue.progress(id).ioValue shouldEqual progress
    }

    "retrieve NoProgress for unknown projections" in {
      projections.ioValue.progress(genString()).ioValue shouldEqual NoProgress
    }

    val firstProgress  = OffsetProgress(Offset.sequence(42), 1L, 0L)
    val secondProgress = OffsetProgress(Offset.sequence(98), 2L, 3L)
    val firstEvent     = SomeEvent(1L, "description")
    val secondEvent    = SomeEvent(2L, "description2")

    "store an event" in {
      projections.ioValue.recordFailure(id, persistenceId, firstProgress, firstEvent).ioValue
    }

    "store another event" in {
      projections.ioValue.recordFailure(id, persistenceId, secondProgress, secondEvent).ioValue
    }

    "retrieve stored events" in {
      val expected = Seq((firstEvent, firstProgress), (secondEvent, secondProgress))
      eventually {
        logOf(projections.ioValue.failures(id)) should contain theSameElementsInOrderAs expected
      }
    }

    "retrieve empty list of events for unknown failure log" in {
      eventually {
        logOf(projections.ioValue.failures(genString())) shouldBe empty
      }
    }

  }

  private def logOf(source: Source[(SomeEvent, ProjectionProgress), _]): Vector[(SomeEvent, ProjectionProgress)] = {
    val f = source.runFold(Vector.empty[(SomeEvent, ProjectionProgress)])(_ :+ _)
    IO.fromFuture(IO(f)).ioValue
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(30 seconds, 50 milliseconds)
}

object ProjectionsSpec {
  final case class SomeEvent(rev: Long, description: String)
}
