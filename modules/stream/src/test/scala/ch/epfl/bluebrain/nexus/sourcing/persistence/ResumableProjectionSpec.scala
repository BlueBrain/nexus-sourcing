package ch.epfl.bluebrain.nexus.sourcing.persistence

import java.util.UUID

import _root_.akka.persistence.query.Offset
import _root_.akka.testkit.{TestKit, TestKitBase}
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress._
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}

import scala.concurrent.duration._

//noinspection TypeAnnotation
@DoNotDiscover
class ResumableProjectionSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit lazy val system = SystemBuilder.persistence("ResumableProjectionSpec")

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A ResumableProjection" should {
    val id = UUID.randomUUID().toString

    "store progress" in {
      ResumableProjection(id).storeProgress(OffsetProgress(Offset.sequence(42), 42)).runToFuture.futureValue
    }

    "retrieve stored progress" in {
      ResumableProjection(id).fetchProgress.runToFuture.futureValue shouldEqual OffsetProgress(Offset.sequence(42), 42)
    }

    "retrieve NoProgress for unknown projections" in {
      ResumableProjection(UUID.randomUUID().toString).fetchProgress.runToFuture.futureValue shouldEqual NoProgress
    }
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(30 seconds, 1 second)
}
