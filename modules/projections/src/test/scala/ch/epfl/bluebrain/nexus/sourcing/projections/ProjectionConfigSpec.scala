package ch.epfl.bluebrain.nexus.sourcing.projections

import cats.effect.{IO, Timer}
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionConfig._
import ch.epfl.bluebrain.nexus.sourcing.retry.RetryStrategy.{Backoff, Linear}
import ch.epfl.bluebrain.nexus.sourcing.retry.{Retry, RetryStrategy}
import org.scalactic.Equality
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ProjectionConfigSpec
    extends ActorSystemFixture("IndexerConfigSpec", startCluster = false)
    with WordSpecLike
    with Matchers {

  private implicit val timer: Timer[IO] = IO.timer(system.dispatcher)

  "A IndexerConfig" should {
    val indexF: List[String] => IO[Unit]            = _ => IO.unit
    val initF: IO[Unit]                             = IO.unit
    val mapProgress: ProjectionProgress => IO[Unit] = _ => IO.unit
    val strategy: RetryStrategy                     = Linear(0 millis, 2000 hours)
    val identity: String => IO[Option[String]]      = (v: String) => IO.pure(Some(v))

    implicit def eqIndexerConfig[T <: ProgressStorage]: Equality[ProjectionConfig[IO, String, String, Throwable, T]] =
      (a: ProjectionConfig[IO, String, String, Throwable, T], b: Any) => {
        val that = b.asInstanceOf[ProjectionConfig[IO, String, String, Throwable, T]]
        a.pluginId == that.pluginId && a.batchTo == that.batchTo && a.batch == that.batch && a.init == that.init && a.storage == that.storage && a.tag == that.tag && that.index == a.index && that.mapping == a.mapping
      }

    "build a the configuration for index function with persistence" in {
      val storage = Persist(restart = false)
      val expected: ProjectionConfig[IO, String, String, Throwable, Persist] =
        ProjectionConfig("t",
                         "p",
                         "n",
                         identity,
                         indexF,
                         mapProgress,
                         mapProgress,
                         initF,
                         1,
                         50 millis,
                         Retry(strategy),
                         storage)
      builder[IO]
        .name("n")
        .plugin("p")
        .tag("t")
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .build shouldEqual expected
    }

    "build a the configuration for index function without persistence" in {
      val st = Linear(10 millis, 1 hour)
      val expected: ProjectionConfig[IO, String, String, Throwable, Volatile] =
        ProjectionConfig("t",
                         "p",
                         "n",
                         identity,
                         indexF,
                         mapProgress,
                         mapProgress,
                         initF,
                         5,
                         100 millis,
                         Retry(st),
                         Volatile)
      builder[IO]
        .name("n")
        .plugin("p")
        .tag("t")
        .batch(5, 100 millis)
        .retry(st)
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .offset(Volatile)
        .build shouldEqual expected
    }

    "build from config" in {
      val storage = Persist(restart = false)
      val st      = Backoff(100 millis, 10 hours, 0.5, 7)
      val expected: ProjectionConfig[IO, String, String, Throwable, Persist] =
        ProjectionConfig("t",
                         "p",
                         "n",
                         identity,
                         indexF,
                         mapProgress,
                         mapProgress,
                         initF,
                         10,
                         40 millis,
                         Retry(st),
                         storage)
      fromConfig[IO]
        .name("n")
        .plugin("p")
        .tag("t")
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .build shouldEqual expected
    }
  }
}
