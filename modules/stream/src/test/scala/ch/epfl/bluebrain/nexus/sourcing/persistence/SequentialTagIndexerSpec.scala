package ch.epfl.bluebrain.nexus.sourcing.persistence

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import _root_.akka.Done
import _root_.akka.cluster.Cluster
import _root_.akka.stream.ActorMaterializer
import _root_.akka.testkit.{TestActorRef, TestKit, TestKitBase}
import _root_.akka.util.Timeout
import cats.MonadError
import cats.effect.{IO, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.test.io.IOOptionValues
import ch.epfl.bluebrain.nexus.sourcing.StreamByTag
import Fixture.memoize
import ch.epfl.bluebrain.nexus.sourcing.StreamByTag.PersistentStreamByTag
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.{PassivationStrategyConfig, RetryStrategyConfig}
import ch.epfl.bluebrain.nexus.sourcing.akka._
import ch.epfl.bluebrain.nexus.sourcing.persistence.Fixture._
import ch.epfl.bluebrain.nexus.sourcing.persistence.IndexerConfig.fromConfig
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress.OffsetProgress
import ch.epfl.bluebrain.nexus.sourcing.persistence.SequentialTagIndexerSpec._
import ch.epfl.bluebrain.nexus.sourcing.retry.RetryStrategy.Linear
import ch.epfl.bluebrain.nexus.sourcing.retry._
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator.StreamCoordinatorActor
import io.circe.generic.auto._
import org.scalatest.concurrent.Eventually
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

//noinspection TypeAnnotation
@DoNotDiscover
class SequentialTagIndexerSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with IOOptionValues
    with Eventually {

  implicit lazy val system              = SystemBuilder.cluster("SequentialTagIndexerSpec")
  implicit val ec                       = system.dispatcher
  implicit val mt                       = ActorMaterializer()
  private implicit val timer: Timer[IO] = IO.timer(ec)

  val projections = memoize(Projections[IO, Fixture.Event])(IO.ioEffect).unsafeRunSync()

  private val cluster = Cluster(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cluster.join(cluster.selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(35 seconds, 500 millis)

  "A SequentialTagIndexer" should {
    val pluginId = "cassandra-query-journal"
    val config = AkkaSourcingConfig(
      Timeout(30.second),
      pluginId,
      200.milliseconds,
      ExecutionContext.global
    )

    implicit val F: MonadError[IO, RetriableErr] = new MonadError[IO, RetriableErr] {
      override def handleErrorWith[A](fa: IO[A])(f: RetriableErr => IO[A]): IO[A] =
        IO.ioEffect.handleErrorWith(fa) {
          case t: RetriableErr => f(t)
          case other           => IO.raiseError(other)
        }
      override def raiseError[A](e: RetriableErr): IO[A]          = IO.raiseError(e)
      override def pure[A](x: A): IO[A]                           = IO.pure(x)
      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
      override def tailRecM[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
        IO.ioEffect.tailRecM(a)(f)
    }

    sealed abstract class Context[T](name: String, batch: Int = 1, tag: String) {
      implicit val sourcingConfig = SourcingConfig(
        config.askTimeout.duration,
        "queryPlugin",
        config.askTimeout.duration,
        "global",
        1,
        PassivationStrategyConfig(None, None),
        RetryStrategyConfig("once",
                            config.askTimeout.duration,
                            config.askTimeout.duration,
                            1,
                            0.1,
                            config.askTimeout.duration)
      )
      val agg = AkkaAggregate
        .sharded[IO](
          name,
          Fixture.initial,
          Fixture.next,
          Fixture.eval,
          PassivationStrategy.immediately[State, Cmd],
          Retry[IO, Throwable](RetryStrategy.Never),
          config,
          shards = 10
        )
        .unsafeRunSync()

      val count = new AtomicLong(0L)
      val init  = new AtomicLong(10L)

      def initFunction(init: AtomicLong): IO[Unit] =
        IO.delay(init.incrementAndGet()) >> IO.unit

      def index: List[EventTransform] => IO[Unit] =
        (l: List[EventTransform]) =>
          IO.delay {
            l.size shouldEqual batch
            count.incrementAndGet()
          } >> IO.unit

      def mapping(event: Event): IO[Option[EventTransform]] =
        IO {
          event match {
            case Executed           => Some(ExecutedTransform)
            case OtherExecuted      => Some(OtherExecutedTransform)
            case AnotherExecuted    => Some(AnotherExecutedTransform)
            case YetAnotherExecuted => Some(YetAnotherExecutedTransform)
            case RetryExecuted      => Some(RetryExecutedTransform)
            case IgnoreExecuted     => Some(IgnoreExecutedTransform)
            case Discarded          => None
            case NotDiscarded       => Some(NotDiscardedTransform)
          }
        }

      val projId = UUID.randomUUID().toString

      lazy val indexConfig = fromConfig[IO]
        .name(projId)
        .plugin(pluginId)
        .tag(tag)
        .batch(batch)
        .init(initFunction(init))
        .mapping(mapping)
        .index(index)
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build

      def buildIndexer: StreamCoordinator[IO, ProjectionProgress] = {
        implicit val ps                                      = projections.ioValue
        val streamByTag: StreamByTag[IO, ProjectionProgress] = new PersistentStreamByTag(indexConfig)
        val actor                                            = TestActorRef(new StreamCoordinatorActor(streamByTag.fetchInit, streamByTag.source))
        new StreamCoordinator[IO, ProjectionProgress](actor)
      }
    }

    "index existing events" in new Context[Event](name = "agg", tag = "executed") {
      val indexer = buildIndexer
      agg.append("first", Fixture.Executed).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 1L
        init.get shouldEqual 11L
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.processedCount shouldEqual 1L
        state.discardedCount shouldEqual 0L
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "recover from temporary failures on init function" in new Context[Event](name = "something", tag = "yetanother") {

      val initCalled = new AtomicLong(0L)

      override def initFunction(init: AtomicLong): IO[Unit] =
        IO.delay {
          if (initCalled.compareAndSet(0L, 1L) || initCalled.compareAndSet(1L, 2L))
            throw new RetriableErr("recoverable error")
          else {
            init.incrementAndGet()
          }
        } >> IO.unit

      val indexer = buildIndexer

      agg.append("a", Fixture.YetAnotherExecuted).unsafeRunAsyncAndForget()

      eventually {
        initCalled.get shouldEqual 2L
        init.get shouldEqual 11L
        count.get shouldEqual 1L
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.processedCount shouldEqual 1L
        state.discardedCount shouldEqual 0L
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "select only the configured event types" in new Context[Event](name = "selected", batch = 2, tag = "other") {
      val indexer = buildIndexer
      agg.append("first", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("second", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("third", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("selected1", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected2", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected3", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected4", Fixture.OtherExecuted).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 2L
        init.get shouldEqual 11L
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.processedCount shouldEqual 4L
        state.discardedCount shouldEqual 0L
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "count discarded events" in new Context[Event](name = "discardtest", batch = 1, tag = "discard") {
      val indexer = buildIndexer
      agg.append("first", Fixture.NotDiscarded).unsafeRunAsyncAndForget()
      agg.append("second", Fixture.NotDiscarded).unsafeRunAsyncAndForget()
      agg.append("third", Fixture.NotDiscarded).unsafeRunAsyncAndForget()
      agg.append("discarded1", Fixture.Discarded).unsafeRunAsyncAndForget()
      agg.append("discarded2", Fixture.Discarded).unsafeRunAsyncAndForget()
      agg.append("discarded3", Fixture.Discarded).unsafeRunAsyncAndForget()
      agg.append("discarded4", Fixture.Discarded).unsafeRunAsyncAndForget()

      eventually {
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.discardedCount shouldEqual 4L
        state.processedCount shouldEqual 7L
      }
      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "restart the indexing if the Done is emitted" in new Context[Event](name = "agg2", tag = "another") {
      val indexer = buildIndexer
      agg.append("first", Fixture.AnotherExecuted).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 1L
        init.get shouldEqual 11L
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.processedCount shouldEqual 1L
        state.discardedCount shouldEqual 0L
      }
      indexer.actor ! Done

      agg.append("second", Fixture.AnotherExecuted).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 2L
        init.get shouldEqual 12L
        val state = indexer.state().some.asInstanceOf[OffsetProgress]
        state.processedCount shouldEqual 2L
        state.discardedCount shouldEqual 0L
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "retry when index function fails" in new Context[RetryExecuted.type](name = "retry", tag = "retry") {
      override val index =
        (_: List[EventTransform]) => IO.delay { throw SomeError(count.incrementAndGet()) }

      val indexer = buildIndexer

      agg.append("retry", Fixture.RetryExecuted).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 4L
        init.get shouldEqual 11L
      }
      eventually {
        projections.ioValue
          .failures(projId)
          .runFold(Vector.empty[Fixture.Event]) {
            case (acc, (ev, _)) => acc :+ ev
          }
          .futureValue shouldEqual List(RetryExecuted)
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }

    "not retry when index function fails with a non RetriableErr" in new Context[IgnoreExecuted.type]("ignore",
                                                                                                      tag = "ignore") {
      override val index =
        (_: List[EventTransform]) => IO.delay(throw SomeOtherError(count.incrementAndGet()))

      val indexer = buildIndexer
      val _       = agg.append("ignore", Fixture.IgnoreExecuted).ioValue

      eventually {
        count.get shouldEqual 1L
        init.get shouldEqual 11L
      }

      eventually {
        projections.ioValue
          .failures(projId)
          .runFold(Vector.empty[Fixture.Event]) {
            case (acc, (ev, _)) => acc :+ ev
          }
          .futureValue shouldEqual List(IgnoreExecuted)
      }

      watch(indexer.actor)
      indexer.stop().ioValue
      expectTerminated(indexer.actor)
    }
  }

}

object SequentialTagIndexerSpec {
  class RetriableErr(message: String)    extends Exception(message)
  case class SomeError(count: Long)      extends RetriableErr("some error")
  case class SomeOtherError(count: Long) extends Exception("some OTHER error")

}
