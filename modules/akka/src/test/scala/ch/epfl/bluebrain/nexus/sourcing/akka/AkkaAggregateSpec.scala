package ch.epfl.bluebrain.nexus.sourcing.akka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import akka.testkit._
import akka.util.Timeout
import cats.arrow.FunctionK
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.AggregateFixture._
import ch.epfl.bluebrain.nexus.sourcing.Command._
import ch.epfl.bluebrain.nexus.sourcing.Event._
import ch.epfl.bluebrain.nexus.sourcing.State.Current
import ch.epfl.bluebrain.nexus.sourcing._
import ch.epfl.bluebrain.nexus.sourcing.akka.Msg._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Random

class AkkaAggregateSpec
    extends TestKit(ActorSystem("AkkaAggregateSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with EitherValues {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1.second.dilated, 30 milliseconds)

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  val config = AkkaSourcingConfig(
    Timeout(1.second.dilated),
    "inmemory-read-journal",
    200 milliseconds,
    ExecutionContext.global
  )

  "A sharded AkkaAggregate" when {

    "configured with immediate passivation an no retries" should {
      val passivation = PassivationStrategy.immediately[State, Command]
      val retry       = RetryStrategy.never[IO]
      val name        = "immediate-passivation-no-retries"
      val agg = AkkaAggregate
        .sharded[IO](name, initialState, next, evaluate[IO], passivation, retry, config, shards = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with no passivation an no retries" should {
      val passivation = PassivationStrategy.never[State, Command]
      val retry       = RetryStrategy.never[IO]
      val name        = "no-passivation-no-retries"
      val agg = AkkaAggregate
        .sharded[IO](name, initialState, next, evaluate[IO], passivation, retry, config, shards = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with fixed passivation an no retries" should {
      val passivation = PassivationStrategy.lapsedSinceRecoveryCompleted[State, Command](10.milliseconds.dilated)
      val retry       = RetryStrategy.never[IO]
      val name        = "fixed-passivation-no-retries"
      val agg = AkkaAggregate
        .sharded[IO](name, initialState, next, evaluate[IO], passivation, retry, config, shards = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with interaction passivation an no retries" should {
      val passivation = PassivationStrategy.lapsedSinceLastInteraction[State, Command](10.milliseconds.dilated)
      val retry       = RetryStrategy.never[IO]
      val name        = "interaction-passivation-no-retries"
      val agg = AkkaAggregate
        .sharded[IO](name, initialState, next, evaluate[IO], passivation, retry, config, shards = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with a retry strategy" should {
      def eval(failCount: Int) = {
        val evaluations = new AtomicInteger(0)
        val f = (state: State, cmd: Command) => {
          if (evaluations.get() < failCount)
            IO.pure(evaluations.incrementAndGet()) *> IO.raiseError(new RuntimeException)
          else IO.pure(evaluations.incrementAndGet()) *> evaluate[IO](state, cmd)
        }
        (evaluations, f)
      }

      "retry the computation once, resulting in success" in {
        val (evaluations, f) = eval(1)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-single-retry-success"
        val retry            = RetryStrategy.once[IO, Throwable]
        val agg = AkkaAggregate
          .sharded[IO](name, initialState, next, f, passivation, retry, config, shards = 10)
          .unsafeRunSync()

        val first = genString()

        agg.evaluateE(first, Increment(0, 2)).unsafeRunSync().right.value
        evaluations.get() shouldEqual 2
      }

      "retry the computation once, resulting in failure" in {
        val (evaluations, f) = eval(100)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-single-retry-failure"
        val retry            = RetryStrategy.once[IO, Throwable]
        val agg = AkkaAggregate
          .sharded[IO](name, initialState, next, f, passivation, retry, config, shards = 10)
          .unsafeRunSync()

        val first = genString()

        val ex = intercept[CommandEvaluationError[Command]] {
          agg.evaluate(first, Increment(0, 2)).unsafeRunSync()
        }
        ex shouldEqual CommandEvaluationError(first, Increment(0, 2), None)
        evaluations.get() shouldEqual 2
      }

      "retry the computation exponentially, resulting in failure" in {
        val (evaluations, f) = eval(100)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-exponential-retry-failure"
        val retry            = RetryStrategy.exponentialBackoff[IO, Throwable](10 millis, 3, 3)
        val agg = AkkaAggregate
          .sharded[IO](name, initialState, next, f, passivation, retry, config, shards = 10)
          .unsafeRunSync()

        val first = genString()

        val start = System.currentTimeMillis()
        val ex = intercept[CommandEvaluationError[Command]] {
          agg.evaluate(first, Increment(0, 2)).unsafeRunSync()
        }
        val duration = System.currentTimeMillis() - start

        duration should be > 130L

        ex shouldEqual CommandEvaluationError(first, Increment(0, 2), None)
        evaluations.get() shouldEqual 4
      }

      "apply the provided FunctionK during retries" in {
        val retries          = new AtomicInteger(0)
        val (evaluations, f) = eval(1)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-kretry-success"
        val retry = RetryStrategy(new FunctionK[IO, IO] {
          override def apply[A](fa: IO[A]): IO[A] = fa.handleErrorWith { _ =>
            retries.incrementAndGet()
            fa
          }
        })
        val agg = AkkaAggregate
          .sharded[IO](name, initialState, next, f, passivation, retry, config, shards = 10)
          .unsafeRunSync()

        val first = genString()
        agg.evaluateS(first, Increment(0, 2)).unsafeRunSync().right.value
        evaluations.get() shouldEqual 2
        retries.get() shouldEqual 1
      }
    }
  }

  "A tree AkkaAggregate" when {

    "configured with immediate passivation an no retries" should {
      val passivation = PassivationStrategy.immediately[State, Command]
      val retry       = RetryStrategy.never[IO]
      val name        = "immediate-passivation-no-retries"
      val agg = AkkaAggregate
        .tree[IO](name, initialState, next, evaluate[IO], passivation, retry, config, poolSize = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with no passivation an no retries" should {
      val passivation = PassivationStrategy.never[State, Command]
      val retry       = RetryStrategy.never[IO]
      val name        = "no-passivation-no-retries"
      val agg = AkkaAggregate
        .tree[IO](name, initialState, next, evaluate[IO], passivation, retry, config, poolSize = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with fixed passivation an no retries" should {
      val passivation = PassivationStrategy.lapsedSinceRecoveryCompleted[State, Command](10.milliseconds.dilated)
      val retry       = RetryStrategy.never[IO]
      val name        = "fixed-passivation-no-retries"
      val agg = AkkaAggregate
        .tree[IO](name, initialState, next, evaluate[IO], passivation, retry, config, poolSize = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with interaction passivation an no retries" should {
      val passivation = PassivationStrategy.lapsedSinceLastInteraction[State, Command](10.milliseconds.dilated)
      val retry       = RetryStrategy.never[IO]
      val name        = "interaction-passivation-no-retries"
      val agg = AkkaAggregate
        .tree[IO](name, initialState, next, evaluate[IO], passivation, retry, config, poolSize = 10)
        .unsafeRunSync()

      val first  = genString()
      val second = genString()
      runTests(agg, name, first, second)
    }

    "configured with a retry strategy" should {
      def eval(failCount: Int) = {
        val evaluations = new AtomicInteger(0)
        val f = (state: State, cmd: Command) => {
          if (evaluations.get() < failCount)
            IO.pure(evaluations.incrementAndGet()) *> IO.raiseError(new RuntimeException)
          else IO.pure(evaluations.incrementAndGet()) *> evaluate[IO](state, cmd)
        }
        (evaluations, f)
      }

      "retry the computation once, resulting in success" in {
        val (evaluations, f) = eval(1)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-single-retry-success"
        val retry            = RetryStrategy.once[IO, Throwable]
        val agg = AkkaAggregate
          .tree[IO](name, initialState, next, f, passivation, retry, config, poolSize = 10)
          .unsafeRunSync()

        val first = genString()
        agg.evaluateS(first, Increment(0, 2)).unsafeRunSync().right.value
        evaluations.get() shouldEqual 2
      }

      "retry the computation once, resulting in failure" in {
        val (evaluations, f) = eval(100)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-single-retry-failure"
        val retry            = RetryStrategy.once[IO, Throwable]
        val agg = AkkaAggregate
          .tree[IO](name, initialState, next, f, passivation, retry, config, poolSize = 10)
          .unsafeRunSync()

        val first = genString()
        val ex = intercept[CommandEvaluationError[Command]] {
          agg.evaluate(first, Increment(0, 2)).unsafeRunSync()
        }
        ex shouldEqual CommandEvaluationError(first, Increment(0, 2), None)
        evaluations.get() shouldEqual 2
      }

      "retry the computation exponentially, resulting in failure" in {
        val (evaluations, f) = eval(100)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-exponential-retry-failure"
        val retry            = RetryStrategy.exponentialBackoff[IO, Throwable](10 millis, 3, 3)
        val agg = AkkaAggregate
          .tree[IO](name, initialState, next, f, passivation, retry, config, poolSize = 10)
          .unsafeRunSync()

        val first = genString()
        val start = System.currentTimeMillis()
        val ex = intercept[CommandEvaluationError[Command]] {
          agg.evaluate(first, Increment(0, 2)).unsafeRunSync()
        }
        val duration = System.currentTimeMillis() - start

        duration should be > 130L

        ex shouldEqual CommandEvaluationError(first, Increment(0, 2), None)
        evaluations.get() shouldEqual 4
      }

      "apply the provided FunctionK during retries" in {
        val retries          = new AtomicInteger(0)
        val (evaluations, f) = eval(1)
        val passivation      = PassivationStrategy.never[State, Command]
        val name             = "no-passivation-kretry-success"
        val retry = RetryStrategy(new FunctionK[IO, IO] {
          override def apply[A](fa: IO[A]): IO[A] = fa.handleErrorWith { _ =>
            retries.incrementAndGet()
            fa
          }
        })
        val agg = AkkaAggregate
          .tree[IO](name, initialState, next, f, passivation, retry, config, poolSize = 10)
          .unsafeRunSync()

        val first = genString()
        agg.evaluateE(first, Increment(0, 2)).unsafeRunSync().right.value
        evaluations.get() shouldEqual 2
        retries.get() shouldEqual 1
      }
    }
  }

  def runTests(agg: Aggregate[IO, String, Event, State, Command, Rejection],
               name: String,
               first: String,
               second: String): Unit = {

    "return its name" in {
      agg.name shouldEqual name
    }

    "update its state when accepting commands" in {
      agg.evaluateE(first, Increment(0, 2)).unsafeRunSync().right.value shouldEqual Incremented(1, 2)
      agg.evaluateS(first, IncrementAsync(1, 5, 10 millis)).unsafeRunSync().right.value shouldEqual Current(2, 7)
      agg.currentState(first).unsafeRunSync() shouldEqual Current(2, 7)
    }

    "return its current seq nr" in {
      agg.lastSequenceNr(first).unsafeRunSync() shouldEqual 2L
    }

    "test without applying changes" in {
      agg.testE(first, Initialize(0)).unsafeRunSync().left.value
      agg.testE(first, Initialize(2)).unsafeRunSync().right.value shouldEqual Initialized(3)
      agg.testS(first, Initialize(2)).unsafeRunSync().right.value shouldEqual Current(3, 0)
      agg.currentState(first).unsafeRunSync() shouldEqual Current(2, 7)
    }

    "not update its state if evaluation fails" in {
      agg.evaluateS(first, Initialize(0)).unsafeRunSync().left.value
      agg.currentState(first).unsafeRunSync() shouldEqual Current(2, 7)
    }

    "evaluate commands one at a time" in {
      agg.evaluateE(first, Initialize(2)).unsafeRunSync().right.value shouldEqual Initialized(3)
      agg.currentState(first).unsafeRunSync() shouldEqual Current(3, 0)
      agg.evaluateS(first, IncrementAsync(3, 2, 30 millis)).unsafeToFuture()
      agg.evaluateE(first, IncrementAsync(4, 2, 10 millis)).unsafeRunSync().right.value shouldEqual Incremented(5, 2)
      agg.currentState(first).unsafeRunSync() shouldEqual Current(5, 4)
    }

    "fold over the event stream in order" in {
      val (_, success) = agg
        .foldLeft(first, (0, true)) {
          case ((lastRev, succeeded), event) => (event.rev, succeeded && event.rev - lastRev == 1)
        }
        .unsafeRunSync()
      success shouldEqual true
    }

    "return all events" in {
      agg.foldLeft(first, 0) { case (acc, _) => acc + 1 }.unsafeRunSync() shouldEqual 5
    }

    "append events" in {
      agg.append(second, Incremented(1, 2)).unsafeRunSync() shouldEqual 1L
      agg.currentState(second).unsafeRunSync() shouldEqual Current(1, 2)
      agg.currentState(first).unsafeRunSync() shouldEqual Current(5, 4)
    }

    "return true for existing ids" in {
      agg.exists(first).unsafeRunSync() shouldEqual true
    }

    "return false for unknown ids" in {
      agg.exists("unknown").unsafeRunSync() shouldEqual false
    }

    "return the sequence number for a snapshot" in {
      agg.snapshot(first).unsafeRunSync() shouldEqual 5L
    }

    "return a timeout when evaluating commands that do not complete" in {
      val ex = intercept[CommandEvaluationTimeout[Command]] {
        agg.evaluate(first, Never(5)).unsafeRunSync()
      }
      ex shouldEqual CommandEvaluationTimeout(first, Never(5))
    }

    "return a timeout when testing commands that do not complete" in {
      val ex = intercept[CommandEvaluationTimeout[Command]] {
        agg.test(first, Never(5)).unsafeRunSync()
      }
      ex shouldEqual CommandEvaluationTimeout(first, Never(5))
    }

    "return the error when evaluating commands that return in error" in {
      val ex = intercept[CommandEvaluationError[Command]] {
        agg.evaluate(first, Boom(5, "the message")).unsafeRunSync()
      }
      ex shouldEqual CommandEvaluationError(first, Boom(5, "the message"), Some("the message"))
    }

    "return the error when testing commands that return in error" in {
      val ex = intercept[CommandEvaluationError[Command]] {
        agg.test(first, Boom(5, "the message")).unsafeRunSync()
      }
      ex shouldEqual CommandEvaluationError(first, Boom(5, "the message"), Some("the message"))
    }

    "return the current state while still evaluating" in {
      val f = agg.evaluate(first, Never(5)).unsafeToFuture()
      agg.currentState(first).unsafeRunSync()
      f.failed.futureValue
    }

    "return the current state while still testing" in {
      val f = agg.test(first, Never(5)).unsafeToFuture()
      agg.currentState(first).unsafeRunSync()
      f.failed.futureValue
    }

    "return the current sequence number while still evaluating" in {
      val f = agg.evaluate(first, Never(5)).unsafeToFuture()
      agg.lastSequenceNr(first).unsafeRunSync()
      f.failed.futureValue
    }

    "return the current sequence number while still testing" in {
      val f = agg.test(first, Never(5)).unsafeToFuture()
      agg.lastSequenceNr(first).unsafeRunSync()
      f.failed.futureValue
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val promise = Promise[Unit]
    Cluster(system).registerOnMemberUp(promise.success(()))
    Cluster(system).join(Cluster(system).selfAddress)
    promise.future.futureValue
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def genString(length: Int = 16, pool: IndexedSeq[Char] = Vector.range('a', 'z')): String = {
    val size = pool.size

    @tailrec
    def inner(acc: String, remaining: Int): String =
      if (remaining <= 0) acc
      else inner(acc + pool(Random.nextInt(size)), remaining - 1)

    inner("", length)
  }
}
