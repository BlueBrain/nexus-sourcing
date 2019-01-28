package ch.epfl.bluebrain.nexus.sourcing.akka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit._
import cats.effect.{ContextShift, IO, Timer}
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.{Backoff, Linear, Never, Once}
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RetryerSpec
    extends TestKit(ActorSystem("RetryerSpec"))
    with WordSpecLike
    with Matchers
    with OptionValues
    with BeforeAndAfterAll
    with ScalaFutures
    with EitherValues {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1.second.dilated, 30 milliseconds)

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  abstract class IncrementAndFail {
    val count = new AtomicInteger(0)
    val fa: IO[Unit] = IO {
      val _ = count.incrementAndGet()
      if (count.get() > -1) throw new RuntimeException
    }
  }

  abstract class IncrementAndPass(retries: Int) {
    val count = new AtomicInteger(0)
    val fa: IO[Unit] = IO {
      val _ = count.incrementAndGet() match {
        case i if i < retries => throw new RuntimeException
        case _                => ()
      }
    }
  }

  abstract class IncrementCondition(conditionPassed: Int) {
    val count = new AtomicInteger(0)
    val fa: IO[Option[Int]] = IO {
      count.incrementAndGet() match {
        case i if i >= conditionPassed => Some(i)
        case _                         => None
      }
    }
  }

  "A Retryer" should {

    "retry exponentially when it fails" in new IncrementAndFail {
      val strategy: RetryStrategy = Backoff(100 millis, 2.5 second, maxRetries = 4, randomFactor = 0.0)
      implicit val r              = Retryer[IO, Throwable](strategy)
      //Should retry at 100 mills + 200 millis + 400 millis + 800 millis
      val _ = fa.retry.unsafeToFuture().failed.futureValue(timeout(1600 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 5
    }

    "retry exponentially when it fails (capped)" in new IncrementAndFail {
      val strategy: RetryStrategy = Backoff(200 millis, 200 millis, maxRetries = 3, randomFactor = 0.0)
      implicit val r              = Retryer[IO, Throwable](strategy)
      //Should retry at 200 mills + 200 mills + 200 mills
      val _ = fa.retry.unsafeToFuture().failed.futureValue(timeout(700 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 4
    }

    "retry linearly when it fails" in new IncrementAndFail {
      val strategy: RetryStrategy = Linear(100 millis, 2.5 second, maxRetries = 4, increment = 150 millis)
      implicit val r              = Retryer[IO, Throwable](strategy)
      //Should retry at 100 mills + 250 millis + 400 millis + 550 millis
      val _ = fa.retry.unsafeToFuture().failed.futureValue(timeout(1400 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 5
    }

    "retry linearly when it fails (capped)" in new IncrementAndFail {
      val strategy: RetryStrategy = Linear(200 millis, 200 millis, maxRetries = 3)
      implicit val r              = Retryer[IO, Throwable](strategy)
      //Should retry at 200 mills + 200 mills + 200 mills
      val _ = fa.retry.unsafeToFuture().failed.futureValue(timeout(700 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 4
    }

    "not retry with Never strategy" in new IncrementAndFail {
      val strategy: RetryStrategy = Never
      implicit val r              = Retryer[IO, Throwable](strategy)
      val _                       = fa.retry.unsafeToFuture().failed.futureValue(timeout(100 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 1
    }

    "retry once strategy" in new IncrementAndFail {
      val strategy: RetryStrategy = Once(100 millis)
      implicit val r              = Retryer[IO, Throwable](strategy)
      val _                       = fa.retry.unsafeToFuture().failed.futureValue(timeout(300 millis)) shouldBe a[RuntimeException]
      count.get shouldEqual 2
    }

    "retry until it passes" in new IncrementAndPass(3) {
      val strategy: RetryStrategy = Backoff(100 millis, 2.5 second, maxRetries = 4, randomFactor = 0.0)
      implicit val r              = Retryer[IO, Throwable](strategy)
      //Should retry at 100 mills + 200 millis
      val _ = fa.retry.unsafeToFuture().futureValue(timeout(400 millis))
      count.get shouldEqual 3
    }

    "retry until condition satisfied" in new IncrementCondition(3) {
      val strategy: RetryStrategy = Backoff(100 millis, 2.5 second, maxRetries = 4, randomFactor = 0.0)
      implicit val r              = RetryerMap[IO, Throwable](strategy)
      //Should retry at 100 mills + 200 millis
      val _ = fa
        .mapRetry({ case Some(v) => v }, new IllegalArgumentException: Throwable)
        .unsafeToFuture()
        .futureValue(timeout(400 millis)) shouldEqual 3
      count.get shouldEqual 3
    }

    "retry and fail when condition never satisfied" in new IncrementCondition(10) {
      val strategy: RetryStrategy = Backoff(100 millis, 2.5 second, maxRetries = 4, randomFactor = 0.0)
      implicit val r              = RetryerMap[IO, Throwable](strategy)
      //Should retry at 100 mills + 200 millis + 400 millis + 800 millis
      val _ = fa
        .mapRetry({ case Some(v) => v }, new IllegalArgumentException: Throwable)
        .unsafeToFuture()
        .failed
        .futureValue(timeout(1600 millis)) shouldBe a[IllegalArgumentException]
      count.get shouldEqual 5
    }
  }
}
