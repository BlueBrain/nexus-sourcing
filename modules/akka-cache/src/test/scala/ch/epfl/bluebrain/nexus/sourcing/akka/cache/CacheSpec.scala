package ch.epfl.bluebrain.nexus.sourcing.akka.cache

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.TestKit
import cats.Show
import ch.epfl.bluebrain.nexus.sourcing.akka.cache.CacheError.EmptyKey
import ch.epfl.bluebrain.nexus.sourcing.akka.cache.CacheSpec.Elem
import ch.epfl.bluebrain.nexus.sourcing.akka.cache.ShardedCache.CacheSettings
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class CacheSpec
    extends TestKit(ActorSystem("CacheAkkaSpec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {
  override implicit val patienceConfig = PatienceConfig(6 seconds, 100 millis)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val cache: Cache[Future, Elem, Int] = ShardedCache[Elem, Int]("some", CacheSettings())

  "A Cache" should {

    "add elements" in {
      cache.put(Elem("one"), 1).futureValue shouldEqual (())
      cache.put(Elem("two"), 2).futureValue shouldEqual (())
      cache.put(Elem("three"), 3).futureValue shouldEqual (())
      cache.put(Elem("one"), 4).futureValue shouldEqual (())
      whenReady(cache.put(Elem(""), 1).failed)(e => e shouldEqual EmptyKey)
    }
    "fetch the added elements" in {
      cache.get(Elem("one")).futureValue shouldEqual Some(4)
      cache.get(Elem("two")).futureValue shouldEqual Some(2)
      cache.get(Elem("three")).futureValue shouldEqual Some(3)
      cache.get(Elem("five")).futureValue shouldEqual None
      cache.getOrElse(Elem("five"), 5).futureValue shouldEqual 5
      whenReady(cache.get(Elem("")).failed)(e => e shouldEqual EmptyKey)
    }

    "remove elements" in {
      cache.remove(Elem("one")).futureValue shouldEqual (())
      cache.get(Elem("one")).futureValue shouldEqual None
      cache.remove(Elem("a")).futureValue shouldEqual (())
      whenReady(cache.remove(Elem("")).failed)(e => e shouldEqual EmptyKey)
    }
  }
}
object CacheSpec {
  final case class Elem(value: String)
  implicit val showElem: Show[Elem] = Show.show(_.value)
}
