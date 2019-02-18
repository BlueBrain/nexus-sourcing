package ch.epfl.bluebrain.nexus.sourcing.indexer

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}
import pureconfig.generic.auto._
import pureconfig.loadConfigOrThrow

import scala.concurrent.duration._

class IndexingConfigSpec
    extends TestKit(ActorSystem("IndexingConfigSpec"))
    with WordSpecLike
    with Matchers
    with OptionValues {

  val config = IndexingConfig(
    10,
    40 millis,
    RetryStrategyConfig("exponential", 100 milliseconds, 10 hours, 7, 0.5, 500 milliseconds)
  )

  "IndexingConfig" should {

    "read from config file" in {
      val readConfig = ConfigFactory.parseFile(new File(getClass.getResource("/example-indexing.conf").toURI))
      loadConfigOrThrow[IndexingConfig](readConfig) shouldEqual config
    }
  }
}
