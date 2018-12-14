package ch.epfl.bluebrain.nexus.sourcing.akka

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.{PassivationStrategyConfig, RetryStrategyConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}
import pureconfig.loadConfigOrThrow

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SourcingConfigSpec
    extends TestKit(ActorSystem("SourcingConfigSpec"))
    with WordSpecLike
    with Matchers
    with OptionValues {

  val config = SourcingConfig(
    10 seconds,
    "inmemory-journal",
    5 seconds,
    "global",
    10,
    PassivationStrategyConfig(Some(5 seconds), Some(0 milliseconds)),
    RetryStrategyConfig("exponential", 100 milliseconds, 7, 2)
  )

  "SourcingConfig" should {

    "read from config file" in {
      val readConfig = ConfigFactory.parseFile(new File(getClass.getResource("/example-sourcing.conf").toURI))
      loadConfigOrThrow[SourcingConfig](readConfig, "sourcing") shouldEqual config
    }

    "return AkkaSourcingConfig" in {
      config.akkaSourcingConfig shouldEqual AkkaSourcingConfig(10 second,
                                                               "inmemory-journal",
                                                               5 seconds,
                                                               ExecutionContext.global)
    }
    "return passivation strategy" in {
      val strategy = config.passivationStrategy()

      strategy.lapsedSinceLastInteraction.value shouldEqual (5 seconds)
      strategy.lapsedSinceRecoveryCompleted.value shouldEqual (0 milliseconds)
    }

  }
}
