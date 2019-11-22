package ch.epfl.bluebrain.nexus.sourcing.akka

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.{PassivationStrategyConfig, RetryStrategyConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpecLike}
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SourcingConfigSpec
    extends TestKit(ActorSystem("SourcingConfigSpec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with OptionValues {

  val config = SourcingConfig(
    10 seconds,
    "inmemory-journal",
    5 seconds,
    "global",
    10,
    PassivationStrategyConfig(Some(5 seconds), Some(0 milliseconds)),
    RetryStrategyConfig("exponential", 100 milliseconds, 10 hours, 7, 2.0, 500 milliseconds)
  )

  "SourcingConfig" should {

    "read from config file" in {
      val readConfig = ConfigFactory.parseFile(new File(getClass.getResource("/example-sourcing.conf").toURI))
      ConfigSource.fromConfig(readConfig).at("sourcing").loadOrThrow[SourcingConfig] shouldEqual config
    }

    "return AkkaSourcingConfig" in {
      config.akkaSourcingConfig shouldEqual AkkaSourcingConfig(
        10 second,
        "inmemory-journal",
        5 seconds,
        ExecutionContext.global
      )
    }
    "return passivation strategy" in {
      val strategy = config.passivationStrategy()

      strategy.lapsedSinceLastInteraction.value shouldEqual (5 seconds)
      strategy.lapsedSinceRecoveryCompleted.value shouldEqual (0 milliseconds)
    }

  }
}
