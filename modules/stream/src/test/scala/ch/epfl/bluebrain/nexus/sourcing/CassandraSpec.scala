package ch.epfl.bluebrain.nexus.sourcing

import java.io.File

import _root_.akka.persistence.cassandra.testkit.CassandraLauncher
import ch.epfl.bluebrain.nexus.sourcing.persistence._
import org.scalatest.{BeforeAndAfterAll, Suites}

class CassandraSpec
    extends Suites(
      new SequentialTagIndexerSpec,
      new ProjectionsSpec,
    )
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val cassandraDirectory = new File("target/cassandra")
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = CassandraLauncher.DefaultTestConfigResource,
      clean = true,
      port = 0,
      CassandraLauncher.classpathForResources("logback-test.xml")
    )
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }
}
