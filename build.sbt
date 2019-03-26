/*
scalafmt: {
  style = defaultWithAlign
  maxColumn = 150
  align.tokens = [
    { code = "=>", owner = "Case" }
    { code = "?", owner = "Case" }
    { code = "extends", owner = "Defn.(Class|Trait|Object)" }
    { code = "//", owner = ".*" }
    { code = "{", owner = "Template" }
    { code = "}", owner = "Template" }
    { code = ":=", owner = "Term.ApplyInfix" }
    { code = "++=", owner = "Term.ApplyInfix" }
    { code = "+=", owner = "Term.ApplyInfix" }
    { code = "%", owner = "Term.ApplyInfix" }
    { code = "%%", owner = "Term.ApplyInfix" }
    { code = "%%%", owner = "Term.ApplyInfix" }
    { code = "->", owner = "Term.ApplyInfix" }
    { code = "?", owner = "Term.ApplyInfix" }
    { code = "<-", owner = "Enumerator.Generator" }
    { code = "?", owner = "Enumerator.Generator" }
    { code = "=", owner = "(Enumerator.Val|Defn.(Va(l|r)|Def|Type))" }
  ]
}
 */

// Dependency versions
val akkaVersion                     = "2.5.21"
val akkaHttpVersion                 = "10.1.8"
val akkaPersistenceCassandraVersion = "0.93"
val akkaPersistenceInMemVersion     = "2.5.1.1"
val catsVersion                     = "1.6.0"
val commonsVersion                  = "0.11.8"
val catsEffectVersion               = "1.2.0"
val circeVersion                    = "0.11.1"
val journalVersion                  = "3.0.19"
val logbackVersion                  = "1.2.3"
val shapelessVersion                = "2.3.3"
val scalaTestVersion                = "3.0.7"
val pureconfigVersion               = "0.10.2"

// Dependency modules
lazy val catsCore                 = "org.typelevel"           %% "cats-core"                           % catsVersion
lazy val catsEffect               = "org.typelevel"           %% "cats-effect"                         % catsEffectVersion
lazy val shapeless                = "com.chuusai"             %% "shapeless"                           % shapelessVersion
lazy val akkaActor                = "com.typesafe.akka"       %% "akka-actor"                          % akkaVersion
lazy val akkaCluster              = "com.typesafe.akka"       %% "akka-cluster"                        % akkaVersion
lazy val akkaClusterSharding      = "com.typesafe.akka"       %% "akka-cluster-sharding"               % akkaVersion
lazy val akkaHttpTestKit          = "com.typesafe.akka"       %% "akka-http-testkit"                   % akkaHttpVersion
lazy val akkaPersistence          = "com.typesafe.akka"       %% "akka-persistence"                    % akkaVersion
lazy val akkaPersistenceCassandra = "com.typesafe.akka"       %% "akka-persistence-cassandra"          % akkaPersistenceCassandraVersion
lazy val akkaPersistenceLauncher  = "com.typesafe.akka"       %% "akka-persistence-cassandra-launcher" % akkaPersistenceCassandraVersion
lazy val akkaPersistenceQuery     = "com.typesafe.akka"       %% "akka-persistence-query"              % akkaVersion
lazy val akkaPersistenceInMem     = "com.github.dnvriend"     %% "akka-persistence-inmemory"           % akkaPersistenceInMemVersion
lazy val akkaTestKit              = "com.typesafe.akka"       %% "akka-testkit"                        % akkaVersion
lazy val akkaSlf4j                = "com.typesafe.akka"       %% "akka-slf4j"                          % akkaVersion
lazy val circeCore                = "io.circe"                %% "circe-core"                          % circeVersion
lazy val circeParser              = "io.circe"                %% "circe-parser"                        % circeVersion
lazy val circeGenericExtras       = "io.circe"                %% "circe-generic-extras"                % circeVersion
lazy val journal                  = "io.verizon.journal"      %% "core"                                % journalVersion
lazy val logback                  = "ch.qos.logback"          % "logback-classic"                      % logbackVersion
lazy val scalaTest                = "org.scalatest"           %% "scalatest"                           % scalaTestVersion
lazy val commonsTest              = "ch.epfl.bluebrain.nexus" %% "commons-test"                        % commonsVersion
lazy val pureconfig               = "com.github.pureconfig"   %% "pureconfig"                          % pureconfigVersion

lazy val core = project
  .in(file("modules/core"))
  .settings(
    name       := "sourcing-core",
    moduleName := "sourcing-core",
    libraryDependencies ++= Seq(
      akkaClusterSharding,
      akkaPersistence,
      akkaPersistenceQuery,
      catsCore,
      catsEffect,
      akkaPersistenceInMem % Test,
      akkaSlf4j            % Test,
      akkaTestKit          % Test,
      logback              % Test,
      scalaTest            % Test,
      pureconfig           % Test
    )
  )

lazy val stream = project
  .in(file("modules/stream"))
  .dependsOn(core)
  .settings(
    name       := "sourcing-stream",
    moduleName := "sourcing-stream",
    libraryDependencies ++= Seq(
      akkaActor,
      akkaCluster,
      akkaPersistenceCassandra,
      circeCore,
      circeGenericExtras,
      circeParser,
      journal,
      pureconfig,
      shapeless,
      akkaPersistenceLauncher % Test,
      akkaTestKit             % Test,
      akkaHttpTestKit         % Test,
      akkaSlf4j               % Test,
      commonsTest             % Test,
      pureconfig              % Test,
      scalaTest               % Test,
    )
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "sourcing",
    moduleName := "sourcing"
  )
  .aggregate(core, stream)

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

lazy val noPublish = Seq(
  publishLocal    := {},
  publish         := {},
  publishArtifact := false,
)

inThisBuild(
  List(
    homepage := Some(url("https://github.com/BlueBrain/nexus-sourcing")),
    licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    scmInfo  := Some(ScmInfo(url("https://github.com/BlueBrain/nexus-sourcing"), "scm:git:git@github.com:BlueBrain/nexus-sourcing.git")),
    developers := List(
      Developer("bogdanromanx", "Bogdan Roman", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("hygt", "Henry Genet", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("umbreak", "Didac Montero Mendez", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("wwajerowicz", "Wojtek Wajerowicz", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
    ),
    // These are the sbt-release-early settings to configure
    releaseEarlyWith              := BintrayPublisher,
    releaseEarlyNoGpg             := true,
    releaseEarlyEnableSyncToMaven := false,
  ))

addCommandAlias("review", ";clean;scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate")
