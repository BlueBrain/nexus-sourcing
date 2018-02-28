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
val catsVersion                 = "1.0.1"
val akkaVersion                 = "2.5.9"
val akkaPersistenceInMemVersion = "2.5.1.1"
val shapelessVersion            = "2.3.3"
val scalaTestVersion            = "3.0.5"

// Dependency modules
lazy val catsCore             = "org.typelevel"       %% "cats-core"                 % catsVersion
lazy val shapeless            = "com.chuusai"         %% "shapeless"                 % shapelessVersion
lazy val akkaPersistence      = "com.typesafe.akka"   %% "akka-persistence"          % akkaVersion
lazy val akkaPersistenceQuery = "com.typesafe.akka"   %% "akka-persistence-query"    % akkaVersion
lazy val akkaClusterSharding  = "com.typesafe.akka"   %% "akka-cluster-sharding"     % akkaVersion
lazy val akkaPersistenceInMem = "com.github.dnvriend" %% "akka-persistence-inmemory" % akkaPersistenceInMemVersion
lazy val akkaTestKit          = "com.typesafe.akka"   %% "akka-testkit"              % akkaVersion
lazy val scalaTest            = "org.scalatest"       %% "scalatest"                 % scalaTestVersion

lazy val core = project
  .in(file("modules/core"))
  .settings(
    name                := "sourcing-core",
    moduleName          := "sourcing-core",
    libraryDependencies ++= Seq(catsCore, scalaTest % Test)
  )

lazy val akkaCache = project
  .in(file("modules/akka-cache"))
  .settings(
    name       := "sourcing-akka-cache",
    moduleName := "sourcing-akka-cache",
    libraryDependencies ++= Seq(
      shapeless,
      akkaPersistence,
      akkaPersistenceQuery,
      akkaClusterSharding,
      akkaTestKit          % Test,
      akkaPersistenceInMem % Test,
      scalaTest            % Test
    )
  )

lazy val akka = project
  .in(file("modules/akka"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name       := "sourcing-akka",
    moduleName := "sourcing-akka",
    libraryDependencies ++= Seq(
      shapeless,
      akkaPersistence,
      akkaPersistenceQuery,
      akkaClusterSharding,
      akkaTestKit          % Test,
      akkaPersistenceInMem % Test,
      scalaTest            % Test
    )
  )

lazy val mem = project
  .in(file("modules/mem"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name                := "sourcing-mem",
    moduleName          := "sourcing-mem",
    libraryDependencies ++= Seq(scalaTest % Test)
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "sourcing",
    moduleName := "sourcing"
  )
  .aggregate(core, akkaCache, akka, mem)

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

addCommandAlias("review", ";clean;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate")
