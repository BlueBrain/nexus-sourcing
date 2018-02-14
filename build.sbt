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
val catsVersion      = "1.0.1"
val scalaTestVersion = "3.0.4"

// Dependency modules
lazy val catsCore  = "org.typelevel" %% "cats-core" % catsVersion
lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

lazy val core = project
  .in(file("modules/core"))
  .settings(publishSettings)
  .settings(
    name                := "sourcing-core",
    moduleName          := "sourcing-core",
    libraryDependencies ++= Seq(catsCore, scalaTest % Test)
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "sourcing",
    moduleName := "sourcing"
  )
  .aggregate(core)

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

lazy val noPublish = Seq(publishLocal := {}, publish := {})

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/BlueBrain/nexus-sourcing")),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  scmInfo  := Some(ScmInfo(url("https://github.com/BlueBrain/nexus-sourcing"), "scm:git:git@github.com:BlueBrain/nexus-sourcing.git"))
)
