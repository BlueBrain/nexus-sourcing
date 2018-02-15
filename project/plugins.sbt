resolvers += Resolver.bintrayRepo("bbp", "nexus-releases")

addSbtPlugin("ch.epfl.bluebrain.nexus" % "sbt-nexus"         % "0.6.2")
addSbtPlugin("io.get-coursier"         % "sbt-coursier"      % "1.0.1")
addSbtPlugin("ch.epfl.scala"           % "sbt-release-early" % "2.1.1")
