addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
// Disabled locally: scalafix plugin triggers an initialization error when the
// project is loaded from another build via checkouts. Commenting it out here
// allows the checkout to be used as a RootProject for local development.
// To re-enable, uncomment the line below.
// addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.9")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
