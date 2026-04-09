import sbt.*
import Versions.*

object Dependencies {
  private val emberClient =
    "org.http4s" %% "http4s-ember-client" % http4sVersion
  private val emberServer =
    "org.http4s" %% "http4s-ember-server" % http4sVersion
  private val http4sDsl =
    "org.http4s" %% "http4s-dsl" % http4sVersion
  private val http4sCirce =
    "org.http4s" %% "http4s-circe" % http4sVersion

  private val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  private val circeLiteral = "io.circe" %% "circe-literal" % circeVersion
  private val circeCore    = "io.circe" %% "circe-core" % circeVersion
  private val circeParser  = "io.circe" %% "circe-parser" % circeVersion

  private val pureConfigCore =
    "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion
  private val doobieCore =
    "org.tpolecat" %% "doobie-core" % doobieVersion
  private val doobieHikari =
    "org.tpolecat" %% "doobie-hikari" % doobieVersion
  private val doobiePostgres =
    "org.tpolecat" %% "doobie-postgres" % doobieVersion

  private val catsEffectCore = "org.typelevel" %% "cats-effect" % catsEffectVersion

  // available for 2.12, 2.13, 3.2
  private val fs2Core = "co.fs2" %% "fs2-core" % fs2Version

  // optional I/O library
  private val fs2IO = "co.fs2" %% "fs2-io" % fs2Version

  // optional reactive streams interop
  private val fs2ReactiveStreams = "co.fs2" %% "fs2-reactive-streams" % fs2Version

  // optional scodec interop
  private val fs2Scodec = "co.fs2" %% "fs2-scodec" % fs2Version

  // Google Cloud Pub/Sub
  private val gcpPubSub = "com.google.cloud" % "google-cloud-pubsub" % gcpPubSubVersion

  // XML parsing
  private val scalaXml = "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion
  private val http4sScalaXml = "org.http4s" %% "http4s-scala-xml" % "0.23.14"

  // Diffing
  private val difflicious = "com.github.jatcwang" %% "difflicious-core" % Versions.difflicious

  // Logging
  private val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4catsVersion
  private val logbackClassic = "ch.qos.logback" % "logback-classic" % logbackVersion

  // Test dependencies
  private val scalatestPlusMockito = "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % Test
  private val catsEffectTesting = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
  private val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion % Test
  private val wiremock = "org.wiremock" % "wiremock-standalone" % wiremockVersion % Test

  val doobie: Seq[ModuleID]     = Seq(doobieCore, doobieHikari, doobiePostgres)
  val pureConfig: Seq[ModuleID] = Seq(pureConfigCore)
  val circe: Seq[ModuleID]      = Seq(circeCore, circeGeneric, circeLiteral, circeParser)
  val catsEffect: Seq[ModuleID] = Seq(catsEffectCore)
  val http4sEmber: Seq[ModuleID] =
    Seq(emberClient, emberServer, http4sDsl, http4sCirce)

  val fs2: Seq[ModuleID] = Seq(fs2Core, fs2IO, fs2ReactiveStreams, fs2Scodec)

  val pubSub: Seq[ModuleID] = Seq(gcpPubSub)

  val xml: Seq[ModuleID] = Seq(scalaXml, http4sScalaXml)

  val logging: Seq[ModuleID] = Seq(log4catsSlf4j, logbackClassic)

  val diff: Seq[ModuleID] = Seq(difflicious)

  val testDeps: Seq[ModuleID] = Seq(scalatestPlusMockito, catsEffectTesting, catsEffectTestkit, wiremock)
}
