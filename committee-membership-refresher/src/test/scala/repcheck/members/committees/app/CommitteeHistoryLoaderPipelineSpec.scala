package repcheck.members.committees.app

import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO, Resource}

import fs2.Stream

import doobie.Transactor

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.app.CommitteeHistoryLoaderPipeline.AppConfig
import repcheck.members.committees.client.HistoricalAssignmentTsvReader
import repcheck.members.committees.config.HistoricalLoaderConfig
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.committees.pipeline.CommitteeHistoryLoader
import repcheck.members.common.persistence.MemberRepository

class CommitteeHistoryLoaderPipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:committee-history-pipeline;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  // Build AppConfig via HOCON so we don't depend on the external DatabaseConfig constructor shape.
  private val appConfig: AppConfig =
    ConfigSource
      .string(
        """database {
          |  host = "localhost"
          |  port = 5432
          |  database = "repcheck"
          |  username = "repcheck"
          |  password = "repcheck"
          |  max-connections = 5
          |}
          |historical { file-path = "/unused.tsv", parallelism = 1 }""".stripMargin
      )
      .loadOrThrow[AppConfig]

  private def loaderWithMocks(config: HistoricalLoaderConfig): CommitteeHistoryLoader[IO] =
    new CommitteeHistoryLoader[IO](
      mock[CommitteeRepository],
      mock[CommitteeMemberRepository],
      mock[MemberRepository],
      testXa,
      config,
      noopLogger,
    )

  "runWithFactories" should "load and exit successfully" in {
    val exit = CommitteeHistoryLoaderPipeline
      .runWithFactories[IO](
        args = List("cfg-unused", "5"),
        configLoader = IO.pure(appConfig),
        loggerFactory = _ => IO.pure(noopLogger),
        transactorFactory = _ => Resource.pure[IO, Transactor[IO]](testXa),
        // Header-only input → no data rows resolved, so the mock repos are never invoked.
        linesFactory = _ => Stream.emit(HistoricalAssignmentTsvReader.Header),
        loaderFactory = (_, cfg, _) => loaderWithMocks(cfg),
      )
      .unsafeRunSync()

    exit shouldBe ExitCode.Success
  }

  "buildLoader" should "construct a loader from the wired repositories" in {
    val loader = CommitteeHistoryLoaderPipeline.buildLoader[IO](testXa, appConfig.historical, noopLogger)
    loader shouldBe a[CommitteeHistoryLoader[IO]]
  }

}
