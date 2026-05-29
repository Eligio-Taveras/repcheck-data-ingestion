package repcheck.members.committees.app

import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO, Resource}

import org.http4s.Response
import org.http4s.client.Client

import doobie.Transactor
import doobie.free.connection

import pureconfig.ConfigSource

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.app.CommitteeHistoryLoaderPipeline.{AppConfig, LoaderResources}
import repcheck.members.committees.client.CdirCommitteeSource
import repcheck.members.committees.model.{CommitteeMemberInsert, HistoricalMemberRow}
import repcheck.members.committees.persistence.{
  CommitteeMemberRepository,
  CommitteeRepository,
  HistoricalMemberRepository,
}
import repcheck.members.committees.pipeline.CommitteeHistoryLoader

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

  private val noClient: Client[IO] = Client[IO](_ => Resource.pure(Response[IO]()))

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
          |historical { current-congress = 119, lookback-congresses = 1 }
          |govinfo { base-url = "https://api.govinfo.gov", api-key = "k" }""".stripMargin
      )
      .loadOrThrow[AppConfig]

  private def stubLoader(resources: LoaderResources[IO]): CommitteeHistoryLoader[IO] = {
    val committeeRepo       = mock[CommitteeRepository]
    val committeeMemberRepo = mock[CommitteeMemberRepository]
    val memberRepo          = mock[HistoricalMemberRepository]
    val _                   = when(committeeRepo.listAll()).thenReturn(connection.pure(Nil))
    val _ = when(memberRepo.membersForCongress(anyInt())).thenReturn(connection.pure(List.empty[HistoricalMemberRow]))
    val _ = when(committeeMemberRepo.upsert(any[CommitteeMemberInsert])).thenReturn(connection.pure(()))
    val source = new CdirCommitteeSource[IO] {
      def committeeListingTexts(congress: Int, runId: Long): IO[List[String]] = IO.pure(Nil)
    }
    new CommitteeHistoryLoader[IO](
      source,
      committeeRepo,
      committeeMemberRepo,
      memberRepo,
      resources.xa,
      appConfig.historical,
      Set("Georgia"),
      noopLogger,
    )
  }

  "runWithFactories" should "load and exit successfully" in {
    val exit = CommitteeHistoryLoaderPipeline
      .runWithFactories[IO](
        args = List("cfg-unused", "5"),
        configLoader = IO.pure(appConfig),
        loggerFactory = _ => IO.pure(noopLogger),
        resourceBuilder = _ => Resource.pure[IO, LoaderResources[IO]](LoaderResources(testXa, noClient)),
        loaderFactory = (resources, _, _) => stubLoader(resources),
      )
      .unsafeRunSync()

    exit shouldBe ExitCode.Success
  }

  "buildLoader" should "construct a loader from the wired resources" in {
    val loader =
      CommitteeHistoryLoaderPipeline.buildLoader[IO](LoaderResources(testXa, noClient), appConfig, noopLogger)
    loader shouldBe a[CommitteeHistoryLoader[IO]]
  }

}
