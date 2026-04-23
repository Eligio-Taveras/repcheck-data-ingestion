package repcheck.ingestion.votes.e2e

import scala.concurrent.duration._
import scala.io.Source

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.Resource

import io.circe.parser.decode

import org.http4s.ember.client.EmberClientBuilder

import doobie.implicits._

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.{DockerRequired, PubSubEmulatorFixture}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{
  EventPublisherConfig,
  GooglePubSubEventPublisher,
  PubSubEventPublisher,
}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.app.{VotesPipeline, VotesPipelineResources, VotesProcessorFactory}
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig, VotesPipelineConfig}
import repcheck.ingestion.votes.testing.TransactorFixture
import repcheck.pipeline.models.errors.RetryConfig
import repcheck.pipeline.models.events.VoteRecordedEvent

/**
 * End-to-end integration spec for the votes pipeline. Stands up a full infrastructure stack — DockerPostgres (via
 * `TransactorFixture`), a Pub/Sub emulator (via `PubSubEmulatorFixture`), and a WireMock server replaying recorded
 * Congress.gov + senate.gov fixtures — then drives the real [[VotesPipeline.runWithFactories]] against that stack and
 * asserts the DB state, emitted events, and exit code match the expected end-to-end semantics.
 *
 * ==What this spec proves that unit specs don't==
 *
 * Unit tests cover each collaborator in isolation with mocked dependencies. This spec proves the composition: real
 * Doobie transactions against real Postgres, real Pub/Sub message round-trip through the emulator, real HTTP client
 * behaviour against a real (albeit WireMock-served) endpoint, real change-detection reading real stored state. The
 * handful of behaviours that only emerge end-to-end (atomic archive-then-upsert-then-replace transactions, FK
 * satisfaction via placeholder upserts, chamber-level failure isolation under `Stream.merge`, pipeline exit code
 * aggregation across both chamber streams) all live here.
 *
 * ==Tagging==
 *
 * Tests are tagged `DockerRequired` to match the sibling `FullChainIntegrationSpec` convention. This puts them in the
 * `dockerTestParallel` CI path — excluded from the default `sbt test` fast loop, included in the Docker-backed CI job.
 * Running locally: `sbt 'votesPipeline / testOnly *VotesPipelineE2ESpec -- -n DockerRequired'`.
 *
 * ==Fixture lifecycle==
 *
 *   - `beforeAll` starts the WireMock server (the DockerPostgres / Pub/Sub emulator containers come from their shared
 *     singletons and are started lazily).
 *   - `beforeEach` drains the Pub/Sub subscription and resets WireMock stubs — every test gets a clean inbound queue
 *     and blank stub set.
 *   - `afterEach` is inherited from `TransactorFixture`, which truncates every vote-family table.
 *   - `afterAll` stops WireMock.
 */
class VotesPipelineE2ESpec
    extends AnyFlatSpec
    with Matchers
    with TransactorFixture
    with PubSubEmulatorFixture
    with BeforeAndAfterEach {

  // -----------------------------------------------------------------------------------
  // WireMock server
  // -----------------------------------------------------------------------------------

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private lazy val wireMockBaseUrl: String = s"http://127.0.0.1:${wireMock.port().toString}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    try wireMock.stop()
    catch { case _: Exception => () }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    drainMessages()
    wireMock.resetAll()
  }

  // -----------------------------------------------------------------------------------
  // Shared structured logger — records every log line so tests can assert on correlation
  // IDs flowing through the pipeline.
  // -----------------------------------------------------------------------------------

  final private class CapturingLogger extends PipelineLogger[IO] {
    private val ref = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    override def info(context: LogContext, message: String): IO[Unit] = IO {
      val _ = ref.updateAndGet(xs => xs :+ s"INFO runId=${context.runId} step=${context.stepName} corr=${context.correlationId.fold("-")(_.toString)} | $message")
    }
    override def warn(context: LogContext, message: String): IO[Unit] = IO {
      val _ = ref.updateAndGet(xs => xs :+ s"WARN runId=${context.runId} step=${context.stepName} corr=${context.correlationId.fold("-")(_.toString)} | $message")
    }
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO {
      val _ = ref.updateAndGet(xs => xs :+ s"ERROR runId=${context.runId} step=${context.stepName} corr=${context.correlationId.fold("-")(_.toString)} | $message | cause=${cause.map(_.getClass.getSimpleName).getOrElse("none")}")
    }
    override def debug(context: LogContext, message: String): IO[Unit] = IO {
      val _ = ref.updateAndGet(xs => xs :+ s"DEBUG | $message")
    }

    def lines: List[String] = ref.get()
  }

  // -----------------------------------------------------------------------------------
  // AppConfig + Resources builders — synthesize a test config pointing at WireMock + emulator
  // -----------------------------------------------------------------------------------

  private val testCongress: Int = 119
  private val testSession: Int  = 1

  /**
   * Build the AppConfig the pipeline will see under test. `baseUrl` on both `congressApi` and `senate` is the
   * WireMock URL so every outbound HTTP request goes there; `eventPublisher.topicName` is set to the fixture's
   * ephemeral `topicId` so the pipeline publishes into the same topic the fixture's subscription reads from.
   */
  private def buildAppConfig(): VotesPipeline.AppConfig =
    VotesPipeline.AppConfig(
      database = DatabaseConfig(
        host = "localhost", // unused — the real transactor is supplied via factory
        port = 5432,
        database = "repcheck",
        username = "repcheck",
        password = "repcheck",
        maxConnections = 3,
      ),
      congressApi = CongressGovClientConfig(
        apiKey = "test-api-key",
        baseUrl = wireMockBaseUrl, // no `/v3/` suffix — the House client builds `/house-vote/...` directly from baseUrl
        pageSize = 10,
        pageDelay = 1.millis,
        retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0),
      ),
      pipeline = VotesPipelineConfig(
        house = HouseVotesConfig(
          congress = testCongress,
          session = testSession,
          parallelism = 1,
          pageDelay = 1.millis,
          lookbackDays = 0, // 0 disables the client-side lookback filter — accept every recorded fixture
        ),
        senate = SenateVoteXmlConfig(
          baseUrl = wireMockBaseUrl,
          parallelism = 1,
          requestDelay = 1.millis,
          retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0),
        ),
      ),
      eventPublisher = EventPublisherConfig(
        projectId = emulatorProjectId,
        topicName = topicId, // ephemeral test topic from PubSubEmulatorFixture
        source = "votes-pipeline-e2e-test",
      ),
    )

  /**
   * Build the Resources bundle the pipeline runs against. Uses the fixture's real `xa` (DockerPostgres), a real
   * EmberClient (so the HTTP stack is genuine), and the fixture's `publisher` (a real Publisher pre-bound to the
   * ephemeral emulator topic — `GooglePubSubEventPublisher.publish` ignores its `topic` argument and uses the
   * Publisher's bound topic, so wiring is automatic).
   */
  private def buildResources(): Resource[IO, VotesPipelineResources.Resources[IO]] = {
    val pubSubFactory: EventPublisherConfig => Resource[IO, PubSubEventPublisher[IO]] =
      _ => Resource.pure[IO, PubSubEventPublisher[IO]](new GooglePubSubEventPublisher[IO](publisher))

    VotesPipelineResources.build[IO](
      config = buildAppConfig(),
      transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, doobie.util.transactor.Transactor[IO]](xa),
      httpClientFactory = EmberClientBuilder.default[IO].build,
      pubSubPublisherFactory = pubSubFactory,
    )
  }

  /**
   * Launch the pipeline end-to-end and return (exitCode, capturedLogLines). Uses a fixed set of launcher args so the
   * runId and stepRunId in logs / events are deterministic across tests.
   */
  private def runPipeline(runId: String = "e2e-run-1"): (cats.effect.ExitCode, List[String]) = {
    val capturing = new CapturingLogger
    val args      = List("cfg-ignored", runId, "42")
    val cfg       = buildAppConfig()
    val exitCode = VotesPipeline
      .runWithFactories[IO](
        args = args,
        configLoader = IO.pure(cfg),
        loggerFactory = IO.pure(capturing),
        resourceBuilder = (_: VotesPipeline.AppConfig) => buildResources(),
        processorFactory = VotesProcessorFactory.build[IO],
        streamFactory = (p, rid) => p.streamAll(rid),
      )
      .unsafeRunSync()
    (exitCode, capturing.lines)
  }

  // -----------------------------------------------------------------------------------
  // WireMock stub helpers — load recorded fixtures from `src/test/resources/wiremock/votes/__files/`
  // and program WireMock to replay them on matching URLs.
  // -----------------------------------------------------------------------------------

  private def loadFixture(path: String): String = {
    val src = Source.fromResource(s"wiremock/votes/__files/$path")
    try src.getLines().mkString("\n")
    finally src.close()
  }

  /** Stub the House list endpoint (`/house-vote/{congress}/{session}`) with a fixture body. */
  private def stubHouseList(body: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/house-vote/${testCongress.toString}/${testSession.toString}"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body))
    )
  }

  /** Stub the House `/members` detail endpoint for a specific vote number. */
  private def stubHouseMembers(voteNumber: Int, body: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/house-vote/${testCongress.toString}/${testSession.toString}/${voteNumber.toString}/members"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(body))
    )
  }

  /** Stub the Senate `/roll_call_lists/vote_menu_{c}_{s}.xml` index endpoint. */
  private def stubSenateIndex(body: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/roll_call_lists/vote_menu_${testCongress.toString}_${testSession.toString}.xml"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/xml").withBody(body))
    )
  }

  /** Stub an individual Senate vote XML. `voteNumber` is the un-padded int; the URL uses 5-digit zero-padding. */
  private def stubSenateVote(voteNumber: Int, body: String): Unit = {
    val padded = f"$voteNumber%05d"
    val _ = wireMock.stubFor(
      get(
        urlPathEqualTo(
          s"/roll_call_votes/vote${testCongress.toString}${testSession.toString}/vote_${testCongress.toString}_${testSession.toString}_$padded.xml"
        )
      )
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/xml").withBody(body))
    )
  }

  // -----------------------------------------------------------------------------------
  // DB assertion helpers
  // -----------------------------------------------------------------------------------

  /** Count rows in the `votes` table. */
  private def countVotes(): Long =
    sql"SELECT COUNT(*) FROM votes".query[Long].unique.transact(xa).unsafeRunSync()

  /** Count positions for a given vote id. */
  private def countPositions(voteId: Long): Long =
    sql"SELECT COUNT(*) FROM vote_positions WHERE vote_id = $voteId".query[Long].unique.transact(xa).unsafeRunSync()

  /** Fetch the vote row by natural key; returns the id or raises. */
  private def voteIdByNaturalKey(naturalKey: String): Long =
    sql"SELECT id FROM votes WHERE natural_key = $naturalKey".query[Long].unique.transact(xa).unsafeRunSync()

  /** Count rows in `stance_materialization_status`. */
  private def countStanceStatus(): Long =
    sql"SELECT COUNT(*) FROM stance_materialization_status".query[Long].unique.transact(xa).unsafeRunSync()

  // -----------------------------------------------------------------------------------
  // Pub/Sub assertion helpers
  // -----------------------------------------------------------------------------------

  /**
   * Pull all pending events from the subscription and decode them as `VoteRecordedEvent`s. Short RPC deadline on the
   * fixture means this returns fast even when the queue is empty; we pull twice to absorb any timing variance from
   * the publisher's async acknowledgement path.
   */
  private def pullAllEvents(): List[VoteRecordedEvent] = {
    val first  = pullMessages(100)
    val second = if (first.size < 100) pullMessages(100 - first.size) else List.empty
    (first ++ second).flatMap { msg =>
      val bytes   = msg.getData.toStringUtf8
      // DefaultIngestionEventPublisher wraps payloads in a `PipelineEvent` envelope: { eventType, payload, correlationId, ... }.
      // We want the inner `payload` decoded as `VoteRecordedEvent`.
      val decoded = decode[io.circe.Json](bytes).flatMap { json =>
        json.hcursor.downField("payload").as[VoteRecordedEvent]
      }
      decoded.toOption.toList
    }
  }

  // =====================================================================================
  // Scenario 1 — Happy path: 2 House + 2 Senate votes, all new, all bill-linked.
  // =====================================================================================

  "VotesPipeline E2E" should "scenario 1 — happy path: 2 House + 2 Senate bill-linked votes persist + emit events" taggedAs DockerRequired in {
    // Replay the list endpoint so the House client sees a two-vote page.
    val miniHouseList = """{
      "houseRollCallVotes": [
        {
          "congress": 119,
          "identifier": 11912025240,
          "legislationNumber": "3424",
          "legislationType": "HR",
          "legislationUrl": "https://www.congress.gov/bill/119/house-bill/3424",
          "result": "Passed",
          "rollCallNumber": 240,
          "sessionNumber": 1,
          "sourceDataURL": "https://clerk.house.gov/evs/2025/roll240.xml",
          "startDate": "2025-09-08T18:56:00-04:00",
          "updateDate": "2025-09-09T18:53:19-04:00",
          "url": "https://api.congress.gov/v3/house-vote/119/1/240",
          "voteType": "2/3 Yea-And-Nay"
        },
        {
          "congress": 119,
          "identifier": 1191202596,
          "legislationNumber": "18",
          "legislationType": "SJRES",
          "legislationUrl": "https://www.congress.gov/bill/119/senate-joint-resolution/18",
          "result": "Passed",
          "rollCallNumber": 96,
          "sessionNumber": 1,
          "sourceDataURL": "https://clerk.house.gov/evs/2025/roll096.xml",
          "startDate": "2025-04-09T16:23:00-04:00",
          "updateDate": "2025-06-24T08:55:52-04:00",
          "url": "https://api.congress.gov/v3/house-vote/119/1/96",
          "voteType": "Yea-and-Nay"
        }
      ],
      "pagination": {"count": 2}
    }"""

    stubHouseList(miniHouseList)
    stubHouseMembers(240, loadFixture("house/house-vote-119-1-240-members-hr3424.json"))
    stubHouseMembers(96, loadFixture("house/house-vote-119-1-96-members-sjres18.json"))

    // Mini Senate index with just two votes — 648 (S. 1071) and 632 (H.J.Res. 131).
    val miniSenateIndex = """<?xml version="1.0" encoding="UTF-8"?><vote_summary>
  <congress>119</congress>
  <session>1</session>
  <congress_year>2025</congress_year>
  <votes>
    <vote>
      <vote_number>00648</vote_number>
      <vote_date>17-Dec</vote_date>
      <issue>S. 1071</issue>
      <question>On the Motion</question>
      <result>Agreed to</result>
    </vote>
    <vote>
      <vote_number>00632</vote_number>
      <vote_date>12-Dec</vote_date>
      <issue>H.J.Res. 131</issue>
      <question>On Passage of the Joint Resolution</question>
      <result>Passed</result>
    </vote>
  </votes>
</vote_summary>"""

    stubSenateIndex(miniSenateIndex)
    stubSenateVote(648, loadFixture("senate/vote-119-1-00648-s1071.xml"))
    stubSenateVote(632, loadFixture("senate/vote-119-1-00632-hjres131.xml"))

    // Act
    val (exitCode, _) = runPipeline()

    // Assert — exit code
    val _ = exitCode.code shouldBe 0

    // Assert — DB state: four votes persisted, two per chamber
    val _ = countVotes() shouldBe 4L

    // Assert — each expected natural key exists
    val _ = voteIdByNaturalKey("119-House-1-240") should be > 0L
    val _ = voteIdByNaturalKey("119-House-1-96") should be > 0L
    val _ = voteIdByNaturalKey("119-Senate-1-648") should be > 0L
    val _ = voteIdByNaturalKey("119-Senate-1-632") should be > 0L

    // Assert — positions populated for each vote (House has ~430 members, Senate has ~100 including pairings)
    val _ = countPositions(voteIdByNaturalKey("119-House-1-240")) should be > 400L
    val _ = countPositions(voteIdByNaturalKey("119-Senate-1-648")) should be > 90L

    // Assert — `stance_materialization_status.has_votes` set for every bill-linked vote
    val _ = countStanceStatus() should be >= 4L

    // Assert — four events published, all with `isUpdate = false`
    val events = pullAllEvents()
    val _      = events.size shouldBe 4
    val _      = events.forall(_.isUpdate == false) shouldBe true

    // Assert — each event carries a `billNaturalKey` (all four votes are bill-linked in this scenario)
    val billKeys = events.flatMap(_.billNaturalKey).toSet
    billKeys should contain allOf ("119-HR-3424", "119-SJRES-18", "119-S-1071", "119-HJRES-131")
  }

}
