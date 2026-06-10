package repcheck.ingestion.amendments.textcheck.integration

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import io.circe.parser._

import org.http4s.ember.client.EmberClientBuilder

import doobie.implicits._
import doobie.postgres.implicits._

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.persistence.DoobieAmendmentRepository
import repcheck.ingestion.amendments.testing.TransactorFixture
import repcheck.ingestion.amendments.textcheck.api.AmendmentTextApiClient
import repcheck.ingestion.amendments.textcheck.config.AmendmentTextCheckerConfig
import repcheck.ingestion.amendments.textcheck.events.DefaultAmendmentTextEventPublisher
import repcheck.ingestion.amendments.textcheck.persistence.DoobieAmendmentTextVersionLookup
import repcheck.ingestion.amendments.textcheck.pipeline.AmendmentTextAvailabilityChecker
import repcheck.ingestion.bills.common.testing.{DockerRequired, PubSubEmulatorFixture}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.GooglePubSubEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.amendment.AmendmentDO

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * Integration spec — exercises the full path against AlloyDB Omni (Docker) + Pub/Sub emulator + WireMock for
 * Congress.gov. Tagged `DockerRequired`; excluded from default `sbt test` and run via the `dockerTest` alias.
 */
class AmendmentTextCheckerIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with TransactorFixture
    with PubSubEmulatorFixture
    with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private val amendmentRepo     = new DoobieAmendmentRepository
  private val textVersionLookup = new DoobieAmendmentTextVersionLookup

  private val testRetryConfig =
    RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0)

  private val checkerConfig = AmendmentTextCheckerConfig(
    minCongress = 117,
    staleAfter = 1.second,
    parallelism = 1,
    eventPublishRetry = testRetryConfig,
  )

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private val testLogger = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    try httpShutdown.unsafeRunSync()
    catch { case _: Exception => () }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    drainMessages()
  }

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  private def buildChecker(): AmendmentTextAvailabilityChecker[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val apiClient       = new AmendmentTextApiClient[IO](congressConfig, httpClient, retryWrapper)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher = new DefaultAmendmentTextEventPublisher[IO](
      publisher = pubsubPublisher,
      topicName = topicName.toString,
      source = "integration-test",
      retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
      retryConfig = testRetryConfig,
    )

    new AmendmentTextAvailabilityChecker[IO](
      apiClient = apiClient,
      amendmentRepo = amendmentRepo,
      textVersionLookup = textVersionLookup,
      eventPublisher = eventPublisher,
      xa = xa,
      config = checkerConfig,
      logger = testLogger,
    )
  }

  private def seedAmendment(naturalKey: String, congress: Int, amendmentType: AmendmentType, number: String): Long = {
    val a = AmendmentDO(
      amendmentId = 0L,
      naturalKey = naturalKey,
      congress = congress,
      amendmentType = Some(amendmentType),
      number = number,
      billId = None,
      chamber = if (amendmentType == AmendmentType.HAMDT) Chamber.House else Chamber.Senate,
      description = None,
      purpose = None,
      sponsorMemberId = None,
      sponsorCommitteeId = None,
      sponsorType = None,
      submittedDate = None,
      proposedDate = None,
      latestActionDate = None,
      latestActionTime = None,
      latestActionText = None,
      updateDate = None,
      apiUrl = None,
      parentAmendmentId = None,
      lastTextCheckAt = None,
      createdAt = None,
      updatedAt = None,
    )
    amendmentRepo.upsert(a).transact(xa).unsafeRunSync().amendmentId
  }

  private def stubTextEndpoint(congress: Int, typePath: String, number: String, body: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/v3/amendment/$congress/$typePath/$number/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )
  }

  private val submittedHtmlBody: String =
    """{
      |  "textVersions": [
      |    { "type": "Submitted", "date": "2024-04-01T12:00:00Z",
      |      "formats": [{ "type": "HTML", "url": "https://www.congress.gov/sub.htm" }] }
      |  ],
      |  "pagination": { "count": 1 }
      |}""".stripMargin

  private def lastTextCheckAt(amendmentId: Long): Option[java.time.Instant] =
    sql"""SELECT last_text_check_at FROM amendments WHERE id = $amendmentId"""
      .query[Option[java.time.Instant]]
      .unique
      .transact(xa)
      .unsafeRunSync()

  // --- Tests ---

  "checker" should "publish AmendmentTextAvailableEvent and stamp last_text_check_at" taggedAs DockerRequired in {
    val id = seedAmendment("117-SAMDT-2137", 117, AmendmentType.SAMDT, "2137")
    stubTextEndpoint(117, "samdt", "2137", submittedHtmlBody)

    val results = buildChecker().checkAll(0L).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    val _       = results.headOption.exists(_.isSucceeded) shouldBe true

    // Give the publisher a moment to flush.
    val _ = TimeUnit.MILLISECONDS.sleep(200L)

    val messages = pullMessages(10)
    val _        = messages.size shouldBe 1
    val payload  = messages.headOption.map(_.getData.toStringUtf8).getOrElse(fail("no message"))
    val _        = payload should include("117-SAMDT-2137")
    val _        = payload should include("SUB")
    val _        = payload should include("HTML")

    // Cross-check the structured payload field.
    val parsed = parse(payload).toOption.flatMap(_.hcursor.downField("payload").as[io.circe.Json].toOption)
    val _      = parsed.flatMap(_.hcursor.downField("versionTypeCode").as[String].toOption) shouldBe Some("SUB")

    lastTextCheckAt(id).isDefined shouldBe true
  }

  it should "leave last_text_check_at unchanged when the API returns 5xx" taggedAs DockerRequired in {
    val id = seedAmendment("117-SAMDT-9999", 117, AmendmentType.SAMDT, "9999")
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/9999/text"))
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
    )

    val results = buildChecker().checkAll(0L).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    val _       = results.headOption.exists(_.isFailed) shouldBe true

    lastTextCheckAt(id) shouldBe None
  }

  it should "stamp last_text_check_at on Skipped (no upstream text)" taggedAs DockerRequired in {
    val id = seedAmendment("117-HAMDT-1", 117, AmendmentType.HAMDT, "1")
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/hamdt/1/text"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val results = buildChecker().checkAll(0L).compile.toList.unsafeRunSync()
    val _       = results.size shouldBe 1
    val _       = results.headOption.exists(_.isSkipped) shouldBe true
    lastTextCheckAt(id).isDefined shouldBe true
  }

  it should "exclude pre-117 amendments from the candidate query" taggedAs DockerRequired in {
    val _       = seedAmendment("116-SAMDT-1", 116, AmendmentType.SAMDT, "1")
    val results = buildChecker().checkAll(0L).compile.toList.unsafeRunSync()
    val _       = results shouldBe empty
    // No /text request is even made for 116 because the SQL pre-filter excludes it.
    wireMock.verify(0, getRequestedFor(urlPathEqualTo("/v3/amendment/116/samdt/1/text")))
  }

  it should "exclude SUAMDT from the candidate query" taggedAs DockerRequired in {
    val _       = seedAmendment("117-SUAMDT-1", 117, AmendmentType.SUAMDT, "1")
    val results = buildChecker().checkAll(0L).compile.toList.unsafeRunSync()
    results shouldBe empty
  }

}
