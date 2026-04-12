package com.repcheck.bills.textcheck.integration

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import io.circe.parser._

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.events.{DefaultIngestionEventPublisher, GooglePubSubEventPublisher}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.bill.BillDO

import com.repcheck.bills.common.persistence.DoobieBillRepository
import com.repcheck.bills.common.testing.{DockerRequired, PubSubEmulatorFixture, TransactorFixture}
import com.repcheck.bills.textcheck.api.BillTextApiClient
import com.repcheck.bills.textcheck.config.BillTextCheckerConfig
import com.repcheck.bills.textcheck.pipeline.BillTextAvailabilityChecker

/**
 * Integration tests for the Bill Text Availability Checker. Uses real AlloyDB Omni (Docker), Pub/Sub emulator, and
 * WireMock for Congress.gov API. Validates the flow: bills in DB → checker queries → fetches text versions → emits
 * events.
 */
class MetadataToCheckerIntegrationSpec
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

  private val billRepo = new DoobieBillRepository()

  private val testRetryConfig =
    RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 10L, backoffMultiplier = 1.0)

  private val checkerConfig = BillTextCheckerConfig(parallelism = 1, eventPublishRetry = testRetryConfig)

  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()
    ._1

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

  private def buildChecker(): BillTextAvailabilityChecker[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    val congressConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = testRetryConfig,
    )
    val textApiClient   = new BillTextApiClient[IO](congressConfig, httpClient, retryWrapper)
    val pubsubPublisher = new GooglePubSubEventPublisher[IO](publisher)
    val eventPublisher =
      new DefaultIngestionEventPublisher[IO](pubsubPublisher, topicName.toString, "integration-test")

    new BillTextAvailabilityChecker[IO](
      textApiClient = textApiClient,
      billRepo = billRepo,
      eventPublisher = eventPublisher,
      retryWrapper = retryWrapper,
      xa = xa,
      config = checkerConfig,
      logger = testLogger,
    )
  }

  private def seedBill(
    naturalKey: String,
    congress: Int = 118,
    billType: BillType = BillType.HR,
    number: String,
    textUrl: Option[String] = None,
    textVersionType: Option[TextVersionCode] = None,
  ): Long = {
    val bill = BillDO(
      billId = 0L,
      naturalKey = naturalKey,
      congress = congress,
      billType = billType,
      number = number,
      title = "Test Bill",
      originChamber = Some(Chamber.House),
      originChamberCode = Some("H"),
      introducedDate = None,
      policyArea = None,
      latestActionDate = None,
      latestActionText = None,
      constitutionalAuthorityText = None,
      sponsorMemberId = None,
      textUrl = textUrl,
      textFormat = None,
      textVersionType = textVersionType,
      textDate = None,
      textContent = None,
      textEmbedding = None,
      summaryText = None,
      summaryActionDesc = None,
      summaryActionDate = None,
      updateDate = None,
      updateDateIncludingText = None,
      legislationUrl = None,
      apiUrl = None,
      createdAt = None,
      updatedAt = None,
      latestTextVersionId = None,
    )
    import doobie.implicits._
    billRepo.upsert(bill).transact(xa).unsafeRunSync()
  }

  private def stubTextVersions(congress: Int, billType: String, number: String, json: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/v3/bill/$congress/$billType/$number/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(json)
        )
    )
  }

  private def stubTextVersionsError(congress: Int, billType: String, number: String, status: Int): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(s"/v3/bill/$congress/$billType/$number/text"))
        .willReturn(
          aResponse()
            .withStatus(status)
            .withBody("Internal Server Error")
        )
    )
  }

  private val singleVersionJson: String =
    """{
      |  "textVersions": [
      |    {
      |      "date": "2024-01-15T00:00:00Z",
      |      "type": "IH",
      |      "formats": [
      |        {"type": "Formatted Text", "url": "https://congress.gov/text/ih/formatted"},
      |        {"type": "PDF", "url": "https://congress.gov/text/ih/pdf"}
      |      ]
      |    }
      |  ]
      |}""".stripMargin

  private def textVersionJson(versionType: String, url: String): String =
    s"""{
       |  "textVersions": [
       |    {
       |      "date": "2024-01-15T00:00:00Z",
       |      "type": "$versionType",
       |      "formats": [
       |        {"type": "Formatted Text", "url": "$url"}
       |      ]
       |    }
       |  ]
       |}""".stripMargin

  private val emptyVersionsJson: String =
    """{"textVersions": []}"""

  private def extractPayloadField(messageData: String, field: String): Option[String] =
    parse(messageData).toOption
      .flatMap(_.hcursor.downField("payload").downField(field).as[String].toOption)

  private def extractOptionalPayloadField(messageData: String, field: String): Option[Option[String]] =
    parse(messageData).toOption
      .map(_.hcursor.downField("payload").downField(field).as[String].toOption)

  // --- Tests ---

  "MetadataToChecker integration" should "emit event when bill has no prior text" taggedAs DockerRequired in {
    val _ = seedBill(naturalKey = "118-HR-1", number = "1")
    stubTextVersions(118, "hr", "1", singleVersionJson)

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSucceeded) shouldBe true

    val messages = pullMessages()
    val _        = messages.size shouldBe 1
    val data     = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val _        = extractPayloadField(data, "naturalKey") shouldBe Some("118-HR-1")
    val _        = extractPayloadField(data, "versionCode") shouldBe Some("IH")
    val _        = extractPayloadField(data, "textFormat") shouldBe Some("Formatted Text")
    extractOptionalPayloadField(data, "previousVersionCode") shouldBe Some(None)
  }

  it should "skip bill when text version is unchanged" taggedAs DockerRequired in {
    val _ = seedBill(
      naturalKey = "118-HR-2",
      number = "2",
      textUrl = Some("https://congress.gov/text/ih/formatted"),
      textVersionType = Some(TextVersionCode.IH),
    )
    stubTextVersions(118, "hr", "2", textVersionJson("IH", "https://congress.gov/text/ih/formatted"))

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSkipped) shouldBe true

    val messages = pullMessages()
    messages shouldBe empty
  }

  it should "emit event with previousVersionCode when text version is upgraded" taggedAs DockerRequired in {
    val _ = seedBill(
      naturalKey = "118-HR-3",
      number = "3",
      textUrl = Some("https://congress.gov/text/ih"),
      textVersionType = Some(TextVersionCode.IH),
    )
    stubTextVersions(118, "hr", "3", textVersionJson("RH", "https://congress.gov/text/rh/formatted"))

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSucceeded) shouldBe true

    val messages = pullMessages()
    val _        = messages.size shouldBe 1
    val data     = messages.headOption.map(_.getData.toStringUtf8).getOrElse("")
    val _        = extractPayloadField(data, "naturalKey") shouldBe Some("118-HR-3")
    val _        = extractPayloadField(data, "versionCode") shouldBe Some("RH")
    extractPayloadField(data, "previousVersionCode") shouldBe Some("IH")
  }

  it should "process multiple bills and emit correct number of events" taggedAs DockerRequired in {
    val _ = seedBill(naturalKey = "118-HR-10", number = "10")
    val _ = seedBill(naturalKey = "118-HR-11", number = "11")
    val _ = seedBill(naturalKey = "118-HR-12", number = "12")

    stubTextVersions(118, "hr", "10", textVersionJson("IH", "https://congress.gov/text/10"))
    stubTextVersions(118, "hr", "11", textVersionJson("IS", "https://congress.gov/text/11"))
    stubTextVersions(118, "hr", "12", textVersionJson("RH", "https://congress.gov/text/12"))

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 3
    val _ = results.count(_.isSucceeded) shouldBe 3

    val messages    = pullMessages()
    val _           = messages.size shouldBe 3
    val naturalKeys = messages.map(m => extractPayloadField(m.getData.toStringUtf8, "naturalKey"))
    val _           = naturalKeys should contain(Some("118-HR-10"))
    val _           = naturalKeys should contain(Some("118-HR-11"))
    naturalKeys should contain(Some("118-HR-12"))
  }

  it should "skip bill when API returns empty text versions" taggedAs DockerRequired in {
    val _ = seedBill(naturalKey = "118-HR-20", number = "20")
    stubTextVersions(118, "hr", "20", emptyVersionsJson)

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSkipped) shouldBe true

    val messages = pullMessages()
    messages shouldBe empty
  }

  it should "continue processing other bills when API fails for one bill" taggedAs DockerRequired in {
    val _ = seedBill(naturalKey = "118-HR-30", number = "30")
    val _ = seedBill(naturalKey = "118-HR-31", number = "31")

    stubTextVersionsError(118, "hr", "30", 500)
    stubTextVersions(118, "hr", "31", textVersionJson("IH", "https://congress.gov/text/31"))

    val checker = buildChecker()
    val results = checker.checkAll(UUID.randomUUID()).compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 2
    val _ = results.count(_.isFailed) shouldBe 1
    val _ = results.count(_.isSucceeded) shouldBe 1

    val messages = pullMessages()
    val _        = messages.size shouldBe 1
    extractPayloadField(messages.headOption.map(_.getData.toStringUtf8).getOrElse(""), "naturalKey") shouldBe Some(
      "118-HR-31"
    )
  }

}
