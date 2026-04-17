package repcheck.members.lismapping.client

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.pipeline.models.errors.RetryConfig

class SenatorLookupXmlClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  implicit private val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private val retryConfig: RetryConfig = RetryConfig(
    maxRetries = 1,
    initialBackoffMs = 10L,
    maxBackoffMs = 50L,
    backoffMultiplier = 2.0,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def baseConfig(
    currentCongress: Int = 118,
    congressLookbackWindow: Int = 5,
  ): LisMappingConfig =
    LisMappingConfig(
      parallelism = 1,
      requestTimeout = 5.seconds,
      currentCongress = currentCongress,
      congressLookbackWindow = congressLookbackWindow,
      senatorXmlUrl = s"http://localhost:${wireMock.port().toString}/senators.xml",
    )

  private def makeClient(config: LisMappingConfig): SenatorLookupXmlClient[IO] = {
    val xmlFeed = XmlFeedClient.make[IO](httpClient, retryConfig)
    new SenatorLookupXmlClient[IO](xmlFeed, config, noopLogger)
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

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  private def stubXml(path: String, body: String, status: Int = 200): Unit = {
    val _ = wireMock.stubFor(
      get(urlEqualTo(path))
        .willReturn(
          aResponse()
            .withStatus(status)
            .withHeader("Content-Type", "application/xml")
            .withBody(body)
        )
    )
  }

  private def member(
    lisId: String,
    bioguide: String,
    congresses: Seq[Int],
    isCurrent: Boolean,
  ): String = {
    val services = congresses
      .map(c => s"<service><congress>${c.toString}</congress></service>")
      .mkString
    s"""<member>
       |  <lis_member_id>$lisId</lis_member_id>
       |  <bioguide_id>$bioguide</bioguide_id>
       |  <first_name>First</first_name>
       |  <last_name>Last</last_name>
       |  <party>D</party>
       |  <state>NY</state>
       |  <is_current>${isCurrent.toString}</is_current>
       |  <service_dates>$services</service_dates>
       |</member>""".stripMargin
  }

  private def document(members: Seq[String]): String =
    s"""<contact_information>${members.mkString}</contact_information>"""

  "fetchMappings" should "emit one DTO per parsed senator on the happy path" in {
    val body = document(
      Seq(
        member("S1", "B1", Seq(118), isCurrent = true),
        member("S2", "B2", Seq(118), isCurrent = true),
        member("S3", "B3", Seq(117, 118), isCurrent = true),
      )
    )
    stubXml("/senators.xml", body)

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 3
    result.map(_.lisId) should contain theSameElementsAs List("S1", "S2", "S3")
  }

  it should "filter out senators whose service entirely predates the lookback window" in {
    val body = document(
      Seq(
        member("S-current-1", "B-c1", Seq(118), isCurrent = true),
        member("S-current-2", "B-c2", Seq(118), isCurrent = true),
        member("S-old-1", "B-o1", Seq(110), isCurrent = false),
        member("S-old-2", "B-o2", Seq(100, 105), isCurrent = false),
      )
    )
    stubXml("/senators.xml", body)

    // Window: currentCongress=118, lookback=5 → [114, 118]
    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 2
    result.map(_.lisId) should contain theSameElementsAs List("S-current-1", "S-current-2")
  }

  it should "include a senator with isCurrent=true even when service dates are all out of window" in {
    val body = document(
      Seq(
        member("S-appointed", "B-a1", Seq(110), isCurrent = true)
      )
    )
    stubXml("/senators.xml", body)

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S-appointed")
  }

  it should "include a senator whose service spans the window boundary" in {
    val body = document(
      Seq(
        // congress 114 is the start of the window [114, 118] with lookback=5
        member("S-boundary", "B-b1", Seq(112, 114), isCurrent = false)
      )
    )
    stubXml("/senators.xml", body)

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S-boundary")
  }

  it should "fail the stream when the feed returns HTTP 404" in {
    stubXml("/senators.xml", "<contact_information></contact_information>", status = 404)

    val thrown = intercept[Exception] {
      makeClient(baseConfig()).fetchMappings(1L).compile.drain.unsafeRunSync()
    }
    thrown.toString should not be empty
  }

  it should "emit an empty stream when the feed has no <member> entries" in {
    stubXml("/senators.xml", "<contact_information></contact_information>")

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

  it should "fail the stream when the feed body is malformed XML" in {
    stubXml("/senators.xml", "this is not xml at all {{{")

    val thrown = intercept[Exception] {
      makeClient(baseConfig()).fetchMappings(1L).compile.drain.unsafeRunSync()
    }
    thrown.toString should not be empty
  }

  it should "apply the window boundary exclusively to congresses older than (current - lookback + 1)" in {
    val body = document(
      Seq(
        // Window: [118 - 5 + 1, 118] = [114, 118]. Congress 113 must be excluded.
        member("S-edge-out", "B-eo", Seq(113), isCurrent = false),
        member("S-edge-in", "B-ei", Seq(114), isCurrent = false),
      )
    )
    stubXml("/senators.xml", body)

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S-edge-in")
  }

  it should "skip malformed member entries but keep valid ones in the same feed" in {
    val validMember = member("S-good", "B-good", Seq(118), isCurrent = true)
    val brokenMember =
      """<member>
        |  <lis_member_id>S-broken</lis_member_id>
        |  <!-- missing bioguide_id - required field -->
        |  <first_name>Broken</first_name>
        |  <last_name>Entry</last_name>
        |  <party>D</party>
        |  <state>NY</state>
        |  <is_current>true</is_current>
        |</member>""".stripMargin

    stubXml("/senators.xml", document(Seq(validMember, brokenMember)))

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S-good")
  }

  it should "drop a senator with no congress service dates and isCurrent=false" in {
    val body = document(
      Seq(
        member("S-empty-dates", "B-ed", Seq.empty, isCurrent = false)
      )
    )
    stubXml("/senators.xml", body)

    val result = makeClient(baseConfig()).fetchMappings(1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

}
