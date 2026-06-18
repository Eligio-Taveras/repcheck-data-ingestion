package repcheck.ingestion.bills.metadata.api

import java.time.Instant

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.bills.metadata.errors.BillFetchFailed
import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class BillsApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()
    ._1

  private lazy val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  private val noOpLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def makeClient(
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 10L)
  ): BillsApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    BillsApiClient[IO](config, httpClient, retryWrapper, noOpLogger)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  private def billListJson(bills: List[String], totalCount: Int = -1): String = {
    // `pagination.count` in Congress.gov's API is the TOTAL across all pages (used by the
    // paginated client to decide when offset+pageSize has reached the end). For tests that stub
    // a single page, the default totalCount = bills.size is correct. For tests that stub multiple
    // pages, callers must pass `totalCount` set to the sum across all pages so the client doesn't
    // terminate prematurely after page 1.
    val items = bills.mkString(",")
    val count = if (totalCount >= 0) totalCount else bills.size
    s"""{"items": [$items], "pagination": {"count": $count}}"""
  }

  private def singleBillJson(
    congress: Int,
    billType: String,
    number: String,
    title: String,
  ): String =
    s"""{
       |  "congress": $congress,
       |  "number": "$number",
       |  "billType": "$billType",
       |  "title": "$title",
       |  "url": "https://api.congress.gov/v3/bill/$congress/$billType/$number",
       |  "updateDate": "2024-01-01T00:00:00Z"
       |}""".stripMargin

  "fetchPage" should "return decoded bill list items" in {
    val billJson = singleBillJson(118, "hr", "1234", "Test Bill")
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List(billJson)))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()

    val _ = result.items.size shouldBe 1
    val _ = result.items.headOption.map(_.congress) shouldBe Some(118)
    result.items.headOption.map(_.title) shouldBe Some("Test Bill")
  }

  it should "pass fromDateTime and toDateTime as ISO query params" in {
    val billJson = singleBillJson(118, "hr", "100", "Date Test Bill")
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("fromDateTime", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("toDateTime", equalTo("2024-06-01T00:00:00Z"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List(billJson)))
        )
    )

    val client = makeClient()
    val params = FetchParams(
      fromDateTime = Some(Instant.parse("2024-01-01T00:00:00Z")),
      toDateTime = Some(Instant.parse("2024-06-01T00:00:00Z")),
      pageSize = 250,
    )
    val result = client.fetchPage(params).unsafeRunSync()
    result.items.size shouldBe 1
  }

  it should "include api_key on every request" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List.empty))
        )
    )

    val client = makeClient()
    val _      = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/bill")).withQueryParam("api_key", equalTo("test-api-key")))
  }

  it should "pass congress as a query param when FetchParams.congress is set" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("congress", equalTo("119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List.empty))
        )
    )
    val client = makeClient()
    val _      = client.fetchPage(FetchParams(congress = Some(119), pageSize = 250)).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/bill")).withQueryParam("congress", equalTo("119")))
  }

  it should "pass only fromDateTime when toDateTime is None" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("fromDateTime", equalTo("2024-01-01T00:00:00Z"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List.empty))
        )
    )
    val client = makeClient()
    val _ = client
      .fetchPage(
        FetchParams(fromDateTime = Some(Instant.parse("2024-01-01T00:00:00Z")), pageSize = 250)
      )
      .unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/bill")).withQueryParam("fromDateTime", equalTo("2024-01-01T00:00:00Z"))
    )
  }

  "fetchAll" should "paginate across multiple pages" in {
    val page1Bills = (1 to 250).map(i => singleBillJson(118, "hr", i.toString, s"Bill $i")).toList
    val page2Bills = (251 to 300).map(i => singleBillJson(118, "hr", i.toString, s"Bill $i")).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(page1Bills, totalCount = 300))
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("offset", equalTo("250"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(page2Bills, totalCount = 300))
        )
    )

    val client = makeClient()
    val result = client.fetchAll(FetchParams(pageSize = 250)).compile.toList.unsafeRunSync()
    result.size shouldBe 300
  }

  it should "stop when page has fewer items than pageSize" in {
    val smallPage =
      (1 to 50).map(i => singleBillJson(118, "hr", i.toString, s"Bill $i")).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(smallPage))
        )
    )

    val client = makeClient()
    val result = client.fetchAll(FetchParams(pageSize = 250)).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 50
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v3/bill")))
  }

  "fetchDetail" should "decode full bill detail" in {
    val detailJson =
      s"""{
         |  "bill": {
         |    "congress": 118,
         |    "number": "1234",
         |    "billType": "hr",
         |    "title": "Detailed Test Bill",
         |    "url": "https://api.congress.gov/v3/bill/118/hr/1234",
         |    "updateDate": "2024-01-01T00:00:00Z",
         |    "sponsors": [{"bioguideId": "S001234", "fullName": "Sen. Test"}],
         |    "subjects": {
         |      "legislativeSubjects": [{"name": "Health"}],
         |      "policyArea": "Health"
         |    }
         |  }
         |}""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson)
        )
    )

    val client = makeClient()
    val detail = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/1234")
      .unsafeRunSync()

    val _ = detail.congress shouldBe 118
    val _ = detail.title shouldBe "Detailed Test Bill"
    detail.sponsors.map(_.size) shouldBe Some(1)
  }

  it should "append api_key to detail URL" in {
    val detailJson =
      s"""{
         |  "bill": {
         |    "congress": 118, "number": "1", "billType": "hr",
         |    "title": "T", "url": "http://x", "updateDate": "2024-01-01"
         |  }
         |}""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson)
        )
    )

    val client = makeClient()
    val _ = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/1")
      .unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/bill/118/hr/1"))
        .withQueryParam("api_key", equalTo("test-api-key"))
    )
  }

  "retry behavior" should "retry on HTTP 429" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .inScenario("429-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withBody("Rate limited"))
        .willSetStateTo("retried")
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .inScenario("429-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    result.items shouldBe empty
  }

  it should "retry on HTTP 500" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .inScenario("500-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
        .willSetStateTo("retried")
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .inScenario("500-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(billListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    result.items shouldBe empty
  }

  it should "fail immediately on HTTP 403" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )

    val client = makeClient()
    val ex = intercept[BillFetchFailed] {
      client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    }
    ex.getMessage should include("Forbidden")
  }

  it should "raise descriptive error on malformed JSON" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{not valid json}")
        )
    )

    val client = makeClient()
    val _ = intercept[Exception] {
      client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    }
  }

  private def cosponsorJson(bioguideId: String, isOriginal: Boolean): String =
    s"""{
       |  "bioguideId": "$bioguideId",
       |  "fullName": "Rep. Test $bioguideId",
       |  "isOriginalCosponsor": $isOriginal,
       |  "sponsorshipDate": "2024-01-15"
       |}""".stripMargin

  private def cosponsorListJson(cosponsors: List[String], nextUrl: Option[String] = None): String = {
    val items = cosponsors.mkString(",")
    val paginationPart = nextUrl match {
      case Some(url) => s""", "pagination": {"count": ${cosponsors.size}, "next": "$url"}"""
      case None      => s""", "pagination": {"count": ${cosponsors.size}}"""
    }
    s"""{"cosponsors": [$items]$paginationPart}"""
  }

  "fetchCosponsors" should "return a single page of cosponsors" in {
    val c1 = cosponsorJson("A000001", isOriginal = true)
    val c2 = cosponsorJson("B000002", isOriginal = false)
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1/cosponsors"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(cosponsorListJson(List(c1, c2)))
        )
    )

    val client = makeClient()
    val result = client
      .fetchCosponsors(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/1/cosponsors")
      .unsafeRunSync()

    val _ = result.size shouldBe 2
    result.headOption.map(_.bioguideId) shouldBe Some("A000001")
  }

  it should "paginate across multiple pages of cosponsors" in {
    val page1 = (1 to 250).map(i => cosponsorJson(s"M${"%06d".format(i)}", isOriginal = true)).toList
    val page2 = (251 to 260).map(i => cosponsorJson(s"M${"%06d".format(i)}", isOriginal = false)).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/2/cosponsors"))
        .withQueryParam("offset", absent())
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              cosponsorListJson(
                page1,
                Some(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/2/cosponsors?offset=250"),
              )
            )
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/2/cosponsors"))
        .withQueryParam("offset", equalTo("250"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(cosponsorListJson(page2))
        )
    )

    val client = makeClient()
    val result = client
      .fetchCosponsors(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/2/cosponsors")
      .unsafeRunSync()

    result.size shouldBe 260
  }

  it should "return empty list for bill with no cosponsors" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/3/cosponsors"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(cosponsorListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client
      .fetchCosponsors(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/3/cosponsors")
      .unsafeRunSync()

    result shouldBe empty
  }

  it should "fail on HTTP 429 after retries" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/4/cosponsors"))
        .willReturn(aResponse().withStatus(429).withBody("Rate limited"))
    )

    val client = makeClient(RetryConfig(maxRetries = 1, initialBackoffMs = 10L))
    val _ = intercept[BillFetchFailed] {
      client
        .fetchCosponsors(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/4/cosponsors")
        .unsafeRunSync()
    }
  }

  it should "fail on HTTP 500 after retries" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/5/cosponsors"))
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
    )

    val client = makeClient(RetryConfig(maxRetries = 1, initialBackoffMs = 10L))
    val _ = intercept[BillFetchFailed] {
      client
        .fetchCosponsors(s"http://localhost:${wireMock.port()}/v3/bill/118/hr/5/cosponsors")
        .unsafeRunSync()
    }
  }

  private def subjectJson(name: String): String =
    s"""{"name": "$name", "updateDate": "2017-02-14T22:56:36Z"}"""

  // The real /subjects response nests policyArea as an OBJECT — included here to prove the decoder ignores it.
  private def subjectsListJson(subjects: List[String], nextUrl: Option[String] = None): String = {
    val items = subjects.mkString(",")
    val paginationPart = nextUrl match {
      case Some(url) => s""", "pagination": {"count": ${subjects.size}, "next": "$url"}"""
      case None      => s""", "pagination": {"count": ${subjects.size}}"""
    }
    s"""{"subjects": {"legislativeSubjects": [$items], "policyArea": {"name": "Health"}}$paginationPart}"""
  }

  "fetchSubjects" should "return a single page of legislative subjects (ignoring the policyArea object)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/103/hr/4593/subjects"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(subjectsListJson(List(subjectJson("Budget deficits"), subjectJson("Taxation"))))
        )
    )

    val result = makeClient()
      .fetchSubjects(103, "hr", "4593")
      .unsafeRunSync()

    result.map(_.name) shouldBe List("Budget deficits", "Taxation")
  }

  it should "paginate across multiple pages of subjects" in {
    val page1 = (1 to 250).map(i => subjectJson(s"Subject $i")).toList
    val page2 = (251 to 260).map(i => subjectJson(s"Subject $i")).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/107/s/1628/subjects"))
        .withQueryParam("offset", absent())
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              subjectsListJson(
                page1,
                Some(s"http://localhost:${wireMock.port()}/v3/bill/107/s/1628/subjects?offset=250"),
              )
            )
        )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/107/s/1628/subjects"))
        .withQueryParam("offset", equalTo("250"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(subjectsListJson(page2))
        )
    )

    val result = makeClient()
      .fetchSubjects(107, "s", "1628")
      .unsafeRunSync()

    result.size shouldBe 260
  }

  it should "return an empty list for a bill with no subjects" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/94/hr/10792/subjects"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(subjectsListJson(List.empty))
        )
    )

    val result = makeClient()
      .fetchSubjects(94, "hr", "10792")
      .unsafeRunSync()

    result shouldBe empty
  }

  it should "fail on HTTP 500 after retries" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/9/subjects"))
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
    )

    val client = makeClient(RetryConfig(maxRetries = 1, initialBackoffMs = 10L))
    val _ = intercept[BillFetchFailed] {
      client
        .fetchSubjects(118, "hr", "9")
        .unsafeRunSync()
    }
  }

}
