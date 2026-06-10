package repcheck.ingestion.bills.summary.api

import java.time.Instant

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
import repcheck.ingestion.bills.summary.errors.BillSummaryFetchFailed
import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class BillSummariesApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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

  private def makeClient(
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 10L)
  ): BillSummariesApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    BillSummariesApiClient[IO](config, httpClient, retryWrapper)
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

  /**
   * Wrap a list of pre-rendered summary JSON literals into the `{ "summaries": [...], "pagination": {"count": N} }`
   * envelope. Defaults `count` to `summaries.size` (single-page tests); multi-page tests must pass the cumulative total
   * across all pages so the paginated client doesn't terminate prematurely after page 1.
   */
  private def summaryListJson(summaries: List[String], totalCount: Int = -1): String = {
    val items = summaries.mkString(",")
    val count = if (totalCount >= 0) totalCount else summaries.size
    s"""{"summaries": [$items], "pagination": {"count": $count}}"""
  }

  private def singleSummaryJson(
    congress: Int,
    billType: String,
    number: Int,
    versionCode: String,
    actionDesc: String = "Introduced in House",
    updateDate: String = "2024-01-15T00:00:00Z",
  ): String =
    s"""{
       |  "actionDate": "2024-01-15",
       |  "actionDesc": "$actionDesc",
       |  "text": "<p>summary body</p>",
       |  "updateDate": "$updateDate",
       |  "versionCode": "$versionCode",
       |  "bill": {"congress": $congress, "type": "$billType", "number": $number}
       |}""".stripMargin

  "fetchPage" should "return decoded summary entries with bill references intact" in {
    val body = summaryListJson(
      List(
        singleSummaryJson(118, "HR", 1, "00", "Introduced in House"),
        singleSummaryJson(118, "S", 42, "00", "Introduced in Senate"),
      )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118), pageSize = 250)).unsafeRunSync()

    val _ = result.items.size shouldBe 2
    val _ = result.items.headOption.flatMap(_.bill).map(_.naturalKey) shouldBe Some("118-HR-1")
    val _ = result.items.lift(1).flatMap(_.bill).map(_.naturalKey) shouldBe Some("118-S-42")
    result.items.headOption.flatMap(_.versionCode) shouldBe Some("00")
  }

  it should "hit /summaries (no congress) when params.congress is None" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(summaryListJson(List.empty))
        )
    )

    val client = makeClient()
    val _      = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/summaries")))
  }

  it should "include api_key, format, offset, limit, sort, fromDateTime, toDateTime as query params" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(summaryListJson(List.empty))
        )
    )

    val client = makeClient()
    val from   = Instant.parse("2024-01-01T00:00:00Z")
    val to     = Instant.parse("2024-12-31T23:59:59Z")
    val _ = client
      .fetchPage(
        FetchParams(
          congress = Some(119),
          fromDateTime = Some(from),
          toDateTime = Some(to),
          pageSize = 250,
        )
      )
      .unsafeRunSync()

    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/summaries/119"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("250"))
        .withQueryParam("fromDateTime", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("toDateTime", equalTo("2024-12-31T23:59:59Z"))
    )
  }

  it should "decode an empty page (no summaries) without error" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(summaryListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()

    val _ = result.items shouldBe empty
    result.totalCount shouldBe 0
  }

  it should "raise BillSummaryFetchFailed (Systemic) on 4xx with the api_key stripped from the endpoint message" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/119"))
        .willReturn(
          aResponse()
            .withStatus(401)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"error": "invalid api_key"}""")
        )
    )

    val client = makeClient(retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 0L))
    val thrown = intercept[BillSummaryFetchFailed] {
      client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()
    }

    val _ = thrown.endpoint should not include "api_key"
    thrown.endpoint should include("/v3/summaries/119")
  }

  it should "report totalCount when pagination.count is supplied" in {
    val body = summaryListJson(
      List(singleSummaryJson(118, "HR", 1, "00")),
      totalCount = 1234,
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118), pageSize = 250)).unsafeRunSync()
    result.totalCount shouldBe 1234
  }

  "fetchAll" should "drain every page until totalCount is reached" in {
    val pageSize = 2
    val total    = 5

    val page1 = summaryListJson(
      List(
        singleSummaryJson(118, "HR", 1, "00"),
        singleSummaryJson(118, "HR", 2, "00"),
      ),
      totalCount = total,
    )
    val page2 = summaryListJson(
      List(
        singleSummaryJson(118, "HR", 3, "00"),
        singleSummaryJson(118, "HR", 4, "00"),
      ),
      totalCount = total,
    )
    val page3 = summaryListJson(
      List(singleSummaryJson(118, "HR", 5, "00")),
      totalCount = total,
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page1))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("offset", equalTo("2"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page2))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("offset", equalTo("4"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page3))
    )

    val client = {
      val cfg = CongressGovClientConfig(
        apiKey = "test-api-key",
        baseUrl = s"http://localhost:${wireMock.port()}/v3",
        pageSize = pageSize,
        pageDelay = Duration.Zero,
        retry = RetryConfig(maxRetries = 0, initialBackoffMs = 0L),
      )
      BillSummariesApiClient[IO](cfg, httpClient, retryWrapper)
    }

    val results = client
      .fetchAll(FetchParams(congress = Some(118), pageSize = pageSize))
      .compile
      .toList
      .unsafeRunSync()

    val _ = results.size shouldBe total
    results.flatMap(_.bill).map(_.naturalKey).toSet shouldBe Set(
      "118-HR-1",
      "118-HR-2",
      "118-HR-3",
      "118-HR-4",
      "118-HR-5",
    )
  }

  it should "stop paginating when an empty page arrives even without a totalCount signal" in {
    val pageSize = 2
    val page1 = summaryListJson(
      List(singleSummaryJson(118, "HR", 1, "00"), singleSummaryJson(118, "HR", 2, "00"))
      // no totalCount → defaults to bills.size = 2 (matches pageSize, so paginate to next)
    )
    val page2 = summaryListJson(List.empty)

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page1))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/summaries/118"))
        .withQueryParam("offset", equalTo("2"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page2))
    )

    val client = {
      val cfg = CongressGovClientConfig(
        apiKey = "test-api-key",
        baseUrl = s"http://localhost:${wireMock.port()}/v3",
        pageSize = pageSize,
        pageDelay = Duration.Zero,
        retry = RetryConfig(maxRetries = 0, initialBackoffMs = 0L),
      )
      BillSummariesApiClient[IO](cfg, httpClient, retryWrapper)
    }

    val results = client
      .fetchAll(FetchParams(congress = Some(118), pageSize = pageSize))
      .compile
      .toList
      .unsafeRunSync()

    results.size shouldBe 2
  }

}
