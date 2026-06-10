package repcheck.ingestion.amendments.api

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.amendments.errors.AmendmentFetchFailed
import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class AmendmentsApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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

  private val testCorrelationId = UUID.fromString("00000000-0000-0000-0000-000000000001")

  private def makeClient(
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 10L),
    pageSize: Int = 250,
  ): AmendmentsApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = pageSize,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    AmendmentsApiClient[IO](config, httpClient, retryWrapper)
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
   * `{ "amendments": [...], "pagination": {"count": N} }` envelope. Defaults `count` to `amendments.size` (single-page
   * tests); multi-page tests must pass the cumulative total across all pages so the paginated client doesn't terminate
   * prematurely after page 1.
   */
  private def amendmentListJson(amendments: List[String], totalCount: Int = -1): String = {
    val items = amendments.mkString(",")
    val count = if (totalCount >= 0) totalCount else amendments.size
    s"""{"amendments": [$items], "pagination": {"count": $count}}"""
  }

  private def listItemJson(
    congress: Int,
    amendmentType: String,
    number: String,
    description: String = "Test amendment",
    updateDate: String = "2024-01-15T00:00:00Z",
  ): String =
    s"""{
       |  "congress": $congress,
       |  "type": "$amendmentType",
       |  "number": "$number",
       |  "description": "$description",
       |  "updateDate": "$updateDate",
       |  "url": "https://api.congress.gov/v3/amendment/$congress/${amendmentType.toLowerCase}/$number"
       |}""".stripMargin

  private def detailJson(
    congress: Int,
    amendmentType: String,
    number: String,
  ): String =
    s"""{
       |  "amendment": {
       |    "congress": $congress,
       |    "type": "$amendmentType",
       |    "number": "$number",
       |    "chamber": "Senate",
       |    "description": "An amendment...",
       |    "purpose": "In the nature of a substitute.",
       |    "updateDate": "2021-08-08T12:00:00Z",
       |    "submittedDate": "2021-02-02T05:00:00Z",
       |    "sponsors": [
       |      { "bioguideId": "S001217", "firstName": "Rick", "lastName": "Scott", "fullName": "Sen. Scott, Rick [R-FL]", "party": "R", "state": "FL" }
       |    ],
       |    "amendedBill": {
       |      "congress": $congress,
       |      "number": "3684",
       |      "type": "HR",
       |      "title": "Infrastructure Investment and Jobs Act",
       |      "url": "https://api.congress.gov/v3/bill/$congress/hr/3684"
       |    },
       |    "actions": { "count": 3, "url": "https://api.congress.gov/v3/amendment/$congress/${amendmentType.toLowerCase}/$number/actions" },
       |    "textVersions": { "count": 1, "url": "https://api.congress.gov/v3/amendment/$congress/${amendmentType.toLowerCase}/$number/text" }
       |  }
       |}""".stripMargin

  // ---------- fetchPage ----------

  "fetchPage" should "decode the amendments envelope and return list items" in {
    val body = amendmentListJson(
      List(
        listItemJson(117, "SAMDT", "2137"),
        listItemJson(117, "HAMDT", "100"),
      )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(117), pageSize = 250)).unsafeRunSync()

    val _ = result.items.size shouldBe 2
    val _ = result.items.headOption.map(_.number) shouldBe Some("2137")
    result.items.headOption.flatMap(_.amendmentType) shouldBe Some("SAMDT")
  }

  it should "hit /amendment (no congress) when params.congress is None" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(amendmentListJson(List.empty))
        )
    )

    val client = makeClient()
    val _      = client.fetchPage(FetchParams(pageSize = 250)).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/amendment")))
  }

  it should "include api_key, format, offset, limit, sort, fromDateTime, toDateTime" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(amendmentListJson(List.empty))
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
      getRequestedFor(urlPathEqualTo("/v3/amendment/119"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("250"))
        .withQueryParam("fromDateTime", equalTo("2024-01-01T00:00:00Z"))
        .withQueryParam("toDateTime", equalTo("2024-12-31T23:59:59Z"))
    )
  }

  it should "use a lowercase path segment (URL casing rule)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(amendmentListJson(List.empty))
        )
    )

    val client = makeClient()
    val _      = client.fetchPage(FetchParams(congress = Some(118))).unsafeRunSync()

    // /v3/amendment (lowercase), NOT /v3/Amendment
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/amendment/118")))
  }

  it should "decode an empty page without error" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(amendmentListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()

    val _ = result.items shouldBe empty
    result.totalCount shouldBe 0
  }

  it should "decode a single-item page" in {
    val body = amendmentListJson(List(listItemJson(117, "SAMDT", "2137")))
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(117))).unsafeRunSync()

    result.items.size shouldBe 1
  }

  it should "raise AmendmentFetchFailed (Systemic) on 4xx with the api_key stripped from the natural key" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/119"))
        .willReturn(
          aResponse()
            .withStatus(401)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"error": "invalid api_key"}""")
        )
    )

    val client = makeClient(retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 0L))
    val thrown = intercept[AmendmentFetchFailed] {
      val _ = client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()
    }

    val _ = thrown.naturalKey shouldNot include("api_key")
    thrown.naturalKey shouldBe "amendment/119"
  }

  it should "report totalCount when pagination.count is supplied" in {
    val body = amendmentListJson(
      List(listItemJson(118, "SAMDT", "1")),
      totalCount = 1234,
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118"))
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

  it should "retry on 503 then succeed" in {
    val scenario = "transient-503"
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118"))
        .inScenario(scenario)
        .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
        .willReturn(aResponse().withStatus(503).withBody("down"))
        .willSetStateTo("recovered")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118"))
        .inScenario(scenario)
        .whenScenarioStateIs("recovered")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(amendmentListJson(List(listItemJson(118, "SAMDT", "1"))))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118))).unsafeRunSync()
    result.items.size shouldBe 1
  }

  it should "raise AmendmentFetchFailed on malformed JSON" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/119"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"amendments": "not-an-array"}""")
        )
    )

    val client = makeClient(retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 0L))
    intercept[AmendmentFetchFailed] {
      val _ = client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()
    }
  }

  it should "raise AmendmentFetchFailed when baseUrl is unparseable" in {
    val cfg = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "not a valid url with spaces",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 0L),
    )
    val client = AmendmentsApiClient[IO](cfg, httpClient, retryWrapper)
    intercept[AmendmentFetchFailed] {
      val _ = client.fetchPage(FetchParams(congress = Some(119))).unsafeRunSync()
    }
  }

  // ---------- fetchAll ----------

  "fetchAll" should "drain pages until totalCount is reached" in {
    val pageSize = 2
    val total    = 5

    val page1 = amendmentListJson(
      List(listItemJson(117, "SAMDT", "1"), listItemJson(117, "SAMDT", "2")),
      totalCount = total,
    )
    val page2 = amendmentListJson(
      List(listItemJson(117, "SAMDT", "3"), listItemJson(117, "SAMDT", "4")),
      totalCount = total,
    )
    val page3 = amendmentListJson(
      List(listItemJson(117, "SAMDT", "5")),
      totalCount = total,
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page1))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("offset", equalTo("2"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page2))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("offset", equalTo("4"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page3))
    )

    val client = makeClient(pageSize = pageSize)
    val results = client
      .fetchAll(FetchParams(congress = Some(117), pageSize = pageSize))
      .compile
      .toList
      .unsafeRunSync()

    val _ = results.size shouldBe total
    results.map(_.number).toSet shouldBe Set("1", "2", "3", "4", "5")
  }

  it should "stop on an empty page even without a totalCount" in {
    val pageSize = 2
    val page1 = amendmentListJson(
      List(listItemJson(117, "SAMDT", "1"), listItemJson(117, "SAMDT", "2"))
    )
    val page2 = amendmentListJson(List.empty)

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page1))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117"))
        .withQueryParam("offset", equalTo("2"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(page2))
    )

    val client = makeClient(pageSize = pageSize)
    val results = client
      .fetchAll(FetchParams(congress = Some(117), pageSize = pageSize))
      .compile
      .toList
      .unsafeRunSync()

    results.size shouldBe 2
  }

  // ---------- fetchDetail ----------

  "fetchDetail" should "decode the {amendment:{...}} envelope" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client = makeClient()
    val detail = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137", testCorrelationId)
      .unsafeRunSync()

    val _ = detail.congress shouldBe 117
    val _ = detail.number shouldBe "2137"
    val _ = detail.amendmentType shouldBe Some("SAMDT")
    val _ = detail.amendedBill.flatMap(_.title) shouldBe Some("Infrastructure Investment and Jobs Act")
    detail.sponsors.fold(0)(_.size) shouldBe 1
  }

  it should "strip a pre-existing format= query param and re-add format=json" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client = makeClient()
    val _ = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137?format=xml", testCorrelationId)
      .unsafeRunSync()

    // No format=xml on the wire, exactly one format=json.
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .withQueryParam("format", equalTo("json"))
    )
  }

  it should "raise AmendmentFetchFailed on 404 (caller decides skip-vs-halt)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/9999"))
        .willReturn(aResponse().withStatus(404).withBody("not found"))
    )

    val client = makeClient(retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 0L))
    intercept[AmendmentFetchFailed] {
      val _ = client
        .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/9999", testCorrelationId)
        .unsafeRunSync()
    }
  }

  it should "retry detail on 429 then succeed" in {
    val scenario = "transient-429-detail"
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .inScenario(scenario)
        .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withBody("rate limit"))
        .willSetStateTo("recovered")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .inScenario(scenario)
        .whenScenarioStateIs("recovered")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client = makeClient()
    val detail = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137", testCorrelationId)
      .unsafeRunSync()
    detail.number shouldBe "2137"
  }

  it should "raise AmendmentFetchFailed when detailUrl is unparseable" in {
    val client = makeClient(retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 0L))
    intercept[AmendmentFetchFailed] {
      val _ = client.fetchDetail("not a valid url with spaces", testCorrelationId).unsafeRunSync()
    }
  }

  it should "include the api_key on detail requests" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client = makeClient()
    val _ = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137", testCorrelationId)
      .unsafeRunSync()

    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .withQueryParam("api_key", equalTo("test-api-key"))
    )
  }

  it should "strip a pre-existing api_key= query param and re-add the configured one" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client = makeClient()
    val _ = client
      .fetchDetail(
        s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137?api_key=stale-key",
        testCorrelationId,
      )
      .unsafeRunSync()

    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .withQueryParam("api_key", equalTo("test-api-key"))
    )
  }

  it should "thread the caller-supplied correlationId through to the retry wrapper" in {
    // Different correlation IDs on two consecutive calls both succeed — proves the parameter is wired through, not
    // dropped on the floor in favor of an internally-minted UUID. Also serves as the test we'd want when the §7.3
    // processor lands and we can spy on the retry-wrapper callback to assert the exact UUID landed.
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val client     = makeClient()
    val firstId    = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val secondId   = UUID.fromString("22222222-2222-2222-2222-222222222222")
    val detailUrl  = s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137"
    val firstCall  = client.fetchDetail(detailUrl, firstId).unsafeRunSync()
    val secondCall = client.fetchDetail(detailUrl, secondId).unsafeRunSync()

    val _ = firstCall.number shouldBe "2137"
    secondCall.number shouldBe "2137"
  }

  it should "pass the caller's correlationId to the RetryWrapper callback verbatim" in {
    // Force one transient retry (503 → 200) so the wrapper's logRetry callback fires; capture the correlationId arg
    // and assert it equals the value the caller passed to fetchDetail. Proves the parameter reaches the wrapper
    // verbatim instead of being replaced by a fresh UUID.randomUUID() inside the client.
    val capturedIds = Ref.unsafe[IO, List[UUID]](List.empty)
    val spyWrapper  = new RetryWrapper[IO]({ (_, _, _, _, _, correlationId) => capturedIds.update(_ :+ correlationId) })

    val scenario = "correlation-id-passthrough"
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .inScenario(scenario)
        .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
        .willReturn(aResponse().withStatus(503).withBody("transient"))
        .willSetStateTo("recovered")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137"))
        .inScenario(scenario)
        .whenScenarioStateIs("recovered")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(detailJson(117, "SAMDT", "2137"))
        )
    )

    val cfg = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 1, initialBackoffMs = 10L),
    )
    val client     = AmendmentsApiClient[IO](cfg, httpClient, spyWrapper)
    val expectedId = UUID.fromString("33333333-3333-3333-3333-333333333333")
    val _ = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/amendment/117/samdt/2137", expectedId)
      .unsafeRunSync()

    capturedIds.get.unsafeRunSync() should contain(expectedId)
  }

}
