package repcheck.ingestion.members.profile.api

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
import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams}
import repcheck.ingestion.members.profile.errors.MemberFetchFailed
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}

class MembersApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private lazy val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  private def makeClient(
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 10L),
    pageSize: Int = 25,
  ): MembersApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = pageSize,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    MembersApiClient[IO](config, httpClient, retryWrapper)
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

  private def memberJson(bioguideId: String, name: String): String =
    s"""{
       |  "bioguideId": "$bioguideId",
       |  "name": "$name",
       |  "partyName": "Democratic",
       |  "state": "California",
       |  "updateDate": "2024-01-01T00:00:00Z",
       |  "url": "https://api.congress.gov/v3/member/$bioguideId"
       |}""".stripMargin

  private def membersListJson(members: List[String]): String = {
    val items = members.mkString(",")
    s"""{"members": [$items], "pagination": {"count": ${members.size}}}"""
  }

  private def memberDetailJson(bioguideId: String): String =
    s"""{
       |  "member": {
       |    "bioguideId": "$bioguideId",
       |    "birthYear": "1960",
       |    "firstName": "Jane",
       |    "lastName": "Doe",
       |    "directOrderName": "Jane Doe",
       |    "invertedOrderName": "Doe, Jane",
       |    "state": "California",
       |    "updateDate": "2024-01-01T00:00:00Z",
       |    "partyHistory": [
       |      {"partyAbbreviation": "D", "partyName": "Democratic", "startYear": 2001}
       |    ],
       |    "terms": [
       |      {"chamber": "House of Representatives", "congress": 118, "startYear": 2023, "stateCode": "CA"}
       |    ]
       |  }
       |}""".stripMargin

  "fetchPage" should "return decoded member list items for a congress" in {
    val members = List(memberJson("N000147", "Norton, Eleanor Holmes"))
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(members))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()

    val _ = result.items.size shouldBe 1
    val _ = result.items.headOption.map(_.bioguideId) shouldBe Some("N000147")
    result.items.headOption.flatMap(_.name) shouldBe Some("Norton, Eleanor Holmes")
  }

  it should "include congress, offset, limit, and api_key in the request URL" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("25"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(List.empty))
        )
    )

    val client = makeClient()
    val _      = client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("25"))
    )
  }

  it should "return an empty page when the response has no members" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()

    val _ = result.items shouldBe empty
    result.totalCount shouldBe 0
  }

  "fetchAll" should "return all members in a single page when fewer than pageSize" in {
    val members = (1 to 25).map(i => memberJson("M%06d".format(i), s"Member $i")).toList
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(members))
        )
    )

    val client = makeClient(pageSize = 250)
    val result = client
      .fetchAll(FetchParams(congress = Some(118), pageSize = 250))
      .compile
      .toList
      .unsafeRunSync()

    result.size shouldBe 25
  }

  it should "paginate across multiple pages" in {
    val page1 = (1 to 25).map(i => memberJson("A%06d".format(i), s"Member $i")).toList
    val page2 = (26 to 40).map(i => memberJson("B%06d".format(i), s"Member $i")).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(page1))
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .withQueryParam("offset", equalTo("25"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(page2))
        )
    )

    val client = makeClient(pageSize = 25)
    val result = client
      .fetchAll(FetchParams(congress = Some(118), pageSize = 25))
      .compile
      .toList
      .unsafeRunSync()

    result.size shouldBe 40
  }

  "fetchDetail" should "decode full member profile with party history and terms" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/N000147"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(memberDetailJson("N000147"))
        )
    )

    val client = makeClient()
    val detail = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/member/N000147")
      .unsafeRunSync()

    val _ = detail.bioguideId shouldBe "N000147"
    val _ = detail.firstName shouldBe Some("Jane")
    val _ = detail.partyHistory.map(_.size) shouldBe Some(1)
    detail.terms.map(_.size) shouldBe Some(1)
  }

  it should "append api_key and format=json to the detail URL" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/N000147"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(memberDetailJson("N000147"))
        )
    )

    val client = makeClient()
    val _ = client
      .fetchDetail(s"http://localhost:${wireMock.port()}/v3/member/N000147")
      .unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/member/N000147"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
    )
  }

  it should "raise MemberFetchFailed when detail endpoint returns 404" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/UNKNOWN"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val client = makeClient()
    val ex = intercept[MemberFetchFailed] {
      client
        .fetchDetail(s"http://localhost:${wireMock.port()}/v3/member/UNKNOWN")
        .unsafeRunSync()
    }
    ex.getMessage should include("Not Found")
  }

  "retry behavior" should "retry on HTTP 500 and succeed on the second attempt" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .inScenario("500-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
        .willSetStateTo("retried")
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .inScenario("500-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(membersListJson(List.empty))
        )
    )

    val client = makeClient()
    val result = client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()
    val _      = result.items shouldBe empty
    wireMock.verify(2, getRequestedFor(urlPathEqualTo("/v3/member/congress/118")))
  }

  it should "fail immediately on HTTP 403 (non-retryable)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )

    val client = makeClient()
    val ex = intercept[MemberFetchFailed] {
      client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()
    }
    val _ = ex.getMessage should include("Forbidden")
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v3/member/congress/118")))
  }

  it should "raise descriptive error on malformed JSON" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/member/congress/118"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{invalid json}")
        )
    )

    val client = makeClient()
    val _ = intercept[Exception] {
      client.fetchPage(FetchParams(congress = Some(118), pageSize = 25)).unsafeRunSync()
    }
  }

}
