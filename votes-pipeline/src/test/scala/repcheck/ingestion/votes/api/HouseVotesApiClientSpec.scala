package repcheck.ingestion.votes.api

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

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
import repcheck.ingestion.common.api.{CongressGovClientConfig, HttpClientConfig}
import repcheck.ingestion.votes.config.HouseVotesConfig
import repcheck.ingestion.votes.errors.{HouseVoteApiErrorClassifier, HouseVoteApiHttpError, HouseVoteFetchFailed}
import repcheck.shared.models.congress.dto.vote.VoteListItemDTO

import com.repcheck.utils.errors.{ErrorClass, RetryConfig, RetryWrapper}

/**
 * Unit + component coverage for [[HouseVotesApiClient]] — combines MockitoScala-free argument-matcher checks with
 * WireMock integration for the happy-path and retry branches. See acceptance-criteria §6.1 rows 1-14 for the full
 * matrix; most rows are covered end-to-end via WireMock while the two that are genuinely unit-level (AC#5: api_key
 * present on every request; AC#13: error context contains all three identifiers) ride alongside.
 *
 * Per P6.H5, the (congress, session) tuple is no longer pinned at construction — every list call to
 * `fetchRecentVotes(congress, session)` carries the pair explicitly. Tests pass `(119, 1)` for determinism. The
 * underlying `fetchPage` / `fetchAllPages` helpers are private now; pagination, page-delay pacing, and lookback
 * filtering are all exercised by driving `fetchRecentVotes` against multi-page WireMock fixtures.
 *
 * WireMock binds to `127.0.0.1` + a dynamic port per memory on Windows to avoid firewall prompts. WireMock stubs use
 * recorded real-shape Congress.gov JSON so decoders are exercised against the actual beta-API field names.
 */
class HouseVotesApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  // Ember client reused across cases — spins up once, shares the event loop. Per-test rate limiting is handled by
  // `pageDelay` in the test config rather than an app-level wrapper because we want to exercise the real inter-page
  // delay branch of `fetchAllPages` (private) via the public `fetchRecentVotes` entry point.
  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()
    ._1

  private lazy val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  private def makeClient(
    retryConfig: RetryConfig =
      RetryConfig(maxRetries = 1, initialBackoffMs = 10L, maxBackoffMs = 100L, backoffMultiplier = 2.0),
    pageDelay: FiniteDuration = Duration.Zero,
    houseConfig: HouseVotesConfig = HouseVotesConfig(parallelism = 1, pageDelay = 0.millis, lookbackDays = 0),
    pageSize: Int = 250,
  ): HouseVotesApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://127.0.0.1:${wireMock.port()}/v3",
      pageSize = pageSize,
      pageDelay = pageDelay,
      retry = retryConfig,
      http = HttpClientConfig(),
    )
    HouseVotesApiClient[IO](config, houseConfig, httpClient, retryWrapper)
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

  // -----------------------------------------------------------------------------------------------------------------
  // Fixture builders — shape matches real Congress.gov house-vote responses (bioguideID/voteParty/voteState casing,
  // houseRollCallVotes / houseRollCallVoteMemberVotes envelope keys, integer identifier, sourceDataURL uppercase URL).
  // -----------------------------------------------------------------------------------------------------------------

  private def voteJson(
    rollCall: Int,
    updateDate: String = "2025-04-20T12:00:00-04:00",
    sessionNumber: Int = 1,
  ): String =
    s"""{
       |  "congress": 119,
       |  "identifier": ${1191000000 + rollCall},
       |  "legislationNumber": "100",
       |  "legislationType": "HR",
       |  "legislationUrl": "https://www.congress.gov/bill/119/house-bill/100",
       |  "result": "Passed",
       |  "rollCallNumber": $rollCall,
       |  "sessionNumber": $sessionNumber,
       |  "sourceDataURL": "https://clerk.house.gov/evs/2025/roll${"%03d".format(rollCall)}.xml",
       |  "startDate": "2025-04-20T11:00:00-04:00",
       |  "updateDate": "$updateDate",
       |  "url": "https://api.congress.gov/v3/house-vote/119/$sessionNumber/$rollCall",
       |  "voteType": "Yea-and-Nay"
       |}""".stripMargin

  private def voteListEnvelopeJson(votes: List[String], totalCount: Int): String = {
    val items = votes.mkString(",")
    s"""{
       |  "houseRollCallVotes": [$items],
       |  "pagination": {
       |    "count": $totalCount
       |  }
       |}""".stripMargin
  }

  private def voteMembersEnvelopeJson(
    rollCall: Int,
    voteQuestion: String = "On Passage",
    results: List[String] = defaultResults,
  ): String = {
    val resultsArr = results.mkString(",")
    s"""{
       |  "houseRollCallVoteMemberVotes": {
       |    "congress": 119,
       |    "identifier": ${1191000000 + rollCall},
       |    "legislationNumber": "100",
       |    "legislationType": "HR",
       |    "legislationUrl": "https://www.congress.gov/bill/119/house-bill/100",
       |    "result": "Passed",
       |    "rollCallNumber": $rollCall,
       |    "sessionNumber": 1,
       |    "sourceDataURL": "https://clerk.house.gov/evs/2025/roll${"%03d".format(rollCall)}.xml",
       |    "startDate": "2025-04-20T11:00:00-04:00",
       |    "updateDate": "2025-04-20T12:00:00-04:00",
       |    "url": "https://api.congress.gov/v3/house-vote/119/1/$rollCall",
       |    "voteQuestion": "$voteQuestion",
       |    "voteType": "Yea-and-Nay",
       |    "results": [$resultsArr]
       |  }
       |}""".stripMargin
  }

  private def memberResultJson(
    bioguideID: String,
    voteCast: String = "Yea",
    voteParty: String = "R",
    voteState: String = "AL",
  ): String =
    s"""{
       |  "bioguideID": "$bioguideID",
       |  "firstName": "Robert",
       |  "lastName": "Aderholt",
       |  "voteCast": "$voteCast",
       |  "voteParty": "$voteParty",
       |  "voteState": "$voteState"
       |}""".stripMargin

  private val defaultResults: List[String] = List(
    memberResultJson("A000055"),
    memberResultJson("A000148", voteParty = "D", voteState = "MA"),
  )

  // -----------------------------------------------------------------------------------------------------------------
  // fetchRecentVotes (single-page) — AC#1 (decoded list items), AC#5 (api_key on every request)
  // -----------------------------------------------------------------------------------------------------------------

  "fetchRecentVotes" should "decode the houseRollCallVotes envelope into VoteListItemDTO instances" in {
    val singleVote = voteJson(rollCall = 17)
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List(singleVote), totalCount = 1))
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()

    val _    = items.size shouldBe 1
    val item = items.headOption.getOrElse(fail("expected one vote in the decoded list"))
    val _    = item.congress shouldBe 119
    val _    = item.rollCallNumber shouldBe 17
    val _    = item.chamber shouldBe "House"
    val _    = item.voteType shouldBe Some("Yea-and-Nay")
    val _    = item.identifier shouldBe Some((1191000000 + 17).toString)
    item.sourceDataUrl.exists(_.contains("roll017.xml")) shouldBe true
  }

  it should "include api_key as a query parameter on every request" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
        )
    )

    val client = makeClient()
    val _      = client.fetchRecentVotes(119, 1).unsafeRunSync()
    // URL shape confirmed against the Congress.gov OpenAPI spec: `/house-vote/{congress}/{session}` at
    // congress-gov-api.yaml lines 1061-1084. Allowed query params are `format` / `offset` / `limit`; we add `api_key`
    // as the global auth param. Nothing else is emitted.
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .withQueryParam("offset", equalTo("0"))
        .withQueryParam("limit", equalTo("250"))
    )
  }

  it should "NOT include fromDateTime, toDateTime, or sort query params on /house-vote" in {
    // AC: the beta endpoint rejects those params with HTTP 400; the client must never emit them. The previous spec
    // exercised this by passing FetchParams with explicit fromDateTime/toDateTime — those slots are now hidden behind
    // the private fetchPage/fetchAllPages helpers and the public fetchRecentVotes entry never accepts them, so the
    // test reduces to "no such params ever appear on the wire". A single fetchRecentVotes call is enough.
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
        )
    )

    val client = makeClient()
    val _      = client.fetchRecentVotes(119, 1).unsafeRunSync()

    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("fromDateTime", absent())
        .withQueryParam("toDateTime", absent())
        .withQueryParam("sort", absent())
    )
  }

  // -----------------------------------------------------------------------------------------------------------------
  // Pagination — AC#2 (stops when items < pageSize). Driven via fetchRecentVotes which walks pages internally.
  // -----------------------------------------------------------------------------------------------------------------

  it should "collect 550 items across 3 pages (250+250+50) and stop when items < pageSize" in {
    val page1 = (1 to 250).map(i => voteJson(rollCall = i)).toList
    val page2 = (251 to 500).map(i => voteJson(rollCall = i)).toList
    val page3 = (501 to 550).map(i => voteJson(rollCall = i)).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page1, totalCount = 550))
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("250"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page2, totalCount = 550))
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("500"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page3, totalCount = 550))
        )
    )

    // Use a lookbackDays=0 (disabled) so the test doesn't exercise the filter branch.
    val client = makeClient(
      pageSize = 250,
      houseConfig = HouseVotesConfig(parallelism = 1, pageDelay = 0.millis, lookbackDays = 0),
    )
    val items = client.fetchRecentVotes(119, 1).unsafeRunSync()
    val _     = items.size shouldBe 550
    wireMock.verify(3, getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1")))
  }

  // -----------------------------------------------------------------------------------------------------------------
  // fetchMembersVotePositions — AC#3 (VoteMembersDTO with results list), AC#4 (voteQuestion populated)
  // -----------------------------------------------------------------------------------------------------------------

  "fetchMembersVotePositions" should "decode the houseRollCallVoteMemberVotes envelope and return VoteMembersDTO with positions" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1/240/members"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteMembersEnvelopeJson(rollCall = 240))
        )
    )

    val client = makeClient()
    val members = client
      .fetchMembersVotePositions(congress = 119, session = 1, voteNumber = 240)
      .unsafeRunSync()
      .getOrElse(fail("expected Some(VoteMembersDTO) for a populated vote response"))

    val _       = members.congress shouldBe 119
    val _       = members.rollCallNumber shouldBe 240
    val _       = members.chamber shouldBe "House"
    val results = members.results.getOrElse(fail("expected a results list to be decoded into the DTO"))
    val _       = results.size shouldBe 2
    val first   = results.headOption.getOrElse(fail("expected a first member-result"))
    val _       = first.memberId shouldBe Some("A000055")
    val _       = first.voteCast shouldBe Some("Yea")
    val _       = first.party shouldBe Some("R")
    first.state shouldBe Some("AL")
  }

  it should "preserve the voteQuestion string verbatim (no lossy normalization)" in {
    val rawQuestion = "On Motion to Suspend the Rules and Pass"
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1/17/members"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteMembersEnvelopeJson(rollCall = 17, voteQuestion = rawQuestion))
        )
    )

    val client = makeClient()
    val members = client
      .fetchMembersVotePositions(119, 1, 17)
      .unsafeRunSync()
      .getOrElse(fail("expected Some(VoteMembersDTO) for a populated vote response"))
    members.voteQuestion shouldBe Some(rawQuestion)
  }

  it should "include api_key as query parameter on the members request" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1/17/members"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteMembersEnvelopeJson(rollCall = 17))
        )
    )

    val client = makeClient()
    val _      = client.fetchMembersVotePositions(119, 1, 17).unsafeRunSync()
    // URL shape confirmed against the Congress.gov OpenAPI spec: `/house-vote/{congress}/{session}/{voteNumber}/members`
    // at congress-gov-api.yaml lines 1110-1134. Only `format` + `api_key` are emitted; we assert neither `offset` nor
    // `limit` nor the forbidden `fromDateTime` / `toDateTime` / `sort` are present.
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1/17/members"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
        .withQueryParam("fromDateTime", absent())
        .withQueryParam("toDateTime", absent())
        .withQueryParam("sort", absent())
    )
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#6 reframed — client-side lookback filtering
  // -----------------------------------------------------------------------------------------------------------------

  "fetchRecentVotes lookback" should "fetch all pages then filter client-side by lookback window" in {
    // Page 1 (5 items, size == pageSize so fetchAllPages keeps paginating) all within 7-day lookback. Page 2 (3 items,
    // size < pageSize so fetchAllPages stops) all outside the window. After client-side filter we expect just the 5
    // from page 1, and WireMock records exactly 2 successful list requests — pagination drives past page 1 because
    // API ordering is undefined and the client doesn't short-circuit, but terminates on page 2 naturally because
    // items.size < pageSize.
    val today   = Instant.now().atOffset(java.time.ZoneOffset.ofHours(-4))
    val sixDays = today.minusDays(6).toString
    val tenDays = today.minusDays(10).toString
    val page1   = (1 to 5).map(i => voteJson(rollCall = i, updateDate = sixDays)).toList
    val page2   = (6 to 8).map(i => voteJson(rollCall = i, updateDate = tenDays)).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page1, totalCount = 8))
        )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("5"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page2, totalCount = 8))
        )
    )

    val client = makeClient(
      pageSize = 5,
      houseConfig = HouseVotesConfig(parallelism = 1, pageDelay = 0.millis, lookbackDays = 7),
    )

    val filtered = client.fetchRecentVotes(119, 1).unsafeRunSync()
    val _        = filtered.size shouldBe 5
    val _        = filtered.forall(_.rollCallNumber <= 5) shouldBe true
    // Two list requests — exactly enough to drain the session's vote list. We do NOT short-circuit after one page,
    // which would be wrong because API ordering is undefined (older items might appear before newer ones).
    wireMock.verify(2, getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1")))
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#7/#8 — Transient classification (429/5xx) → retried. AC#9 — Systemic (401/403) not retried.
  // -----------------------------------------------------------------------------------------------------------------

  "retry behavior" should "retry on HTTP 429 and succeed on the second attempt" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .inScenario("429-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withBody("Rate limited"))
        .willSetStateTo("retried")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .inScenario("429-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items shouldBe empty
  }

  it should "retry on HTTP 500 and succeed on the second attempt" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .inScenario("500-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
        .willSetStateTo("retried")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .inScenario("500-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items shouldBe empty
  }

  it should "NOT retry on HTTP 401 (Systemic) and raise HouseVoteFetchFailed immediately" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(aResponse().withStatus(401).withBody("Unauthorized"))
    )

    val client = makeClient()
    val ex = intercept[HouseVoteFetchFailed] {
      client.fetchRecentVotes(119, 1).unsafeRunSync()
    }
    val _ = ex.getMessage should include("congress=119")
    val _ = ex.getMessage should include("session=1")
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1")))
  }

  it should "NOT retry on HTTP 403 (Systemic) and raise HouseVoteFetchFailed immediately" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )

    val client = makeClient()
    val _ = intercept[HouseVoteFetchFailed] {
      client.fetchRecentVotes(119, 1).unsafeRunSync()
    }
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1")))
  }

  it should "NOT retry on HTTP 400 (Systemic) and raise HouseVoteFetchFailed immediately" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(aResponse().withStatus(400).withBody("Bad request"))
    )

    val client = makeClient()
    val _ = intercept[HouseVoteFetchFailed] {
      client.fetchRecentVotes(119, 1).unsafeRunSync()
    }
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/v3/house-vote/119/1")))
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#10 — 404 on a specific vote surfaces as HouseVoteFetchFailed with the vote number attached.
  // -----------------------------------------------------------------------------------------------------------------

  "fetchMembersVotePositions" should "raise HouseVoteFetchFailed with voteNumber on 404" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1/9999/members"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val client = makeClient()
    val ex = intercept[HouseVoteFetchFailed] {
      client.fetchMembersVotePositions(119, 1, 9999).unsafeRunSync()
    }
    val _ = ex.voteNumber shouldBe Some(9999)
    val _ = ex.congress shouldBe 119
    val _ = ex.session shouldBe 1
    val _ = ex.getMessage should include("vote=9999")
    ex.getMessage should include("congress=119")
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#11 — pageDelay is respected between pages. Real clock (not time-mocked).
  // -----------------------------------------------------------------------------------------------------------------

  "pageDelay" should "introduce at least the configured delay between pages" in {
    val page1 = (1 to 5).map(i => voteJson(rollCall = i)).toList
    val page2 = (6 to 10).map(i => voteJson(rollCall = i)).toList
    val page3 = (11 to 12).map(i => voteJson(rollCall = i)).toList

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("0"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page1, totalCount = 12))
        )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("5"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page2, totalCount = 12))
        )
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .withQueryParam("offset", equalTo("10"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(page3, totalCount = 12))
        )
    )

    // pageDelay applies between pages internally during fetchAllPages. Use lookbackDays=0 so the filter doesn't drop
    // any vote and pagination drives all 3 pages.
    val client = makeClient(
      pageSize = 5,
      pageDelay = 100.millis,
      houseConfig = HouseVotesConfig(parallelism = 1, pageDelay = 0.millis, lookbackDays = 0),
    )
    val startedAt = System.nanoTime()
    val _         = client.fetchRecentVotes(119, 1).unsafeRunSync()
    val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
    // 3 pages → 2 inter-page delays of ≥ 100ms → ≥ 200ms total elapsed in the delay dimension. We allow a small margin
    // to avoid flakes from clock resolution on CI hosts but enforce the contract.
    elapsedMs should be >= 200L
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#12 — Timeout applies per-request. Simulated via `withFixedDelay` larger than the client timeout.
  // -----------------------------------------------------------------------------------------------------------------

  "request timeout" should "fail when the server exceeds the configured timeout" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
            .withFixedDelay(8000) // 8s, longer than the 5s Ember timeout
        )
    )

    val client = makeClient(
      retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 10L, backoffMultiplier = 1.0)
    )
    val _ = intercept[Exception] {
      client.fetchRecentVotes(119, 1).unsafeRunSync()
    }
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#13 — Error context (congress, session, voteNumber) present in HouseVoteFetchFailed messages.
  // -----------------------------------------------------------------------------------------------------------------

  "HouseVoteFetchFailed" should "render congress, session, and voteNumber identifiers in the message" in {
    val withVote = HouseVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = Some(17),
      detail = "bang",
      cause = new Exception("boom"),
    )
    val _ = withVote.getMessage should include("congress=119")
    val _ = withVote.getMessage should include("session=1")
    val _ = withVote.getMessage should include("vote=17")
    val _ = withVote.getMessage should include("bang")

    val listOnly = HouseVoteFetchFailed(
      congress = 119,
      session = 1,
      voteNumber = None,
      detail = "no context",
      cause = new Exception(""),
    )
    val _ = listOnly.getMessage should not include "vote="
    val _ = listOnly.getMessage should include("congress=119")
    listOnly.getMessage should include("session=1")
  }

  // -----------------------------------------------------------------------------------------------------------------
  // AC#14 — decoders exercised on recorded real-shape JSON (covered by every WireMock fixture above, since all stubs
  // use the actual Congress.gov envelope keys and field casings).
  // -----------------------------------------------------------------------------------------------------------------

  it should "decode an empty houseRollCallVotes envelope without error" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteListEnvelopeJson(List.empty, totalCount = 0))
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items shouldBe empty
  }

  it should "decode a members envelope with an empty results list" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1/5/members"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(voteMembersEnvelopeJson(rollCall = 5, results = List.empty))
        )
    )

    val client = makeClient()
    val members = client
      .fetchMembersVotePositions(119, 1, 5)
      .unsafeRunSync()
      .getOrElse(fail("expected Some(VoteMembersDTO) when the envelope object is present (just with empty results)"))
    members.results shouldBe Some(List.empty[repcheck.shared.models.congress.dto.vote.VoteResultDTO])
  }

  it should "return None for the sentinel 'no member-vote data' shape (houseRollCallVoteMemberVotes is an empty array)" in {
    // Surfaced live during P6 backfill on 117-House-1-1 (Jan 2021 — early 117th congress vote that
    // pre-dates the API's member-vote dataset). Congress.gov returns the wrapper with an empty array
    // instead of the normal object body. Pre-fix the decoder failed and the whole vote was dropped;
    // post-fix the API client returns None and the processor emits ProcessingResult.Skipped.
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/117/1/1/members"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              """{
                |  "houseRollCallVoteMemberVotes": [],
                |  "request": {
                |    "congress": "117",
                |    "contentType": "application/json",
                |    "format": "json",
                |    "session": "1"
                |  }
                |}""".stripMargin
            )
        )
    )

    val client = makeClient()
    val result = client.fetchMembersVotePositions(117, 1, 1).unsafeRunSync()
    result shouldBe None
  }

  // -----------------------------------------------------------------------------------------------------------------
  // Classifier unit tests — no HTTP, just classification correctness for sample statuses.
  // -----------------------------------------------------------------------------------------------------------------

  "HouseVoteApiErrorClassifier" should "classify 429 as Transient" in {
    HouseVoteApiErrorClassifier.classify(HouseVoteApiHttpError(429, "rate limited")) shouldBe ErrorClass.Transient
  }

  it should "classify 500/502/503/504 as Transient" in {
    Set(500, 502, 503, 504).foreach { code =>
      val _ = HouseVoteApiErrorClassifier.classify(HouseVoteApiHttpError(code, "")) shouldBe ErrorClass.Transient
    }
  }

  it should "classify 400 as Systemic" in {
    HouseVoteApiErrorClassifier.classify(HouseVoteApiHttpError(400, "bad")) shouldBe ErrorClass.Systemic
  }

  it should "classify 401/403/404 as Systemic" in {
    Set(401, 403, 404).foreach { code =>
      val _ = HouseVoteApiErrorClassifier.classify(HouseVoteApiHttpError(code, "")) shouldBe ErrorClass.Systemic
    }
  }

  it should "classify a non-HttpStatusError throwable as Systemic" in {
    val stranger = new Exception("not HTTP")
    HouseVoteApiErrorClassifier.classify(stranger) shouldBe ErrorClass.Systemic
  }

  // -----------------------------------------------------------------------------------------------------------------
  // filterByLookback — pure helper tests (no HTTP, no async)
  // -----------------------------------------------------------------------------------------------------------------

  "filterByLookback" should "sort items DESC by updateDate" in {
    val now = Instant.parse("2025-04-20T12:00:00Z")
    val items = List(
      sampleListItem(rollCall = 1, updateDate = Some("2025-04-15T00:00:00Z")),
      sampleListItem(rollCall = 2, updateDate = Some("2025-04-19T00:00:00Z")),
      sampleListItem(rollCall = 3, updateDate = Some("2025-04-10T00:00:00Z")),
    )

    val sorted = HouseVotesApiClient.filterByLookback(items, lookbackDays = 0, now = now)
    sorted.map(_.rollCallNumber) shouldBe List(2, 1, 3)
  }

  it should "drop items whose updateDate is older than the lookback window" in {
    val now     = Instant.parse("2025-04-20T12:00:00Z")
    val cutoff  = now.minus(7L, ChronoUnit.DAYS)
    val within  = sampleListItem(rollCall = 1, updateDate = Some(cutoff.plusSeconds(60).toString))
    val outside = sampleListItem(rollCall = 2, updateDate = Some(cutoff.minusSeconds(60).toString))

    val filtered = HouseVotesApiClient.filterByLookback(List(within, outside), lookbackDays = 7, now = now)
    val _        = filtered.map(_.rollCallNumber) shouldBe List(1)
    filtered.size shouldBe 1
  }

  it should "drop items with no updateDate when the filter is active" in {
    val now      = Instant.parse("2025-04-20T12:00:00Z")
    val withDate = sampleListItem(rollCall = 1, updateDate = Some(now.toString))
    val noDate   = sampleListItem(rollCall = 2, updateDate = None)

    val filtered = HouseVotesApiClient.filterByLookback(List(withDate, noDate), lookbackDays = 7, now = now)
    filtered.map(_.rollCallNumber) shouldBe List(1)
  }

  it should "keep all items when lookbackDays <= 0" in {
    val now = Instant.parse("2025-04-20T12:00:00Z")
    val items = List(
      sampleListItem(rollCall = 1, updateDate = Some("2025-01-01T00:00:00Z")),
      sampleListItem(rollCall = 2, updateDate = Some("2025-04-19T00:00:00Z")),
      sampleListItem(rollCall = 3, updateDate = None),
    )

    val filtered = HouseVotesApiClient.filterByLookback(items, lookbackDays = 0, now = now)
    filtered.size shouldBe 3
  }

  "parseUpdateDate" should "accept ISO with offset" in {
    HouseVotesApiClient.parseUpdateDate("2025-09-09T18:53:19-04:00") shouldBe defined
  }

  it should "accept UTC Z suffix" in {
    HouseVotesApiClient.parseUpdateDate("2025-09-09T18:53:19Z") shouldBe defined
  }

  it should "return None for malformed date strings" in {
    HouseVotesApiClient.parseUpdateDate("not a date") shouldBe None
  }

  // -----------------------------------------------------------------------------------------------------------------
  // parseUri failure path — invalid URL surfaces as HouseVoteFetchFailed
  // -----------------------------------------------------------------------------------------------------------------

  "parseUri" should "raise HouseVoteFetchFailed on malformed URL" in {
    // Build a client whose `baseUrl` contains a newline, which http4s refuses to parse as a URI scheme/host.
    val badConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "http://\nhost-with-newline:1234/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 10L, backoffMultiplier = 1.0),
      http = HttpClientConfig(),
    )
    val client = HouseVotesApiClient[IO](badConfig, HouseVotesConfig(), httpClient, retryWrapper)

    val ex = intercept[HouseVoteFetchFailed] {
      client.fetchRecentVotes(119, 1).unsafeRunSync()
    }
    val _ = ex.voteNumber shouldBe None
    val _ = ex.congress shouldBe 119
    ex.getMessage should include("Invalid URL")
  }

  it should "raise HouseVoteFetchFailed with voteNumber on members URL parse failure" in {
    val badConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "http://\nhost-with-newline:1234/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 10L, backoffMultiplier = 1.0),
      http = HttpClientConfig(),
    )
    val client = HouseVotesApiClient[IO](badConfig, HouseVotesConfig(), httpClient, retryWrapper)

    val ex = intercept[HouseVoteFetchFailed] {
      client.fetchMembersVotePositions(119, 1, 42).unsafeRunSync()
    }
    ex.voteNumber shouldBe Some(42)
  }

  // -----------------------------------------------------------------------------------------------------------------
  // Identifier decoder — string fallback and missing-field path
  // -----------------------------------------------------------------------------------------------------------------

  "identifier decoding" should "accept a string-typed identifier from the API as a fallback" in {
    // In case the API ever returns `"identifier": "11912025240"` as a string instead of the documented integer, we
    // should still decode without failing. We feed the decoder such a payload and assert the field round-trips.
    val body =
      """{
        |  "houseRollCallVotes": [{
        |    "congress": 119,
        |    "rollCallNumber": 1,
        |    "sessionNumber": 1,
        |    "identifier": "abc-not-an-int"
        |  }]
        |}""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items.headOption.flatMap(_.identifier) shouldBe Some("abc-not-an-int")
  }

  it should "accept a payload with no identifier field at all" in {
    val body =
      """{
        |  "houseRollCallVotes": [{
        |    "congress": 119,
        |    "rollCallNumber": 1,
        |    "sessionNumber": 1
        |  }]
        |}""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items.headOption.flatMap(_.identifier) shouldBe None
  }

  it should "accept the legacy sourceDataUrl (lowercase l) field if the API changes casing" in {
    // The real API uses `sourceDataURL` but we tolerate the lowercase variant so future API casing fixes don't break
    // the client.
    val body =
      """{
        |  "houseRollCallVotes": [{
        |    "congress": 119,
        |    "rollCallNumber": 1,
        |    "sessionNumber": 1,
        |    "sourceDataUrl": "https://clerk.house.gov/evs/2025/roll001.xml"
        |  }]
        |}""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/house-vote/119/1"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(body)
        )
    )

    val client = makeClient()
    val items  = client.fetchRecentVotes(119, 1).unsafeRunSync()
    items.headOption.flatMap(_.sourceDataUrl) shouldBe Some("https://clerk.house.gov/evs/2025/roll001.xml")
  }

  private def sampleListItem(rollCall: Int, updateDate: Option[String]): VoteListItemDTO =
    VoteListItemDTO(
      congress = 119,
      chamber = "House",
      rollCallNumber = rollCall,
      sessionNumber = Some(1),
      startDate = None,
      updateDate = updateDate,
      result = None,
      voteType = None,
      legislationNumber = None,
      legislationType = None,
      legislationUrl = None,
      url = None,
      identifier = None,
      sourceDataUrl = None,
    )

}
