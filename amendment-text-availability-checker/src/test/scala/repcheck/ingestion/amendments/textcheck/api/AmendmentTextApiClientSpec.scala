package repcheck.ingestion.amendments.textcheck.api

import java.util.UUID

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
import repcheck.ingestion.amendments.textcheck.errors.AmendmentTextCheckFailed
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.shared.models.congress.amendment.AmendmentType

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class AmendmentTextApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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

  private val correlationId = UUID.fromString("11111111-2222-3333-4444-555555555555")

  private def makeClient(
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 5L)
  ): AmendmentTextApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    new AmendmentTextApiClient[IO](config, httpClient, retryWrapper)
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

  // Inline JSON fixtures — mirror the on-disk fixtures under
  // src/test/resources/__files/wiremock/amendment-text/. Inlined here so the WireMock server doesn't
  // have to be configured with `withRootDirectory(...)`; the on-disk files remain available for
  // the integration spec which boots a fresh WireMock instance without the resource-loading dance.
  private val submittedHtmlBody: String =
    """{
      |  "textVersions": [
      |    {
      |      "type": "Submitted",
      |      "date": "2024-04-01T12:00:00Z",
      |      "formats": [
      |        { "type": "HTML", "url": "https://www.congress.gov/117/amdt/SAMDT2137/SUB/text.htm" },
      |        { "type": "PDF",  "url": "https://www.congress.gov/117/amdt/SAMDT2137/SUB/text.pdf" }
      |      ]
      |    }
      |  ],
      |  "pagination": { "count": 1 }
      |}""".stripMargin

  private val submittedAndModifiedBody: String =
    """{
      |  "textVersions": [
      |    { "type": "Submitted", "date": "2024-04-01T12:00:00Z",
      |      "formats": [{ "type": "HTML", "url": "https://www.congress.gov/sub.htm" }] },
      |    { "type": "Modified",  "date": "2024-04-15T12:00:00Z",
      |      "formats": [{ "type": "PDF",  "url": "https://www.congress.gov/mod.pdf" }] }
      |  ],
      |  "pagination": { "count": 2 }
      |}""".stripMargin

  private val emptyBody: String = """{ "textVersions": [], "pagination": { "count": 0 } }"""

  "fetchTextVersions" should "decode a single Submitted/HTML version" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(submittedHtmlBody)
        )
    )

    val result = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    val _      = result.size shouldBe 1
    val _      = result.headOption.flatMap(_.`type`) shouldBe Some("Submitted")
    result.headOption.map(_.formats.size) shouldBe Some(2)
  }

  it should "decode multiple text versions (Submitted + Modified)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(submittedAndModifiedBody)
        )
    )

    val result = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    val _      = result.size shouldBe 2
    result.flatMap(_.`type`) should contain allOf ("Submitted", "Modified")
  }

  it should "return empty list on 404 (amendment has no text granules)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118/hamdt/9999/text"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val result = makeClient().fetchTextVersions(118, AmendmentType.HAMDT, "9999", correlationId).unsafeRunSync()
    result shouldBe empty
  }

  it should "return empty list when textVersions array is empty" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/1/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(emptyBody)
        )
    )

    val result = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "1", correlationId).unsafeRunSync()
    result shouldBe empty
  }

  it should "send api_key as a query parameter" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/100/text"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(emptyBody)
        )
    )

    val _ = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "100", correlationId).unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/amendment/117/samdt/100/text"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .withQueryParam("format", equalTo("json"))
    )
  }

  it should "lowercase the amendmentType in the path" in {
    // AmendmentType.apiValue is already lowercase, but verify the URL routing here so a future
    // change to apiValue casing surfaces in the test suite.
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/118/hamdt/42/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(emptyBody)
        )
    )
    val _ = makeClient().fetchTextVersions(118, AmendmentType.HAMDT, "42", correlationId).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/amendment/118/hamdt/42/text")))
  }

  "retry behavior" should "retry on HTTP 429" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .inScenario("429-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withBody("Rate limited"))
        .willSetStateTo("retried")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .inScenario("429-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(submittedHtmlBody)
        )
    )
    val result = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    result.size shouldBe 1
  }

  it should "retry on HTTP 500" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .inScenario("500-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
        .willSetStateTo("retried")
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .inScenario("500-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(submittedHtmlBody)
        )
    )
    val result = makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    result.size shouldBe 1
  }

  it should "fail immediately on HTTP 403" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )
    val ex = intercept[AmendmentTextCheckFailed] {
      makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    }
    ex.getMessage should include("117-SAMDT-2137")
  }

  it should "raise an error on malformed JSON" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/2137/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{invalid json}")
        )
    )
    intercept[Exception] {
      makeClient().fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    }
  }

  it should "raise AmendmentTextCheckFailed when the base URL is unparseable" in {
    val badConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "::not-a-valid-uri::",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L),
    )
    val client = new AmendmentTextApiClient[IO](badConfig, httpClient, retryWrapper)
    val ex = intercept[AmendmentTextCheckFailed] {
      client.fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    }
    ex.naturalKey shouldBe "117-SAMDT-2137"
  }

  it should "fall back to the HTTP status reason when the response body cannot be read" in {
    val errorBody   = fs2.Stream.raiseError[IO](new RuntimeException("simulated body read failure"))
    val badResponse = org.http4s.Response[IO](status = org.http4s.Status.Forbidden, body = errorBody)
    val badClient   = org.http4s.client.Client[IO](_ => cats.effect.Resource.pure(badResponse))
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port().toString}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 1L),
    )
    val client = new AmendmentTextApiClient[IO](config, badClient, retryWrapper)
    val ex = intercept[AmendmentTextCheckFailed] {
      client.fetchTextVersions(117, AmendmentType.SAMDT, "2137", correlationId).unsafeRunSync()
    }
    ex.naturalKey shouldBe "117-SAMDT-2137"
  }

}
