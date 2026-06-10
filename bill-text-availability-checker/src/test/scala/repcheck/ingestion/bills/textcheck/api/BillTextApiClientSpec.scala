package repcheck.ingestion.bills.textcheck.api

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
import repcheck.ingestion.bills.textcheck.errors.BillTextCheckFailed
import repcheck.ingestion.common.api.CongressGovClientConfig

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class BillTextApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val testCorrelationId = UUID.fromString("00000000-0000-0000-0000-000000000001")

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
    retryConfig: RetryConfig = RetryConfig(maxRetries = 1, initialBackoffMs = 10L)
  ): BillTextApiClient[IO] = {
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = retryConfig,
    )
    new BillTextApiClient[IO](config, httpClient, retryWrapper)
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

  private val singleVersionJson: String =
    """{
      |  "textVersions": [
      |    {
      |      "date": "2024-01-15T00:00:00Z",
      |      "type": "Introduced in House",
      |      "formats": [
      |        {"type": "Formatted Text", "url": "https://congress.gov/text/ih/formatted"},
      |        {"type": "PDF", "url": "https://congress.gov/text/ih/pdf"},
      |        {"type": "XML", "url": "https://congress.gov/text/ih/xml"}
      |      ]
      |    }
      |  ]
      |}""".stripMargin

  private val multiVersionJson: String =
    """{
      |  "textVersions": [
      |    {
      |      "date": "2024-01-15T00:00:00Z",
      |      "type": "Introduced in House",
      |      "formats": [{"type": "Formatted Text", "url": "https://congress.gov/text/ih"}]
      |    },
      |    {
      |      "date": "2024-02-01T00:00:00Z",
      |      "type": "Reported in House",
      |      "formats": [{"type": "Formatted Text", "url": "https://congress.gov/text/rh"}]
      |    },
      |    {
      |      "date": "2024-03-01T00:00:00Z",
      |      "type": "Enrolled Bill",
      |      "formats": [{"type": "Formatted Text", "url": "https://congress.gov/text/enr"}]
      |    }
      |  ]
      |}""".stripMargin

  "fetchTextVersions" should "return text versions with format URLs" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(singleVersionJson)
        )
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()

    val _ = result.size shouldBe 1
    result.headOption.flatMap(_.type_) shouldBe Some("Introduced in House")
  }

  it should "handle 404 gracefully (no text available)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/9999/text"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "9999", testCorrelationId).unsafeRunSync()
    result shouldBe empty
  }

  it should "return multiple text versions when available" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(multiVersionJson)
        )
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    result.size shouldBe 3
  }

  it should "preserve all format types (PDF, Formatted Text, XML)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(singleVersionJson)
        )
    )

    val client  = makeClient()
    val result  = client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    val formats = result.headOption.flatMap(_.formats).getOrElse(List.empty)
    val _       = formats.size shouldBe 3
    formats.map(_.type_) should contain allOf ("Formatted Text", "PDF", "XML")
  }

  it should "pass api_key on every request" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/100/text"))
        .withQueryParam("api_key", equalTo("test-api-key"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"textVersions": []}""")
        )
    )

    val client = makeClient()
    val _      = client.fetchTextVersions(118, "hr", "100", testCorrelationId).unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/bill/118/hr/100/text"))
        .withQueryParam("api_key", equalTo("test-api-key"))
    )
  }

  it should "construct correct URL from congress/billType/number" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/117/s/500/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"textVersions": []}""")
        )
    )

    val client = makeClient()
    val _      = client.fetchTextVersions(117, "s", "500", testCorrelationId).unsafeRunSync()
    wireMock.verify(getRequestedFor(urlPathEqualTo("/v3/bill/117/s/500/text")))
  }

  "retry behavior" should "retry on HTTP 429" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .inScenario("429-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(429).withBody("Rate limited"))
        .willSetStateTo("retried")
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .inScenario("429-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(singleVersionJson)
        )
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    result.size shouldBe 1
  }

  it should "retry on HTTP 500" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .inScenario("500-retry")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(500).withBody("Internal error"))
        .willSetStateTo("retried")
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .inScenario("500-retry")
        .whenScenarioStateIs("retried")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(singleVersionJson)
        )
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    result.size shouldBe 1
  }

  it should "fail immediately on HTTP 403" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )

    val client = makeClient()
    val ex = intercept[BillTextCheckFailed] {
      client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    }
    ex.getMessage should include("118-HR-1234")
  }

  it should "raise descriptive error on malformed JSON" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{invalid json}")
        )
    )

    val client = makeClient()
    val _ = intercept[Exception] {
      client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    }
  }

  it should "raise an error when the base URL is not a valid URI" in {
    val badConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = "::not-a-valid-uri::",
      pageSize = 250,
      pageDelay = scala.concurrent.duration.Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 10L),
    )
    val client = new BillTextApiClient[IO](badConfig, httpClient, retryWrapper)
    intercept[Exception] {
      client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    }
  }

  it should "return an empty list when textVersions array is empty" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/100/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"textVersions": []}""")
        )
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "100", testCorrelationId).unsafeRunSync()
    result shouldBe empty
  }

  it should "pass format=json query parameter on every request" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/200/text"))
        .withQueryParam("format", equalTo("json"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"textVersions": []}""")
        )
    )

    val client = makeClient()
    val _      = client.fetchTextVersions(118, "hr", "200", testCorrelationId).unsafeRunSync()
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/v3/bill/118/hr/200/text"))
        .withQueryParam("format", equalTo("json"))
    )
  }

  it should "fall back to the HTTP status reason when the response body stream raises an error" in {
    val errorBody   = fs2.Stream.raiseError[IO](new RuntimeException("simulated body read failure"))
    val badResponse = org.http4s.Response[IO](status = org.http4s.Status.Forbidden, body = errorBody)
    val badClient   = org.http4s.client.Client[IO](_ => cats.effect.Resource.pure(badResponse))
    val config = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port()}/v3",
      pageSize = 250,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 0, initialBackoffMs = 10L),
    )
    val client = new BillTextApiClient[IO](config, badClient, retryWrapper)
    val ex = intercept[BillTextCheckFailed] {
      client.fetchTextVersions(118, "hr", "1234", testCorrelationId).unsafeRunSync()
    }
    ex.getMessage should include("118-HR-1234")
  }

}
