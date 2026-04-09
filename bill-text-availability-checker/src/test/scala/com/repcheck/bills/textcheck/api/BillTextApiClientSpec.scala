package com.repcheck.bills.textcheck.api

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}

import com.repcheck.bills.textcheck.errors.BillTextCheckFailed

class BillTextApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(wireMockConfig().dynamicPort())

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
    val result = client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()

    val _ = result.size shouldBe 1
    result.headOption.flatMap(_.type_) shouldBe Some("Introduced in House")
  }

  it should "handle 404 gracefully (no text available)" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/9999/text"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val client = makeClient()
    val result = client.fetchTextVersions(118, "hr", "9999").unsafeRunSync()
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
    val result = client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
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
    val result  = client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
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
    val _      = client.fetchTextVersions(118, "hr", "100").unsafeRunSync()
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
    val _      = client.fetchTextVersions(117, "s", "500").unsafeRunSync()
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
    val result = client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
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
    val result = client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
    result.size shouldBe 1
  }

  it should "fail immediately on HTTP 403" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/118/hr/1234/text"))
        .willReturn(aResponse().withStatus(403).withBody("Forbidden"))
    )

    val client = makeClient()
    val ex = intercept[BillTextCheckFailed] {
      client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
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
      client.fetchTextVersions(118, "hr", "1234").unsafeRunSync()
    }
  }

}
