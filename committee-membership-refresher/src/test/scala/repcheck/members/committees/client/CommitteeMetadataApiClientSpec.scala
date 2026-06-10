package repcheck.members.committees.client

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
import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class CommitteeMetadataApiClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration.options().bindAddress("127.0.0.1").dynamicPort()
  )

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private val retryConfig: RetryConfig = RetryConfig(
    maxRetries = 0,
    initialBackoffMs = 10L,
    maxBackoffMs = 50L,
    backoffMultiplier = 2.0,
  )

  private def congressGovConfig: CongressGovClientConfig = CongressGovClientConfig(
    apiKey = "test-key",
    baseUrl = s"http://127.0.0.1:${wireMock.port().toString}/v3",
    pageSize = 250,
    pageDelay = 10.millis,
    retry = retryConfig,
  )

  private def makeClient(): CommitteeMetadataApiClient[IO] = {
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)
    CommitteeMetadataApiClient[IO](congressGovConfig, httpClient, retryWrapper)
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

  private val sampleResponse: String =
    """{
      |  "committees": [
      |    {
      |      "systemCode": "hsru00",
      |      "name": "Committee on Rules",
      |      "chamber": "House",
      |      "committeeTypeCode": "Standing",
      |      "updateDate": "2024-01-01",
      |      "url": "https://api.congress.gov/v3/committee/house/hsru00"
      |    },
      |    {
      |      "systemCode": "ssfi00",
      |      "name": "Committee on Finance",
      |      "chamber": "Senate",
      |      "committeeTypeCode": "Standing"
      |    }
      |  ],
      |  "pagination": {
      |    "count": 2
      |  }
      |}""".stripMargin

  "fetchPageForChamber" should "parse committee list from API response" in {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/committee/house"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(sampleResponse)
        )
    )

    val result = makeClient().fetchPageForChamber("house", FetchParams(offset = 0, pageSize = 250)).unsafeRunSync()
    val _      = result.items.size shouldBe 2
    val _      = result.totalCount shouldBe 2
    result.items.map(_.systemCode) should contain theSameElementsAs List("hsru00", "ssfi00")
  }

  it should "parse optional parent and subcommittees" in {
    val withParent =
      """{
        |  "committees": [
        |    {
        |      "systemCode": "hsru01",
        |      "name": "Subcommittee on Legislative",
        |      "chamber": "House",
        |      "parent": {"systemCode": "hsru00", "name": "Rules"}
        |    }
        |  ],
        |  "pagination": {"count": 1}
        |}""".stripMargin

    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/committee/house"))
        .willReturn(
          aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(withParent)
        )
    )

    val result = makeClient().fetchPageForChamber("house", FetchParams(offset = 0, pageSize = 250)).unsafeRunSync()
    val _      = result.items.size shouldBe 1
    result.items.headOption.foreach { item =>
      val _ = item.parent.isDefined shouldBe true
      item.parent.flatMap(_.systemCode) shouldBe Some("hsru00")
    }
  }

  it should "raise CommitteeApiHttpError on non-2xx response" in {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/committee/senate"))
        .willReturn(
          aResponse().withStatus(500).withBody("Internal Server Error")
        )
    )

    val thrown = intercept[Exception] {
      makeClient().fetchPageForChamber("senate", FetchParams(offset = 0, pageSize = 250)).unsafeRunSync()
    }
    thrown.toString should not be empty
  }

  it should "calculate nextOffset when page is full" in {
    val fullPage = (1 to 250)
      .map(i => s"""{"systemCode": "hs${"%04d".format(i)}", "name": "Committee $i", "chamber": "House"}""")
      .mkString(",")
    val response = s"""{"committees": [$fullPage], "pagination": {"count": 500}}"""

    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/v3/committee/house"))
        .willReturn(
          aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(response)
        )
    )

    val result = makeClient().fetchPageForChamber("house", FetchParams(offset = 0, pageSize = 250)).unsafeRunSync()
    val _      = result.items.size shouldBe 250
    result.nextOffset shouldBe Some(250)
  }

}
