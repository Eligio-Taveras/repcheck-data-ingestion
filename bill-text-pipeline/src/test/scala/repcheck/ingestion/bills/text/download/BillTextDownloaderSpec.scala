package repcheck.ingestion.bills.text.download

import java.util.UUID

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
import repcheck.ingestion.bills.text.config.BillTextPipelineConfig
import repcheck.ingestion.bills.text.errors.{InvalidTextUrl, TextContentTooLarge, TextDownloadFailed}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

class BillTextDownloaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private val testConfig = BillTextPipelineConfig(
    parallelism = 1,
    downloadTimeoutSeconds = 5,
    maxContentBytes = 10485760L,
    pageDelay = 100.millis,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private val correlationId = UUID.randomUUID()

  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()
    ._1

  private lazy val downloader = new BillTextDownloader[IO](httpClient, testConfig, noopLogger)

  private def baseUrl: String = s"http://127.0.0.1:${wireMock.port()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    wireMock.resetAll()
  }

  // --- HTML parsing (Congress.gov "Formatted Text" format) ---

  "download" should "extract bill text from HTML pre tag" in {
    val htmlContent =
      """<html><body><pre>
        |[Congressional Bills 118th Congress]
        |
        |SECTION 1. SHORT TITLE.
        |
        |    This Act may be cited as the ``Test Act''.
        |</pre></body></html>""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(htmlContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    val _      = result should include("SECTION 1. SHORT TITLE.")
    val _      = result should include("Test Act")
    val _      = result should not include "<html>"
    result should not include "<pre>"
  }

  it should "decode HTML entities in bill text" in {
    val htmlContent = "<html><body><pre>5 &amp; 10 &lt; 20 &gt; 3 &#167;101</pre></body></html>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(htmlContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    val _      = result should include("5 & 10")
    val _      = result should include("< 20")
    val _      = result should include("> 3")
    result should include("\u00A7101")
  }

  it should "fall back to body text when no pre tag exists in HTML" in {
    val htmlContent = "<html><body><div>Some bill content here</div></body></html>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(htmlContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result should include("Some bill content here")
  }

  // --- XML parsing (Congress.gov "Formatted XML" / USLM format) ---

  it should "extract text from legis-body in XML bill" in {
    val xmlContent =
      """<?xml version="1.0"?>
        |<bill>
        |  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
        |    <dublinCore>
        |      <dc:title>118 HR 1 IH: Test Act</dc:title>
        |      <dc:publisher>U.S. House of Representatives</dc:publisher>
        |    </dublinCore>
        |  </metadata>
        |  <form>
        |    <congress>118th CONGRESS</congress>
        |    <session>1st Session</session>
        |    <legis-num>H. R. 1</legis-num>
        |  </form>
        |  <legis-body>
        |    <section>
        |      <enum>1.</enum>
        |      <header>SHORT TITLE</header>
        |      <text>This Act may be cited as the Test Act.</text>
        |    </section>
        |    <section>
        |      <enum>2.</enum>
        |      <header>FINDINGS</header>
        |      <text>Congress finds the following important provisions.</text>
        |    </section>
        |  </legis-body>
        |</bill>""".stripMargin

    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(xmlContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted XML", correlationId).unsafeRunSync()
    val _      = result should include("SHORT TITLE")
    val _      = result should include("This Act may be cited as the Test Act.")
    val _      = result should include("FINDINGS")
    val _      = result should include("Congress finds the following important provisions.")
    // Should NOT include metadata or form content
    val _ = result should not include "U.S. House of Representatives"
    result should not include "118th CONGRESS"
  }

  it should "fall back to full text when no legis-body exists in XML" in {
    val xmlContent = "<root><section>Fallback content here</section></root>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(xmlContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted XML", correlationId).unsafeRunSync()
    result should include("Fallback content here")
  }

  // --- Error handling ---

  it should "raise TextDownloadFailed on HTTP 404" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(404).withBody("Not Found"))
    )

    val ex = intercept[TextDownloadFailed] {
      downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    }
    ex.getMessage should include("404")
  }

  it should "raise TextDownloadFailed on HTTP 500" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(500).withBody("Internal Server Error"))
    )

    val ex = intercept[TextDownloadFailed] {
      downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    }
    ex.getMessage should include("500")
  }

  it should "raise TextContentTooLarge when content exceeds max size" in {
    val smallConfig = BillTextPipelineConfig(
      parallelism = 1,
      downloadTimeoutSeconds = 5,
      maxContentBytes = 10L,
      pageDelay = 100.millis,
    )
    val smallDownloader = new BillTextDownloader[IO](httpClient, smallConfig, noopLogger)

    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody("This content is longer than ten bytes"))
    )

    val ex = intercept[TextContentTooLarge] {
      smallDownloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    }
    val _ = ex.getMessage should include("exceeding maximum")
    ex.maxBytes shouldBe 10L
  }

  it should "raise InvalidTextUrl for malformed URL" in {
    val ex = intercept[InvalidTextUrl] {
      downloader.download("not a valid url %%%", "Formatted Text", correlationId).unsafeRunSync()
    }
    ex.getMessage should include("not a valid url")
  }

  // --- Pass-through formats ---

  it should "pass through PDF format without modification" in {
    val rawContent = "raw pdf bytes"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(rawContent))
    )

    val result = downloader.download(s"$baseUrl/text", "PDF", correlationId).unsafeRunSync()
    result shouldBe rawContent
  }

  it should "pass through unknown format without modification" in {
    val rawContent = "Just some raw text content."
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(aResponse().withStatus(200).withBody(rawContent))
    )

    val result = downloader.download(s"$baseUrl/text", "Unknown Format", correlationId).unsafeRunSync()
    result shouldBe rawContent
  }

  // --- Unit tests for extraction methods ---

  "extractHtmlText" should "extract text from pre tag" in {
    val html   = "<html><body><pre>Section 1. The bill text.</pre></body></html>"
    val result = downloader.extractHtmlText(html)
    val _      = result should include("Section 1.")
    result should not include "<pre>"
  }

  it should "fall back to body text when no pre tag" in {
    val html   = "<html><body><div>Content here</div></body></html>"
    val result = downloader.extractHtmlText(html)
    result should include("Content here")
  }

  "extractXmlText" should "extract legis-body content" in {
    val xml =
      """<bill>
        |  <metadata><dc:title xmlns:dc="x">Title</dc:title></metadata>
        |  <legis-body><section><text>Bill content.</text></section></legis-body>
        |</bill>""".stripMargin
    val result = downloader.extractXmlText(xml)
    val _      = result should include("Bill content.")
    result should not include "Title"
  }

  it should "fall back to full text when no legis-body" in {
    val xml    = "<root><data>Fallback</data></root>"
    val result = downloader.extractXmlText(xml)
    result should include("Fallback")
  }

}
