package com.repcheck.bills.text.download

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.bills.text.config.BillTextPipelineConfig
import com.repcheck.bills.text.errors.TextDownloadFailed

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
    maxContentBytes = 1048576L,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit] = IO.unit
    def warn(context: LogContext, message: String): IO[Unit] = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit] = IO.unit
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

  "download" should "download and strip HTML successfully" in {
    val htmlContent = "<html><body><p>Section 1.</p><p>The bill text here.</p></body></html>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(htmlContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result should not include "<html>"
    result should not include "<body>"
    result should not include "<p>"
    result should include("Section 1.")
    result should include("The bill text here.")
  }

  it should "decode HTML entities correctly" in {
    val htmlContent = "<html><body>5 &amp; 10 &lt; 20 &gt; 3 &quot;test&quot; &#167;101</body></html>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(htmlContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result should include("5 & 10")
    result should include("< 20")
    result should include("> 3")
    result should include("\"test\"")
    result should include("\u00A7101")
  }

  it should "fully strip HTML tags" in {
    val htmlContent = "<div class=\"main\"><span style=\"bold\">Bold text</span><a href=\"link\">Link</a></div>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(htmlContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result should not include "<div"
    result should not include "<span"
    result should not include "<a "
    result should include("Bold text")
    result should include("Link")
  }

  it should "preserve paragraph breaks as newlines" in {
    val htmlContent = "<p>Paragraph one.</p><p>Paragraph two.</p>"
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(htmlContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result should include("Paragraph one.")
    result should include("Paragraph two.")
    result should include("\n")
  }

  it should "download and strip XML" in {
    val xmlContent =
      """<?xml version="1.0"?>
        |<bill>
        |  <!-- This is a comment -->
        |  <section>Section text here</section>
        |  <title>Bill Title</title>
        |</bill>""".stripMargin
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(xmlContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted XML", correlationId).unsafeRunSync()
    result should not include "<?xml"
    result should not include "<bill>"
    result should not include "<!--"
    result should include("Section text here")
    result should include("Bill Title")
  }

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

  it should "raise TextDownloadFailed when content exceeds max size" in {
    val smallConfig = BillTextPipelineConfig(
      parallelism = 1,
      downloadTimeoutSeconds = 5,
      maxContentBytes = 10L,
    )
    val smallDownloader = new BillTextDownloader[IO](httpClient, smallConfig, noopLogger)

    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody("This content is longer than ten bytes")
        )
    )

    val ex = intercept[TextDownloadFailed] {
      smallDownloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    }
    ex.getMessage should include("exceeds maximum")
  }

  it should "pass through unknown format without modification" in {
    val rawContent = "Just some raw text content."
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(rawContent)
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Unknown Format", correlationId).unsafeRunSync()
    result shouldBe rawContent
  }

  it should "produce empty string for empty document" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/text"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody("")
        )
    )

    val result = downloader.download(s"$baseUrl/text", "Formatted Text", correlationId).unsafeRunSync()
    result shouldBe ""
  }

  "extractText" should "handle Formatted Text format" in {
    val html = "<p>Hello</p>"
    val result = downloader.extractText(html, "Formatted Text")
    result should include("Hello")
    result should not include "<p>"
  }

  it should "handle Formatted XML format" in {
    val xml = "<root><child>Content</child></root>"
    val result = downloader.extractText(xml, "Formatted XML")
    result should include("Content")
    result should not include "<root>"
  }

  it should "pass through PDF format" in {
    val pdfContent = "raw pdf bytes"
    val result = downloader.extractText(pdfContent, "PDF")
    result shouldBe pdfContent
  }

  it should "pass through unrecognized format" in {
    val content = "some content"
    val result = downloader.extractText(content, "Something Else")
    result shouldBe content
  }

  "stripHtml" should "convert br tags to newlines" in {
    val result = downloader.stripHtml("line1<br/>line2<br>line3")
    result should include("line1\nline2\nline3")
  }

  it should "handle nbsp entities" in {
    val result = downloader.stripHtml("word1&nbsp;word2")
    result should include("word1 word2")
  }

  "stripXml" should "remove processing instructions" in {
    val result = downloader.stripXml("<?xml version=\"1.0\"?><root>data</root>")
    result should not include "<?xml"
    result should include("data")
  }

  it should "remove XML comments" in {
    val result = downloader.stripXml("<root><!-- comment -->data</root>")
    result should not include "comment"
    result should include("data")
  }

}
