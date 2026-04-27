package repcheck.ingestion.bills.text.download

import java.nio.charset.StandardCharsets
import java.nio.file.Files
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
import repcheck.ingestion.bills.text.errors.{InvalidTextUrl, TextDownloadFailed}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Specs for the streaming-to-temp-file [[BillTextDownloader]]. Phase 2 of the bill-text-10mb plan: the downloader's
 * responsibility narrowed to "stream HTTP body to a `Resource[F, Path]`"; HTML/XML/PDF extraction moved to
 * [[repcheck.ingestion.bills.text.extraction.BillTextExtractor]] (covered separately in `BillTextExtractorSpec`).
 *
 * Test pattern is unchanged from the pre-Phase-2 spec: a WireMock server bound to `127.0.0.1` with a dynamic port (per
 * memory `feedback_wiremock_localhost`) serves canned responses; tests assert the downloader's behaviour against the
 * streamed bytes and the temp-file lifecycle.
 */
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
    pageDelay = 100.millis,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    wireMock.resetAll()
    super.beforeEach()
  }

  private val correlationId = UUID.randomUUID()

  private def downloaderResource(config: BillTextPipelineConfig = testConfig) =
    EmberClientBuilder.default[IO].build.map(client => new BillTextDownloader[IO](client, config, noopLogger))

  "downloadToTempFile" should "stream a successful response into a temp file and yield its path" in {
    val expectedBody = "Section 1. Title — first sentence."
    wireMock.stubFor(
      get(urlPathEqualTo("/bill"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withBody(expectedBody)
        )
    )

    val program = downloaderResource().use { downloader =>
      downloader
        .downloadToTempFile(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
        .use(path => IO(Files.readString(path, StandardCharsets.UTF_8)))
    }

    program.unsafeRunSync() shouldBe expectedBody
  }

  it should "delete the temp file when the Resource is released" in {
    wireMock.stubFor(get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(200).withBody("body")))

    val program = downloaderResource().use { downloader =>
      downloader
        .downloadToTempFile(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
        .use(path => IO.pure(path))
        .map(path => Files.exists(path))
    }

    program.unsafeRunSync() shouldBe false
  }

  it should "raise TextDownloadFailed on HTTP 404" in {
    wireMock.stubFor(get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(404)))

    val attempt = downloaderResource()
      .use { downloader =>
        downloader
          .downloadToTempFile(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
          .use(_ => IO.unit)
          .attempt
      }
      .unsafeRunSync()

    attempt match {
      case Left(_: TextDownloadFailed) => succeed
      case other                       => fail(s"Expected TextDownloadFailed, got $other")
    }
  }

  it should "raise TextDownloadFailed on non-success status with body included" in {
    wireMock.stubFor(get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(500).withBody("server crashed")))

    val attempt = downloaderResource()
      .use { downloader =>
        downloader
          .downloadToTempFile(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
          .use(_ => IO.unit)
          .attempt
      }
      .unsafeRunSync()

    attempt match {
      case Left(err: TextDownloadFailed) =>
        val _ = err.getMessage should include("500")
        err.getMessage should include("server crashed")
      case other => fail(s"Expected TextDownloadFailed, got $other")
    }
  }

  "parseUrl" should "raise InvalidTextUrl for malformed URLs" in {
    val program = downloaderResource().use(downloader => downloader.parseUrl("not a url at all").attempt)
    val attempt = program.unsafeRunSync()
    attempt match {
      case Left(_: InvalidTextUrl) => succeed
      case other                   => fail(s"Expected InvalidTextUrl, got $other")
    }
  }

  it should "parse a valid URL successfully" in {
    val program = downloaderResource().use(downloader => downloader.parseUrl("https://example.com/text"))
    program.unsafeRunSync().toString shouldBe "https://example.com/text"
  }

  it should "stream a body of arbitrary size to disk without imposing a configured cap" in {
    // Sanity check that the no-cap design works for bodies larger than the previous (now-removed)
    // maxContentBytes default. 1 MiB body exceeds the prior 10 MiB / 200 MiB cap noise; it would
    // have tripped any in-pipe size check. With no cap, all bytes flow through to the temp file.
    val largeBody = "X".repeat(1024 * 1024)
    wireMock.stubFor(
      get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(200).withBody(largeBody))
    )

    val program = downloaderResource().use { downloader =>
      downloader
        .downloadToTempFile(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
        .use(path => IO.delay(Files.size(path)))
    }
    program.unsafeRunSync() shouldBe largeBody.length.toLong
  }

}
