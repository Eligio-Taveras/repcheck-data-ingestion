package repcheck.ingestion.bills.text.download

import java.nio.charset.StandardCharsets
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.bills.text.errors.{InvalidTextUrl, TextDownloadFailed}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Specs for the streaming [[BillTextDownloader]]. Phase 3 of the bill-text-10mb plan: the downloader's API narrowed to
 * `streamBody` returning `Stream[F, Byte]`. Format-specific extractors (HTML, XML, plaintext, PDF) consume that byte
 * stream directly via their respective parsers.
 *
 * Test pattern: a WireMock server bound to `127.0.0.1` with a dynamic port (per memory `feedback_wiremock_localhost`)
 * serves canned responses; tests assert the downloader's behaviour against the streamed bytes.
 */
class BillTextDownloaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
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

  private def downloaderResource =
    EmberClientBuilder.default[IO].build.map(client => new BillTextDownloader[IO](client, noopLogger))

  private def streamBodyAsString(downloader: BillTextDownloader[IO], url: String, format: String): IO[String] =
    downloader
      .streamBody(url, format, correlationId)
      .through(fs2.text.utf8.decode)
      .compile
      .string

  "streamBody" should "emit the full response body as a Stream[F, Byte] for a successful response" in {
    val expectedBody = "Section 1. Title — first sentence."
    wireMock.stubFor(
      get(urlPathEqualTo("/bill"))
        .willReturn(aResponse().withStatus(200).withBody(expectedBody))
    )

    val program = downloaderResource.use(downloader =>
      streamBodyAsString(downloader, s"${wireMock.baseUrl()}/bill", "Formatted Text")
    )
    program.unsafeRunSync() shouldBe expectedBody
  }

  it should "raise TextDownloadFailed on HTTP 404" in {
    wireMock.stubFor(get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(404)))

    val attempt = downloaderResource
      .use(downloader => streamBodyAsString(downloader, s"${wireMock.baseUrl()}/bill", "Formatted Text"))
      .attempt
      .unsafeRunSync()

    attempt match {
      case Left(_: TextDownloadFailed) => succeed
      case other                       => fail(s"Expected TextDownloadFailed, got $other")
    }
  }

  it should "raise TextDownloadFailed on non-success status with body text included for debugging" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(500).withBody("server crashed"))
    )

    val attempt = downloaderResource
      .use(downloader => streamBodyAsString(downloader, s"${wireMock.baseUrl()}/bill", "Formatted Text"))
      .attempt
      .unsafeRunSync()

    attempt match {
      case Left(err: TextDownloadFailed) =>
        val _ = err.getMessage should include("500")
        err.getMessage should include("server crashed")
      case other => fail(s"Expected TextDownloadFailed, got $other")
    }
  }

  it should "stream a body of arbitrary size without imposing a cap" in {
    // Sanity check that no body-size cap applies. 1 MiB body would have tripped the prior 10 MiB
    // cap (since removed in PR #81). With no cap, all bytes flow through unchanged.
    val largeBody = "X".repeat(1024 * 1024)
    wireMock.stubFor(
      get(urlPathEqualTo("/bill")).willReturn(aResponse().withStatus(200).withBody(largeBody))
    )

    val program = downloaderResource.use { downloader =>
      downloader
        .streamBody(s"${wireMock.baseUrl()}/bill", "Formatted Text", correlationId)
        .compile
        .toVector
        .map(_.length)
    }
    program.unsafeRunSync() shouldBe largeBody.getBytes(StandardCharsets.UTF_8).length
  }

  "parseUrl" should "raise InvalidTextUrl for malformed URLs" in {
    val program = downloaderResource.use(downloader => downloader.parseUrl("not a url at all").attempt)
    val attempt = program.unsafeRunSync()
    attempt match {
      case Left(_: InvalidTextUrl) => succeed
      case other                   => fail(s"Expected InvalidTextUrl, got $other")
    }
  }

  it should "parse a valid URL successfully" in {
    val program = downloaderResource.use(downloader => downloader.parseUrl("https://example.com/text"))
    program.unsafeRunSync().toString shouldBe "https://example.com/text"
  }

}
