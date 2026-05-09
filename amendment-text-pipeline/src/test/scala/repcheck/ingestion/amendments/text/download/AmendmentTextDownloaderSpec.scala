package repcheck.ingestion.amendments.text.download

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
import repcheck.ingestion.amendments.text.errors.{
  AmendmentTextDownloadFailed,
  AmendmentTextDownloadHttpError,
  InvalidAmendmentTextUrl,
}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Specs for the streaming [[AmendmentTextDownloader]]. Mirror of [[BillTextDownloaderSpec]] for the CREC URL pattern; a
 * WireMock server bound to `127.0.0.1` with a dynamic port serves canned responses.
 */
class AmendmentTextDownloaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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

  private val FakeApiKey = "test-api-key"

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
    EmberClientBuilder
      .default[IO]
      .build
      .map { client =>
        new AmendmentTextDownloader[IO](
          client = client,
          govInfoApiKey = FakeApiKey,
          govInfoBaseUrl = wireMock.baseUrl(),
          logger = noopLogger,
        )
      }

  private def streamBodyAsString(downloader: AmendmentTextDownloader[IO], url: String, format: String): IO[String] =
    downloader
      .streamBody(url, format, correlationId)
      .through(fs2.text.utf8.decode)
      .compile
      .string

  "streamBody" should "emit the full response body as a Stream[F, Byte] for a successful response" in {
    val expectedBody = "Amendment text — Submitted version"
    wireMock.stubFor(
      get(urlPathEqualTo("/amendment"))
        .willReturn(aResponse().withStatus(200).withBody(expectedBody))
    )

    val program =
      downloaderResource.use(downloader => streamBodyAsString(downloader, s"${wireMock.baseUrl()}/amendment", "HTML"))
    program.unsafeRunSync() shouldBe expectedBody
  }

  it should "raise AmendmentTextDownloadFailed on HTTP 404" in {
    wireMock.stubFor(get(urlPathEqualTo("/amendment")).willReturn(aResponse().withStatus(404)))

    val attempt = downloaderResource
      .use(downloader => streamBodyAsString(downloader, s"${wireMock.baseUrl()}/amendment", "HTML"))
      .attempt
      .unsafeRunSync()

    attempt match {
      case Left(_: AmendmentTextDownloadFailed) => succeed
      case other                                => fail(s"Expected AmendmentTextDownloadFailed, got $other")
    }
  }

  it should "raise AmendmentTextDownloadHttpError (typed, status-aware) on non-404 non-success status" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/amendment")).willReturn(aResponse().withStatus(500).withBody("server crashed"))
    )

    val attempt = downloaderResource
      .use(d => streamBodyAsString(d, s"${wireMock.baseUrl()}/amendment", "HTML"))
      .attempt
      .unsafeRunSync()

    attempt match {
      case Left(err: AmendmentTextDownloadHttpError) =>
        val _ = err.statusCode shouldBe 500
        err.body should include("server crashed")
      case other => fail(s"Expected AmendmentTextDownloadHttpError, got $other")
    }
  }

  it should "cap the error response body at MaxErrorBodyBytes (~2 KiB) so a multi-MB error can't OOM" in {
    val giantBody = "X".repeat(64 * 1024) // 64 KiB; well over the 2 KiB cap
    wireMock.stubFor(
      get(urlPathEqualTo("/amendment")).willReturn(aResponse().withStatus(502).withBody(giantBody))
    )

    val attempt = downloaderResource
      .use(d => streamBodyAsString(d, s"${wireMock.baseUrl()}/amendment", "HTML"))
      .attempt
      .unsafeRunSync()

    attempt match {
      case Left(err: AmendmentTextDownloadHttpError) =>
        val _ = err.statusCode shouldBe 502
        // The cap is at the byte layer; the captured body must be far below the 64 KiB sent body and bounded by
        // a small constant — proves the body was truncated rather than slurped whole.
        err.body.length should be <= 4096
      case other => fail(s"Expected AmendmentTextDownloadHttpError, got $other")
    }
  }

  it should "stream a body of arbitrary size without imposing a cap" in {
    val largeBody = "X".repeat(512 * 1024)
    wireMock.stubFor(
      get(urlPathEqualTo("/amendment")).willReturn(aResponse().withStatus(200).withBody(largeBody))
    )

    val program = downloaderResource.use { d =>
      d.streamBody(s"${wireMock.baseUrl()}/amendment", "HTML", correlationId)
        .compile
        .toVector
        .map(_.length)
    }
    program.unsafeRunSync() shouldBe largeBody.getBytes(StandardCharsets.UTF_8).length
  }

  it should "rewrite a Congress.gov CREC HTM URL to api.govinfo.gov + append api_key" in {
    val expectedBody = "Amendment text from CREC GovInfo"
    // The rewriter maps the input URL to `${baseUrl}/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm`.
    wireMock.stubFor(
      get(urlPathEqualTo("/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"))
        .withQueryParam("api_key", equalTo(FakeApiKey))
        .willReturn(aResponse().withStatus(200).withBody(expectedBody))
    )

    val congressGovUrl =
      "https://www.congress.gov/117/crec/2021/08/01/167/136/modified/CREC-2021-08-01-pt1-PgS5255.htm"
    val program = downloaderResource.use(d => streamBodyAsString(d, congressGovUrl, "HTML"))

    val _ = program.unsafeRunSync() shouldBe expectedBody
    wireMock.verify(
      getRequestedFor(urlPathEqualTo("/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"))
        .withQueryParam("api_key", equalTo(FakeApiKey))
    )
  }

  it should "rewrite a Congress.gov CREC PDF URL with the pdf format suffix" in {
    wireMock.stubFor(
      get(urlPathEqualTo("/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/pdf"))
        .withQueryParam("api_key", equalTo(FakeApiKey))
        .willReturn(aResponse().withStatus(200).withBody("%PDF-1.4 stub"))
    )

    val pdfBody = downloaderResource
      .use(d =>
        streamBodyAsString(
          d,
          "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.pdf",
          "PDF",
        )
      )
      .unsafeRunSync()

    pdfBody shouldBe "%PDF-1.4 stub"
  }

  it should "fall through to the original URL when the rewriter doesn't match" in {
    // A non-CREC URL — the downloader uses it as-is. We point at WireMock so the test passes; in production the
    // long-tail URL would go to www.congress.gov and may trip Cloudflare.
    val nonCrecUrl = s"${wireMock.baseUrl()}/long-tail-amendment"
    wireMock.stubFor(
      get(urlPathEqualTo("/long-tail-amendment"))
        .willReturn(aResponse().withStatus(200).withBody("long-tail body"))
    )

    val body = downloaderResource
      .use(d => streamBodyAsString(d, nonCrecUrl, "HTML"))
      .unsafeRunSync()
    body shouldBe "long-tail body"
  }

  "parseUrl" should "raise InvalidAmendmentTextUrl for malformed URLs" in {
    val program = downloaderResource.use(d => d.parseUrl("not a url at all").attempt)
    val attempt = program.unsafeRunSync()
    attempt match {
      case Left(_: InvalidAmendmentTextUrl) => succeed
      case other                            => fail(s"Expected InvalidAmendmentTextUrl, got $other")
    }
  }

  it should "parse a valid URL successfully" in {
    val program = downloaderResource.use(d => d.parseUrl("https://example.com/text"))
    program.unsafeRunSync().toString shouldBe "https://example.com/text"
  }

  "previewDownloadUrl" should "return the rewritten api.govinfo.gov URL when input matches the CREC pattern" in {
    downloaderResource
      .use { d =>
        IO {
          val preview =
            d.previewDownloadUrl(
              "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm"
            )
          val _ = preview shouldBe Some(
            s"${wireMock.baseUrl()}/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"
          )
          val _ = preview.exists(_.contains("api_key")) shouldBe false
          succeed
        }
      }
      .unsafeRunSync()
  }

  it should "return None for non-CREC URLs (the rewriter falls through to the original URL at request time)" in {
    downloaderResource
      .use(d => IO(d.previewDownloadUrl("https://example.com/long-tail.htm") shouldBe None))
      .unsafeRunSync()
  }

  "buildRequest" should "rewrite when the URL matches the CREC pattern" in {
    val program = downloaderResource.use { d =>
      d.buildRequest("https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm")
    }
    val (request, effectiveUrl) = program.unsafeRunSync()
    val _ = effectiveUrl should include("/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm")
    val _ = request.uri.toString should include("api_key=test-api-key")
    // Effective URL is redacted (api_key not embedded) for log safety.
    effectiveUrl should not include "api_key"
  }

  it should "use the original URL when the pattern doesn't match" in {
    val program                 = downloaderResource.use(d => d.buildRequest("https://example.com/some-amendment.txt"))
    val (request, effectiveUrl) = program.unsafeRunSync()
    val _                       = effectiveUrl shouldBe "https://example.com/some-amendment.txt"
    request.uri.toString should not include "api_key"
  }

  it should "redact query params on the passthrough branch so an inbound api_key never lands in logs" in {
    // Inbound URLs from §7.5 events can already carry query params (the rewriter regex tolerates them);
    // if the URL doesn't match the rewriter pattern we still pass the inbound URL into the GET, but
    // the *effective URL* returned for logging must drop the query string entirely.
    val program = downloaderResource.use(d =>
      d.buildRequest("https://example.com/some-amendment.txt?api_key=SECRET&token=ALSO-SECRET")
    )
    val (request, effectiveUrl) = program.unsafeRunSync()
    val _                       = effectiveUrl shouldBe "https://example.com/some-amendment.txt"
    val _                       = effectiveUrl should not include "api_key"
    val _                       = effectiveUrl should not include "SECRET"
    // The actual GET still uses the inbound URL verbatim — caller controls what they hand us.
    request.uri.toString should include("api_key=SECRET")
  }

  "redactQueryParams" should "drop api_key from a URL that already carries one" in {
    AmendmentTextDownloader.redactQueryParams(
      "https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm?api_key=SECRET"
    ) shouldBe
      "https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"
  }

  it should "drop the entire query string (not just api_key) so future query-param secrets are also redacted" in {
    AmendmentTextDownloader.redactQueryParams(
      "https://api.govinfo.gov/x?api_key=SECRET&token=SECRET2&debug=true"
    ) shouldBe "https://api.govinfo.gov/x"
  }

  it should "leave a URL with no query string unchanged" in {
    val url = "https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm"
    AmendmentTextDownloader.redactQueryParams(url) shouldBe url
  }

  it should "fall back to the original input when the URL is unparseable" in {
    val malformed = "not a url at all :: %%%"
    AmendmentTextDownloader.redactQueryParams(malformed) shouldBe malformed
  }

}
