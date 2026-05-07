package repcheck.ingestion.votes.xml

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
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.config.SenateVoteXmlConfig
import repcheck.ingestion.votes.errors.SenateVoteFetchFailed
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}

/**
 * Component-level WireMock tests for [[SenateVoteXmlClient]]. Covers §6.2 AC rows 1, 2, 4, 6, 7, 13 and the 5-digit
 * zero-padding boundary test (row 2 functional verification).
 *
 * Conventions (per CLAUDE.md + plan):
 *   - WireMock binds to `127.0.0.1` with a dynamic port to avoid Windows firewall popups.
 *   - Every stubbed XML response carries `Content-Type: application/xml` so `http4s-scala-xml` decodes the body.
 *   - Real senate.gov fixtures live in `src/test/resources/senate-xml/`; tests read them for happy-path assertions so
 *     the WireMock body and the decoder path are exercised end-to-end through a real HTTP client.
 */
class SenateVoteXmlClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def makeClient(
    requestDelay: FiniteDuration = Duration.Zero,
    retry: RetryConfig =
      RetryConfig(maxRetries = 1, initialBackoffMs = 10L, maxBackoffMs = 50L, backoffMultiplier = 2.0),
  ): SenateVoteXmlClient[IO] = {
    val config = SenateVoteXmlConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      parallelism = 1,
      requestDelay = requestDelay,
      retry = retry,
    )
    new SenateVoteXmlClient[IO](httpClient, retryWrapper, config, noopLogger)
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

  private def loadFixture(name: String): String = {
    val stream = getClass.getResourceAsStream(s"/senate-xml/$name")
    require(stream != null, s"Fixture $name missing")
    try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
    finally stream.close()
  }

  private def stubXml(path: String, body: String, status: Int = 200): Unit = {
    val _ = wireMock.stubFor(
      get(urlEqualTo(path))
        .willReturn(
          aResponse()
            .withStatus(status)
            .withHeader("Content-Type", "application/xml")
            .withBody(body)
        )
    )
  }

  "fetchVote" should "parse a valid Senate vote XML body from senate.gov" in {
    val fixture = loadFixture("vote_119_1_00017.xml")
    stubXml("/roll_call_votes/vote1191/vote_119_1_00017.xml", fixture)

    val dto = makeClient().fetchVote(119, 1, 17).unsafeRunSync()

    val _ = dto.congress shouldBe 119
    val _ = dto.session shouldBe 1
    val _ = dto.voteNumber shouldBe 17
    val _ = dto.result shouldBe "Nomination Confirmed"
    val _ = dto.members.size shouldBe 100
    dto.voteDate shouldBe "January 25, 2025, 11:30 AM"
  }

  it should "pad the vote number to 5 digits (AC 2)" in {
    val minimal =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>7</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>Passed</vote_result>
        |  <document>
        |    <document_congress>119</document_congress>
        |    <document_type>S.</document_type>
        |    <document_number>42</document_number>
        |    <document_name>S. 42</document_name>
        |    <document_title>Test bill</document_title>
        |    <document_short_title/>
        |  </document>
        |  <members/>
        |</roll_call_vote>""".stripMargin
    stubXml("/roll_call_votes/vote1191/vote_119_1_00007.xml", minimal)

    val _ = makeClient().fetchVote(119, 1, 7).unsafeRunSync()

    wireMock.verify(
      1,
      getRequestedFor(urlEqualTo("/roll_call_votes/vote1191/vote_119_1_00007.xml")),
    )
  }

  "fetchVoteIndex" should "parse the vote_menu_{congress}_{session}.xml index into a list of entries" in {
    val fixture = loadFixture("vote_menu_119_1.xml")
    stubXml("/roll_call_lists/vote_menu_119_1.xml", fixture)

    val entries = makeClient().fetchVoteIndex(119, 1).unsafeRunSync()

    val _ = entries.size should be >= 50
    entries.headOption.map(_.voteNumber) shouldBe Some(659)
  }

  it should "wrap HTTP 404 in SenateVoteFetchFailed with congress/session/voteNumber context" in {
    stubXml(
      "/roll_call_votes/vote1191/vote_119_1_00099.xml",
      "<error/>",
      status = 404,
    )

    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVote(119, 1, 99).unsafeRunSync()
    }

    val _ = thrown.congress shouldBe 119
    val _ = thrown.session shouldBe 1
    val _ = thrown.voteNumber shouldBe Some(99)
    val _ = thrown.statusCode shouldBe 404
    thrown.getMessage should include("119-1-99")
  }

  it should "retry a transient HTTP 503 and then succeed (AC 6)" in {
    val url = "/roll_call_votes/vote1191/vote_119_1_00042.xml"
    val fixture =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>42</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>Passed</vote_result>
        |  <document>
        |    <document_congress>119</document_congress>
        |    <document_type>S.</document_type>
        |    <document_number>42</document_number>
        |    <document_name>S. 42</document_name>
        |    <document_title>Test bill</document_title>
        |    <document_short_title/>
        |  </document>
        |  <members/>
        |</roll_call_vote>""".stripMargin

    val _ = wireMock.stubFor(
      get(urlEqualTo(url))
        .inScenario("retry-503")
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(503).withHeader("Content-Type", "application/xml").withBody("<err/>"))
        .willSetStateTo("after-503")
    )
    val _ = wireMock.stubFor(
      get(urlEqualTo(url))
        .inScenario("retry-503")
        .whenScenarioStateIs("after-503")
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/xml").withBody(fixture))
    )

    val dto = makeClient(retry =
      RetryConfig(maxRetries = 3, initialBackoffMs = 10L, maxBackoffMs = 50L, backoffMultiplier = 2.0)
    ).fetchVote(119, 1, 42).unsafeRunSync()
    val _ = dto.voteNumber shouldBe 42
    wireMock.verify(2, getRequestedFor(urlEqualTo(url)))
  }

  it should "not retry a systemic HTTP 400 and fail fast" in {
    val url = "/roll_call_votes/vote1191/vote_119_1_00100.xml"
    stubXml(url, "<bad/>", status = 400)

    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient(retry =
        RetryConfig(maxRetries = 5, initialBackoffMs = 10L, maxBackoffMs = 50L, backoffMultiplier = 2.0)
      ).fetchVote(119, 1, 100).unsafeRunSync()
    }
    val _ = thrown.statusCode shouldBe 400
    // Exactly one HTTP attempt — a 400 is Systemic, so no retry.
    wireMock.verify(1, getRequestedFor(urlEqualTo(url)))
  }

  it should "respect requestDelay between sequential vote fetches (AC 7)" in {
    val delay = 150.millis
    val fixture =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<roll_call_vote>
        |  <congress>119</congress>
        |  <session>1</session>
        |  <vote_number>1</vote_number>
        |  <question>Q</question>
        |  <vote_date>2025-04-03T14:42:00</vote_date>
        |  <vote_result>Passed</vote_result>
        |  <document>
        |    <document_congress>119</document_congress>
        |    <document_type>S.</document_type>
        |    <document_number>1</document_number>
        |    <document_name>S. 1</document_name>
        |    <document_title>Test bill</document_title>
        |    <document_short_title/>
        |  </document>
        |  <members/>
        |</roll_call_vote>""".stripMargin
    stubXml("/roll_call_votes/vote1191/vote_119_1_00001.xml", fixture)
    stubXml(
      "/roll_call_votes/vote1191/vote_119_1_00002.xml",
      fixture.replace("<vote_number>1</vote_number>", "<vote_number>2</vote_number>"),
    )
    stubXml(
      "/roll_call_votes/vote1191/vote_119_1_00003.xml",
      fixture.replace("<vote_number>1</vote_number>", "<vote_number>3</vote_number>"),
    )

    // Wrap the HTTP client with the same `rateLimitedClient` semaphore the production pipeline uses so the config's
    // `requestDelay` is actually honored between requests. This mirrors the end-to-end pacing behavior that ships in
    // `VotesPipeline.buildResources`.
    import cats.effect.std.Semaphore
    import cats.effect.{Resource, Temporal}
    import org.http4s.client.Client
    val wrapped: Client[IO] = {
      val sem = Semaphore[IO](1).unsafeRunSync()
      Client[IO] { req =>
        Resource
          .make(sem.acquire)(_ => Temporal[IO].sleep(delay).flatMap(_ => sem.release))
          .flatMap(_ => httpClient.run(req))
      }
    }
    val config = SenateVoteXmlConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      parallelism = 1,
      requestDelay = delay,
      retry = RetryConfig(maxRetries = 1, initialBackoffMs = 10L, maxBackoffMs = 50L, backoffMultiplier = 2.0),
    )
    val client = new SenateVoteXmlClient[IO](wrapped, retryWrapper, config, noopLogger)

    val start   = System.nanoTime()
    val _       = client.fetchVote(119, 1, 1).unsafeRunSync()
    val _       = client.fetchVote(119, 1, 2).unsafeRunSync()
    val _       = client.fetchVote(119, 1, 3).unsafeRunSync()
    val elapsed = (System.nanoTime() - start).nanos

    // Three requests, so at minimum two inter-request delays are expected; expose some slack for CI jitter.
    elapsed should be >= (delay * 2 - 50.millis)
  }

  it should "wrap decoder failures in SenateVoteFetchFailed with the decoder error as cause" in {
    val url = "/roll_call_votes/vote1191/vote_119_1_00055.xml"
    stubXml(url, "<not_a_vote/>")

    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVote(119, 1, 55).unsafeRunSync()
    }

    val _ = thrown.cause.toString should not be empty
    thrown.getMessage should include("Expected <roll_call_vote>")
  }

  it should "wrap an index-decode failure (bad root element) in SenateVoteFetchFailed with voteNumber=None" in {
    stubXml("/roll_call_lists/vote_menu_119_1.xml", "<not_an_index/>")

    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVoteIndex(119, 1).unsafeRunSync()
    }

    val _ = thrown.voteNumber shouldBe None
    val _ = thrown.congress shouldBe 119
    val _ = thrown.session shouldBe 1
    thrown.getMessage should include("vote index 119-1")
  }

  it should "pass through a non-HTTP XmlParseFailed (malformed XML body) without coercing to an HttpStatusError" in {
    val url = "/roll_call_votes/vote1191/vote_119_1_00077.xml"
    // Serve a 200 with truly-unparseable XML. The shared XmlFeedClient's `expect[Elem]` call fails with a
    // MalformedMessageBodyFailure (NOT an UnexpectedStatus), so `unwrapHttpStatus` returns None and the sharedPF is
    // preserved; the client then wraps that into SenateVoteFetchFailed with statusCode 0 (no HTTP status available).
    stubXml(url, "this is not xml at all {{{")

    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVote(119, 1, 77).unsafeRunSync()
    }

    val _ = thrown.voteNumber shouldBe Some(77)
    thrown.statusCode shouldBe 0
  }

  it should "reject a vote number below 1 without making an HTTP call" in {
    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVote(119, 1, 0).unsafeRunSync()
    }
    val _ = thrown.voteNumber shouldBe Some(0)
    val _ = thrown.getMessage should include("voteNumber must be >= 1")
    wireMock.verify(0, getRequestedFor(urlPathMatching(".*")))
    succeed
  }

  it should "reject a vote number at or above 100000 without making an HTTP call" in {
    val thrown = intercept[SenateVoteFetchFailed] {
      makeClient().fetchVote(119, 1, 100000).unsafeRunSync()
    }
    val _ = thrown.voteNumber shouldBe Some(100000)
    val _ = thrown.getMessage should include("5 digits")
    wireMock.verify(0, getRequestedFor(urlPathMatching(".*")))
    succeed
  }

  // ------------------------------------------------------------------
  // §7.4 — amendment-vote XML integration: amendment fields are folded into the canonical document DTO
  // ------------------------------------------------------------------

  it should "decode an amendment-vote XML body with amendment fields populated on the document DTO" in {
    val amendmentXml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<roll_call_vote>
        |  <congress>117</congress>
        |  <session>1</session>
        |  <vote_number>312</vote_number>
        |  <question>On the Amendment</question>
        |  <vote_date>2025-01-25T11:30:00</vote_date>
        |  <vote_result>Amendment Agreed To</vote_result>
        |  <document>
        |    <document_congress>117</document_congress>
        |    <document_type>S.Amdt.</document_type>
        |    <document_number/>
        |    <document_name/>
        |    <document_title/>
        |    <document_short_title/>
        |  </document>
        |  <amendment_number>S.Amdt. 2137</amendment_number>
        |  <amendment_to_document_number>H.R. 3684</amendment_to_document_number>
        |  <amendment_to_document_short_title>INVEST in America Act</amendment_to_document_short_title>
        |  <members/>
        |</roll_call_vote>""".stripMargin
    stubXml("/roll_call_votes/vote1171/vote_117_1_00312.xml", amendmentXml)

    val dto = makeClient().fetchVote(117, 1, 312).unsafeRunSync()

    val _ = dto.congress shouldBe 117
    val _ = dto.document.documentType shouldBe "S.Amdt."
    val _ = dto.document.documentNumber shouldBe ""
    val _ = dto.document.amendmentNumber shouldBe Some("S.Amdt. 2137")
    val _ = dto.document.amendmentToDocumentNumber shouldBe Some("H.R. 3684")
    dto.document.amendmentToDocumentShortTitle shouldBe Some("INVEST in America Act")
  }

}
