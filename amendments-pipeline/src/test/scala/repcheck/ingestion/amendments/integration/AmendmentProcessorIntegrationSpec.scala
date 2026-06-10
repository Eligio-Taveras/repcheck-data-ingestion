package repcheck.ingestion.amendments.integration

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import doobie.implicits._

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.amendments.api.AmendmentsApiClient
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.persistence.{DoobieAmendmentRepository, DoobieCommitteeLookupRepository}
import repcheck.ingestion.amendments.pipeline.AmendmentProcessor
import repcheck.ingestion.amendments.testing.TransactorFixture
import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieMemberRepository, MemberWriteInstances}
import repcheck.members.common.testing.DockerRequired
import repcheck.shared.models.congress.dos.member.MemberDO

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * AlloyDB Omni-backed integration spec for [[AmendmentProcessor]] running against the real `amendments` / `bills` /
 * `members` schema. WireMock simulates the Congress.gov endpoints; rows persist to the live container so we can assert
 * on the actual `bill_id (resolved ancestor)` values written.
 *
 * Tagged `DockerRequired` so `sbt test` skips when Docker isn't running. Run via the `dockerTest` alias in CI.
 */
class AmendmentProcessorIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with TransactorFixture {

  import MemberWriteInstances._

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()
    ._1

  private lazy val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  private def silentLogger(): PipelineLogger[IO] = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  /**
   * Load a JSON fixture from `__files/...` as a UTF-8 string. WireMock 3.x's `withBodyFile` lookup is unreliable across
   * the different sbt classpath layouts the integration suite hits (forked vs in-process, classes vs jar packaging), so
   * we read the file into the JVM and pass it via `withBody` instead. Rewrites any literal `https://api.congress.gov`
   * URLs in the fixture to point at the running WireMock instance — the captured fixtures embed the real Congress.gov
   * host, but the parent-recursion path follows those URLs verbatim, so without rewriting them the test would call out
   * to the live API (and 403, since we don't ship a real key).
   */
  private def loadBodyFile(path: String): String = {
    val resource = s"__files/$path"
    val stream = Option(getClass.getClassLoader.getResourceAsStream(resource)) match {
      case Some(s) => s
      case None    => sys.error(s"missing fixture: $resource")
    }
    val raw =
      try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
      finally stream.close()
    raw.replace("https://api.congress.gov", s"http://localhost:${wireMock.port().toString}")
  }

  private def buildProcessor(
    config: AmendmentsConfig,
    metrics: AmendmentMetrics,
  ): AmendmentProcessor[IO] = {
    val apiConfig = CongressGovClientConfig(
      apiKey = "test-api-key",
      baseUrl = s"http://localhost:${wireMock.port().toString}/v3",
      pageSize = config.pageSize,
      pageDelay = Duration.Zero,
      retry = RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 10L),
    )
    val apiClient        = AmendmentsApiClient[IO](apiConfig, httpClient, retryWrapper)
    val amendmentRepo    = new DoobieAmendmentRepository
    val billRepo         = new DoobieBillRepository
    val committeeRepo    = new DoobieCommitteeLookupRepository
    val memberRepo       = new DoobieMemberRepository
    val placeholder      = new DefaultPlaceholderCreator[IO]
    val memberEntityRepo = new DoobieEntityRepository[IO, MemberDO](xa, MemberInsertSql.value)

    new AmendmentProcessor[IO](
      apiClient = apiClient,
      amendmentRepository = amendmentRepo,
      placeholderCreator = placeholder,
      memberRepository = memberRepo,
      memberEntityRepo = memberEntityRepo,
      billRepository = billRepo,
      committeeRepository = committeeRepo,
      xa = xa,
      config = config,
      logger = silentLogger(),
      metrics = metrics,
    )
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
    super.beforeEach()
    wireMock.resetAll()
  }

  // --------------------------------------------------------------------------------------------
  // Sub-amendment chain: child references parent which references a bill.
  // --------------------------------------------------------------------------------------------

  "AmendmentProcessor.streamAll" should "hydrate a sub-amendment chain via inline recursion" taggedAs DockerRequired in {
    // List page returns just the sub-amendment (depth=2). The processor now hits the GLOBAL
    // `/v3/amendment` endpoint (no congress in path) and filters by `[congressesMin, congressesMax]` in-process.
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              s"""{
                  "amendments": [{
                    "congress": 117,
                    "number": "200",
                    "type": "SUAMDT",
                    "updateDate": "2024-06-02T13:00:00Z",
                    "url": "http://localhost:${wireMock.port().toString}/v3/amendment/117/suamdt/200"
                  }],
                  "pagination": { "count": 1 }
                }"""
            )
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/suamdt/200")).willReturn(
        aResponse()
          .withStatus(200)
          .withHeader("Content-Type", "application/json")
          .withBody(loadBodyFile("wiremock/amendments/detail-117-suamdt-200.json"))
      )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/100")).willReturn(
        aResponse()
          .withStatus(200)
          .withHeader("Content-Type", "application/json")
          .withBody(loadBodyFile("wiremock/amendments/detail-117-samdt-100.json"))
      )
    )

    val metrics = AmendmentMetrics.make()
    val cfg = AmendmentsConfig(
      congressesMin = 117,
      congressesMax = 117,
      lookbackDays = 7,
      parallelism = 1,
      pageDelay = 0.millis,
      maxRecursionDepth = 5,
      pageSize = 50,
    )
    val processor = buildProcessor(cfg, metrics)

    val results = processor.streamAll("integration-run-1").compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSucceeded) shouldBe true

    // Both rows should be persisted: parent SAMDT-100 (recursively hydrated) and child SUAMDT-200.
    val parentRow = sql"""SELECT congress, amendment_type::text, number, bill_id, parent_amendment_id
                          FROM amendments WHERE natural_key = '117-SAMDT-100'"""
      .query[(Int, String, String, Option[Long], Option[Long])]
      .option
      .transact(xa)
      .unsafeRunSync()
    val childRow = sql"""SELECT congress, amendment_type::text, number, bill_id, parent_amendment_id
                         FROM amendments WHERE natural_key = '117-SUAMDT-200'"""
      .query[(Int, String, String, Option[Long], Option[Long])]
      .option
      .transact(xa)
      .unsafeRunSync()

    val _ = parentRow.isDefined shouldBe true
    val _ = childRow.isDefined shouldBe true

    // Parent has bill_id resolved (direct from amendedBill).
    val _ = parentRow.flatMap(_._4).isDefined shouldBe true

    // Child inherits parent's bill_id via the resolved-ancestor logic.
    val _ = childRow.flatMap(_._4).isDefined shouldBe true

    // Child has parent_amendment_id pointing at the parent row.
    childRow.flatMap(_._5).isDefined shouldBe true
  }

  // --------------------------------------------------------------------------------------------
  // Standalone amendment with direct bill reference.
  // --------------------------------------------------------------------------------------------

  it should "persist a standalone amendment with direct bill resolution" taggedAs DockerRequired in {
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(
              s"""{
                  "amendments": [{
                    "congress": 117,
                    "number": "100",
                    "type": "SAMDT",
                    "updateDate": "2024-06-01T12:00:00Z",
                    "url": "http://localhost:${wireMock.port().toString}/v3/amendment/117/samdt/100"
                  }],
                  "pagination": { "count": 1 }
                }"""
            )
        )
    )

    wireMock.stubFor(
      get(urlPathEqualTo("/v3/amendment/117/samdt/100")).willReturn(
        aResponse()
          .withStatus(200)
          .withHeader("Content-Type", "application/json")
          .withBody(loadBodyFile("wiremock/amendments/detail-117-samdt-100.json"))
      )
    )

    val metrics = AmendmentMetrics.make()
    val cfg = AmendmentsConfig(
      congressesMin = 117,
      congressesMax = 117,
      lookbackDays = 7,
      parallelism = 1,
      pageDelay = 0.millis,
      maxRecursionDepth = 3,
      pageSize = 50,
    )
    val processor = buildProcessor(cfg, metrics)

    val results = processor.streamAll("integration-run-2").compile.toList.unsafeRunSync()

    val _ = results.size shouldBe 1
    val _ = results.headOption.exists(_.isSucceeded) shouldBe true

    val row = sql"""SELECT bill_id, sponsor_member_id, parent_amendment_id
                    FROM amendments WHERE natural_key = '117-SAMDT-100'"""
      .query[(Option[Long], Option[Long], Option[Long])]
      .option
      .transact(xa)
      .unsafeRunSync()

    val _ = row.isDefined shouldBe true
    val _ = row.flatMap(_._1).isDefined shouldBe true // bill_id populated
    val _ = row.flatMap(_._2).isDefined shouldBe true // sponsor_member_id populated
    row.flatMap(_._3).isEmpty shouldBe true // no parent
  }

}
