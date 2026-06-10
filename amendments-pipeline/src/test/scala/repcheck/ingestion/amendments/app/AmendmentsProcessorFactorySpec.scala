package repcheck.ingestion.amendments.app

import scala.concurrent.duration._

import cats.effect.IO

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.pipeline.AmendmentProcessor
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

/**
 * Smoke test for [[AmendmentsProcessorFactory.build]]. Guards against latent wiring regressions (null-defaulted dep,
 * wrong constructor arity, etc.) that the compiler could miss. Runs against mocked Resources — no DB, no HTTP — and
 * asserts the returned processor is a valid instance.
 */
class AmendmentsProcessorFactorySpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private def cfg(): AmendmentsPipeline.AppConfig =
    AmendmentsPipeline.AppConfig(
      database = DatabaseConfig(
        host = "localhost",
        port = 5432,
        database = "repcheck_test",
        username = "test",
        password = "test",
        maxConnections = 11,
      ),
      congressApi = CongressGovClientConfig(
        baseUrl = "http://localhost:0",
        apiKey = "test-key",
        pageSize = 10,
        pageDelay = 1.millis,
        retry = RetryConfig(),
      ),
      pipeline = AmendmentsConfig(
        congressesMin = 117,
        congressesMax = 117,
        lookbackDays = 7,
        parallelism = 2,
        pageDelay = 0.millis,
        maxRecursionDepth = 3,
        pageSize = 10,
      ),
    )

  private def silentLogger(): PipelineLogger[IO] = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  "build" should "construct a fully wired AmendmentProcessor without throwing" in {
    val resources = AmendmentsPipelineResources.Resources[IO](
      xa = mock[Transactor[IO]],
      httpClient = mock[Client[IO]],
      retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit),
    )

    val processor = AmendmentsProcessorFactory.build[IO](cfg(), resources, silentLogger(), AmendmentMetrics.make())
    processor shouldBe a[AmendmentProcessor[?]]
  }

}
