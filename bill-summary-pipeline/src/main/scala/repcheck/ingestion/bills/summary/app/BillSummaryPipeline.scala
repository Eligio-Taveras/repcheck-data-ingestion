package repcheck.ingestion.bills.summary.app

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, Resource, Sync, Temporal}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.bills.summary.api.BillSummariesApiClient
import repcheck.ingestion.bills.summary.config.BillSummaryConfig
import repcheck.ingestion.bills.summary.persistence.DoobieWorkflowRunStepsRepository
import repcheck.ingestion.bills.summary.pipeline.BillSummaryProcessor
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.pipeline.models.errors.RetryWrapper

private[app] object BillSummaryPipeline {

  private val PipelineName = "bill-summary-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: BillSummaryConfig,
  ) derives pureconfig.ConfigReader

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    for {
      config <- Sync[F].delay {
        ConfigSource.default.loadOrThrow[AppConfig]
      }
      logger <- PipelineLoggerFactory.make[F](PipelineName)
      exitCode <- buildResources[F](config).use {
        case (xa, httpClient) =>
          val billRepo     = new DoobieBillRepository
          val workflowRepo = new DoobieWorkflowRunStepsRepository
          val retryWrapper = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
          val apiClient    = BillSummariesApiClient[F](config.congressApi, httpClient, retryWrapper)

          val processor = new BillSummaryProcessor[F](
            apiClient = apiClient,
            billRepo = billRepo,
            workflowRepo = workflowRepo,
            xa = xa,
            config = config.pipeline,
            logger = logger,
          )

          // TODO: replace 0L with the Long run ID obtained from workflow_runs DB registration once
          // PipelineBootstrap.extractRunId (ingestion-common §3.7) is implemented. Same TODO as the
          // sibling pipelines.
          val runId        = 0L
          val resultStream = processor.streamAll(runId)
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId)
      }
    } yield exitCode
  }

  /**
   * Wraps an HTTP client with a per-pipeline rate limiter. A semaphore ensures only one request is in-flight at a time,
   * with `pageDelay` inserted after each request completes — keeps this pipeline's call rate independent of the other
   * Congress.gov-consuming pipelines that share the same API key.
   */
  private def rateLimitedClient[F[_]: Async](
    underlying: Client[F],
    config: CongressGovClientConfig,
  ): Resource[F, Client[F]] =
    Resource.eval(Semaphore[F](1)).map { sem =>
      Client[F] { request =>
        Resource.make(sem.acquire)(_ => Temporal[F].sleep(config.pageDelay) >> sem.release) >>
          underlying.run(request)
      }
    }

  private def buildResources[F[_]: Async: Network](
    config: AppConfig
  ): Resource[F, (Transactor[F], Client[F])] =
    for {
      xa              <- TransactorResource.make[F](config.database)
      rawClient       <- EmberClientBuilder.default[F].build
      throttledClient <- rateLimitedClient(rawClient, config.congressApi)
    } yield (xa, throttledClient)

}
