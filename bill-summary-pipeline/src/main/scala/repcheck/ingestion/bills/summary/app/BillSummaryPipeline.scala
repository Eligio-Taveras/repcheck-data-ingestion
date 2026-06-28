package repcheck.ingestion.bills.summary.app

import cats.effect.{Async, ExitCode, Resource}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.{DoobieBillRepository, DoobieBillSummaryRepository}
import repcheck.ingestion.bills.summary.api.BillSummariesApiClient
import repcheck.ingestion.bills.summary.config.BillSummaryConfig
import repcheck.ingestion.bills.summary.persistence.DoobieWorkflowRunStepsRepository
import repcheck.ingestion.bills.summary.pipeline.BillSummaryProcessor
import repcheck.ingestion.common.api.{CongressGovClientConfig, RateLimitedHttpClient}
import repcheck.ingestion.common.congresses.CongressResolver
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.execution.{PipelineBootstrap, PipelineExecutor}
import repcheck.ingestion.common.logging.PipelineLoggerFactory

import com.repcheck.utils.errors.RetryWrapper

/**
 * Top-level wiring for the bill-summary pipeline. Loads config, extracts the launcher-supplied `runId` / `stepRunId`,
 * builds the managed Resource bundle (transactor + rate-limited HTTP client), and hands a result stream to
 * [[PipelineExecutor]] for streaming aggregation.
 *
 * ==Launcher contract==
 *
 *   - `args(0)` — config-override JSON blob (`{}` = none), layered over `application.conf` via
 *     `PipelineBootstrap.loadConfig`.
 *   - `args(1)` — run-level identifier (`workflow_runs.id` `Long`). Required and parseable.
 *   - `args(2)` — step-level identifier (`workflow_run_steps.id` `Long`). Required and parseable.
 *
 * For docker-compose / Ofelia local environments where the launcher hasn't been wired up yet, callers can pass `"0"`
 * for both `runId` and `stepRunId` (mirrors votes-pipeline's stance).
 */
private[app] object BillSummaryPipeline {

  private val PipelineName = "bill-summary-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: BillSummaryConfig,
  ) derives pureconfig.ConfigReader

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] =
    for {
      config    <- PipelineBootstrap.loadConfig[F, AppConfig](args)
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[F](args)
      logger    <- PipelineLoggerFactory.make[F](PipelineName)
      exitCode <- buildResources[F](config).use {
        case (xa, httpClient) =>
          for {
            congresses <- CongressResolver.resolve[F](
              envVarName = "BILL_SUMMARY_CONGRESSES",
              stepName = "bill-summary-pipeline:resolve-congresses",
              configuredCongresses = config.pipeline.congresses,
              xa = xa,
              logger = logger,
              envGetter = sys.env.get,
            )
            billRepo        = new DoobieBillRepository
            billSummaryRepo = new DoobieBillSummaryRepository
            workflowRepo    = new DoobieWorkflowRunStepsRepository
            retryWrapper    = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
            apiClient       = BillSummariesApiClient[F](config.congressApi, httpClient, retryWrapper)
            processor = new BillSummaryProcessor[F](
              apiClient = apiClient,
              billRepo = billRepo,
              billSummaryRepo = billSummaryRepo,
              workflowRepo = workflowRepo,
              xa = xa,
              config = config.pipeline,
              logger = logger,
            )
            resultStream = processor.streamAll(runId.toString, congresses)
            result <- PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId, stepRunId)
          } yield result
      }
    } yield exitCode

  private def buildResources[F[_]: Async: Network](
    config: AppConfig
  ): Resource[F, (Transactor[F], Client[F])] =
    for {
      xa        <- TransactorResource.make[F](config.database)
      rawClient <- EmberClientBuilder.default[F].build
      throttledClient <- RateLimitedHttpClient.make[F](
        rawClient,
        pageDelay = config.congressApi.pageDelay,
        permits = config.pipeline.httpConcurrency.toLong,
      )
    } yield (xa, throttledClient)

}
