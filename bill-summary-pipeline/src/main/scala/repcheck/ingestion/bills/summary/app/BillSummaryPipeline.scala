package repcheck.ingestion.bills.summary.app

import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.bills.summary.api.BillSummariesApiClient
import repcheck.ingestion.bills.summary.config.BillSummaryConfig
import repcheck.ingestion.bills.summary.errors.StepRunIdInvalid
import repcheck.ingestion.bills.summary.persistence.DoobieWorkflowRunStepsRepository
import repcheck.ingestion.bills.summary.pipeline.BillSummaryProcessor
import repcheck.ingestion.common.api.{CongressGovClientConfig, RateLimitedHttpClient}
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.pipeline.models.errors.RetryWrapper

/**
 * Top-level wiring for the bill-summary pipeline. Loads config, extracts the launcher-supplied `runId` / `stepRunId`,
 * builds the managed Resource bundle (transactor + rate-limited HTTP client), and hands a result stream to
 * [[PipelineExecutor]] for streaming aggregation.
 *
 * ==Launcher contract==
 *
 *   - `args(0)` — config JSON placeholder (currently unused; the pipeline loads `application.conf` directly).
 *   - `args(1)` — run-level identifier (`workflow_runs.id` string). Required and non-blank.
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
      config    <- Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig])
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- extractStepRunId[F](args)
      logger    <- PipelineLoggerFactory.make[F](PipelineName)
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

          val resultStream = processor.streamAll(runId)
          PipelineExecutor.execute[F](resultStream, logger, PipelineName, runId, stepRunId)
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

  /**
   * Extract the step-level identifier from `args(2)` and parse it as a `Long`. The launcher is responsible for creating
   * the `workflow_run_steps` row and passing its BIGSERIAL PK before invoking the pipeline — a missing or non-numeric
   * value indicates a broken launcher contract and fails the run fast via [[StepRunIdInvalid]].
   *
   * For docker-compose / Ofelia entries where workflow_run_steps integration is not yet wired, callers pass `"0"` to
   * supply a placeholder Long that satisfies the contract without dishonestly hardcoding the value in source.
   */
  private[app] def extractStepRunId[F[_]: Sync](args: List[String]): F[Long] =
    args.lift(2) match {
      case Some(raw) if raw.trim.nonEmpty =>
        raw.trim.toLongOption match {
          case Some(id) => Sync[F].pure(id)
          case None     => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
        }
      case Some(raw) => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
      case None      => Sync[F].raiseError[Long](StepRunIdInvalid("<missing>"))
    }

}
