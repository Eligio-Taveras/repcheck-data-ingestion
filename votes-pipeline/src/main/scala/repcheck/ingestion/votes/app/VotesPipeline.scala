package repcheck.ingestion.votes.app

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, Resource, Sync, Temporal}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.Stream
import fs2.io.net.Network

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.ingestion.votes.config.VotesPipelineConfig
import repcheck.ingestion.votes.errors.StepRunIdInvalid

/**
 * Testable wiring for the votes pipeline. Config loading, resource acquisition, and high-level orchestration live here
 * — the actual processing collaborators (House API client, Senate XML client, LIS resolver, repositories, processor)
 * land in Phase 2 PRs per the votes-pipeline execution plan.
 *
 * Scaffold state: `run` produces an empty `Stream[F, ProcessingResult]` and defers to [[PipelineExecutor.execute]] for
 * the summary-log / exit-code plumbing. This lets the pipeline compile and ship a smoke test before any Phase 2
 * collaborators exist. Once `VotesProcessor` lands (P3.1), the empty stream is replaced with `processor.streamAll`, and
 * the processor generates a fresh per-vote correlation ID inside its parallel-eval loop so individual votes can be
 * traced through the logs independently of the run-level `runId`.
 *
 * ==Launcher contract==
 *   - `args(0)` — config JSON (consumed via [[PipelineBootstrap.loadConfig]] or, currently, via the embedded
 *     `application.conf` reference config).
 *   - `args(1)` — run-level identifier (`workflow_runs.id` string).
 *   - `args(2)` — step-level identifier (`workflow_run_steps.id` Long assigned by the launcher before invocation).
 */
private[app] object VotesPipeline {

  private val PipelineName = "votes-pipeline"

  final case class AppConfig(
    database: DatabaseConfig,
    congressApi: CongressGovClientConfig,
    pipeline: VotesPipelineConfig,
  ) derives pureconfig.ConfigReader

  def run[F[_]: Async: Network](args: List[String]): F[ExitCode] =
    for {
      config    <- Sync[F].delay(ConfigSource.default.loadOrThrow[AppConfig])
      runId     <- PipelineBootstrap.extractRunId[F](args)
      stepRunId <- extractStepRunId[F](args)
      logger    <- PipelineLoggerFactory.make[F](PipelineName)
      exitCode <- buildResources[F](config).use {
        case (_, _) =>
          // Scaffold placeholder — Phase 2 replaces `Stream.empty` with `processor.streamAll(runId)` once the votes
          // processor is wired in P3.1. Emitting an empty stream here lets the pipeline run end-to-end (config →
          // resources → executor → summary log → exit code) without any real work, so the scaffold can ship
          // independently.
          PipelineExecutor.execute[F](Stream.empty, logger, PipelineName, runId, stepRunId)
      }
    } yield exitCode

  /**
   * Extract the step-level identifier from `args(2)` and parse it as a `Long`. The launcher is responsible for creating
   * the `workflow_run_steps` row and passing its BIGSERIAL PK before invoking the pipeline — a missing or non-numeric
   * value indicates a broken launcher contract and fails the run fast via [[StepRunIdInvalid]].
   */
  private def extractStepRunId[F[_]: Sync](args: List[String]): F[Long] =
    args.lift(2) match {
      case Some(raw) if raw.trim.nonEmpty =>
        raw.trim.toLongOption match {
          case Some(id) => Sync[F].pure(id)
          case None     => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
        }
      case Some(raw) => Sync[F].raiseError[Long](StepRunIdInvalid(raw))
      case None      => Sync[F].raiseError[Long](StepRunIdInvalid("<missing>"))
    }

  /**
   * Wraps an HTTP client with per-client rate limiting: a semaphore ensures only one request is in-flight at a time,
   * with `pageDelay` inserted after each request completes. Canonical pattern across RepCheck pipelines — each HTTP
   * client gets its own wrapper with its own configured `pageDelay`.
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
