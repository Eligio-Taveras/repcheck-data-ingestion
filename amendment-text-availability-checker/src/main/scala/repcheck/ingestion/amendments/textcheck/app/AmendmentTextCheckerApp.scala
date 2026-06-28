package repcheck.ingestion.amendments.textcheck.app

import cats.effect.{ExitCode, IO, IOApp}

import org.http4s.ember.client.EmberClientBuilder

import repcheck.ingestion.amendments.textcheck.app.AmendmentTextCheckerRun.AppConfig
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.PipelineLoggerFactory

/**
 * One-shot Cloud Run Job entry point. Loads config, builds resources, runs the stream to completion, exits.
 *
 * Pure wiring — every line here is a factory call into [[AmendmentTextCheckerRun]] (the testable companion).
 *
 * Uniform 3-arg launcher contract (all strict):
 *   - `args(0)` — config-override JSON blob (`{}` = none), layered over `application.conf` via
 *     `PipelineBootstrap.loadConfig`.
 *   - `args(1)` — `runId`: workflow-run identifier (Long). Required and parseable.
 *   - `args(2)` — `stepRunId`: workflow_run_steps row identifier (Long). Required and parseable.
 *
 * For local / pre-registrar environments callers pass `"0"` for `runId` / `stepRunId`.
 */
object AmendmentTextCheckerApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      runId     <- PipelineBootstrap.extractRunId[IO](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[IO](args)
      exitCode  <- runChecker(args, runId, stepRunId)
    } yield exitCode

  private[app] def runChecker(args: List[String], runId: Long, stepRunId: Long): IO[ExitCode] =
    AmendmentTextCheckerRun.runWithFactories[IO](
      configLoader = PipelineBootstrap.loadConfig[IO, AppConfig](args),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        AmendmentTextCheckerRun.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          // Timeouts come from `congressApi.http` (HttpClientConfig in ingestion-common): a stuck request
          // would otherwise hang on a half-open connection (no timeout = unbounded wait), and idle eviction
          // stops stale connections from accumulating across long-running runs and silently failing on first
          // reuse. Defaults are 30s request / 60s idle in HttpClientConfig — overridable via
          // `congress-api.http.request-timeout` / `idle-timeout` in application.conf.
          EmberClientBuilder
            .default[IO]
            .withTimeout(config.congressApi.http.requestTimeout)
            .withIdleConnectionTime(config.congressApi.http.idleTimeout)
            .build
            .flatMap { raw =>
              // permits=1L mirrors the bill-side default — shared API key across multiple pipelines means the
              // per-pipeline call rate stays under Congress.gov's 5K/hr free-tier budget.
              RateLimitedHttpClient.make[IO](raw, pageDelay = config.congressApi.pageDelay, permits = 1L)
            },
          PubSubPublisherResource.make[IO](_),
        ),
      checkerFactory = AmendmentTextCheckerRun.buildChecker[IO],
      streamFactory = AmendmentTextCheckerRun.buildStream[IO],
      runId = runId,
      stepRunId = stepRunId,
    )

}
