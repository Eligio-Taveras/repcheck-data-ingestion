package repcheck.ingestion.amendments.textcheck.app

import scala.concurrent.duration._

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.amendments.textcheck.app.AmendmentTextCheckerRun.AppConfig
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory

/**
 * One-shot Cloud Run Job entry point. Loads config, builds resources, runs the stream to completion, exits.
 *
 * Pure wiring — every line here is a factory call into [[AmendmentTextCheckerRun]] (the testable companion).
 */
object AmendmentTextCheckerApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    AmendmentTextCheckerRun.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        AmendmentTextCheckerRun.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          // 30s request timeout + 60s idle eviction matches the bill-side checker — protects against half-open
          // connections hanging the JVM and against stale-pool reuse on long-running runs.
          EmberClientBuilder
            .default[IO]
            .withTimeout(30.seconds)
            .withIdleConnectionTime(60.seconds)
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
    )
  }

}
