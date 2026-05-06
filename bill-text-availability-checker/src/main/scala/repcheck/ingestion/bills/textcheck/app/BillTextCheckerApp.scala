package repcheck.ingestion.bills.textcheck.app

import scala.concurrent.duration._

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.bills.textcheck.app.BillTextCheckerPipeline.AppConfig
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory

object BillTextCheckerApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        BillTextCheckerPipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          // withTimeout(30s) + withIdleConnectionTime(60s): without these, a stuck request can hang
          // indefinitely on a half-open connection (no timeout = unbounded wait). 30s is generous
          // for Congress.gov's slow responses; 60s pool eviction stops stale connections from
          // accumulating across long-running runs and silently failing on first reuse.
          EmberClientBuilder
            .default[IO]
            .withTimeout(30.seconds)
            .withIdleConnectionTime(60.seconds)
            .build
            .flatMap { raw =>
              // permits=1L is the right default after a brief 2026-04 experiment that bumped to 3
              // and triggered a sustained Congress.gov 429 throttle (the per-key bucket couldn't
              // refill fast enough across all three pipelines sharing the key). With the 750ms
              // pageDelay this gives ~80 req/min ≈ 4800 req/hr — comfortably under the 5K/hr
              // free-tier limit when the metadata + member-profile pipelines are also active.
              RateLimitedHttpClient.make[IO](raw, pageDelay = config.congressApi.pageDelay, permits = 1L)
            },
          PubSubPublisherResource.make[IO](_),
        ),
      checkerFactory = BillTextCheckerPipeline.buildChecker[IO],
      streamFactory = BillTextCheckerPipeline.buildStream[IO],
    )
  }

}
