package repcheck.ingestion.members.profile.app

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, IO, IOApp, Resource, Sync, Temporal}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.ingestion.members.profile.app.MemberProfilePipeline.AppConfig

object MemberProfilePipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    MemberProfilePipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        MemberProfilePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          rateLimitedClient[IO](EmberClientBuilder.default[IO].build, config.congressApi),
          PubSubPublisherResource.make[IO](_),
        ),
      processorFactory = MemberProfilePipeline.buildProcessor[IO],
      streamFactory = MemberProfilePipeline.buildStream[IO],
    )
  }

  /**
   * Wraps an HTTP client with a global rate limiter: a semaphore ensures only one request is in-flight at a time, with
   * `pageDelay` inserted after each request completes. This throttles Congress.gov API calls without requiring changes
   * inside the client.
   */
  private def rateLimitedClient[F[_]: Async](
    underlying: Resource[F, Client[F]],
    config: CongressGovClientConfig,
  ): Resource[F, Client[F]] =
    underlying.flatMap { raw =>
      Resource.eval(Semaphore[F](1)).map { sem =>
        Client[F] { request =>
          Resource.make(sem.acquire)(_ => Temporal[F].sleep(config.pageDelay) >> sem.release) >>
            raw.run(request)
        }
      }
    }

}
