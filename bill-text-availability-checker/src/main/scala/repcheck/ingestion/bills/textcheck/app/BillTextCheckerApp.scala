package repcheck.ingestion.bills.textcheck.app

import cats.effect.std.Semaphore
import cats.effect.{Async, ExitCode, IO, IOApp, Resource, Sync, Temporal}
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import repcheck.ingestion.bills.textcheck.app.BillTextCheckerPipeline.AppConfig
import repcheck.ingestion.common.api.CongressGovClientConfig
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
          rateLimitedClient[IO](EmberClientBuilder.default[IO].build, config.congressApi),
          PubSubPublisherResource.make[IO](_),
        ),
      checkerFactory = BillTextCheckerPipeline.buildChecker[IO],
      streamFactory = BillTextCheckerPipeline.buildStream[IO],
    )
  }

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
