package repcheck.ingestion.members.profile.app

import cats.effect.{ExitCode, IO, IOApp}

import org.http4s.ember.client.EmberClientBuilder

import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.PipelineBootstrap
import repcheck.ingestion.common.logging.PipelineLoggerFactory
import repcheck.ingestion.members.profile.app.MemberProfilePipeline.AppConfig

object MemberProfilePipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      runId     <- PipelineBootstrap.extractRunId[IO](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[IO](args)
      exitCode  <- runPipeline(args, runId, stepRunId)
    } yield exitCode

  private[app] def runPipeline(args: List[String], runId: Long, stepRunId: Long): IO[ExitCode] =
    MemberProfilePipeline.runWithFactories[IO](
      configLoader = PipelineBootstrap.loadConfig[IO, AppConfig](args),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        MemberProfilePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          EmberClientBuilder.default[IO].build.flatMap { raw =>
            RateLimitedHttpClient.make[IO](raw, pageDelay = config.congressApi.pageDelay, permits = 1L)
          },
          PubSubPublisherResource.make[IO](_),
        ),
      processorFactory = MemberProfilePipeline.buildProcessor[IO],
      congressesResolver =
        (cfg, xa, logger) => MemberProfilePipeline.resolveCongresses[IO](cfg, xa, logger, sys.env.get),
      streamFactory = MemberProfilePipeline.buildStream[IO],
      runId = runId,
      stepRunId = stepRunId,
    )

}
