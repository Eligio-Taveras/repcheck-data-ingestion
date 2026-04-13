package com.repcheck.bills.text.app

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.effect.{Async, ExitCode, IO, IOApp, Resource, Sync, Temporal}
import cats.effect.std.Semaphore
import cats.syntax.all._

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

import io.grpc.ManagedChannelBuilder

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.PipelineLoggerFactory

import com.repcheck.bills.text.app.BillTextPipelinePipeline.AppConfig
import com.repcheck.bills.text.subscription.PubSubSubscriberResource

object BillTextPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args // args reserved for future CLI config override support
    BillTextPipelinePipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        BillTextPipelinePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          rateLimitedClient[IO](
            EmberClientBuilder
              .default[IO]
              .withTimeout(120.seconds)
              .withIdleConnectionTime(120.seconds)
              .build,
            config.pipeline.pageDelay,
          ),
          PubSubPublisherResource.make[IO](_),
          (subConfig, log) =>
            PubSubSubscriberResource.make[IO](
              subConfig,
              log,
              () => {
                val builder = SubscriberStubSettings.newBuilder()
                sys.env.get("PUBSUB_EMULATOR_HOST").foreach { host =>
                  val channel         = ManagedChannelBuilder.forTarget(host).usePlaintext().build()
                  val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
                  builder.setTransportChannelProvider(channelProvider)
                  builder.setCredentialsProvider(NoCredentialsProvider.create())
                }
                GrpcSubscriberStub.create(builder.build())
              },
            ),
        ),
      processorFactory = BillTextPipelinePipeline.buildProcessor[IO],
      streamFactory = BillTextPipelinePipeline.buildStream[IO],
      workflowStateUpdaterFactory = (xa, cfg) =>
        sys.env.get("WORKFLOW_RUN_ID").map(_ => new WorkflowStateUpdater[IO](xa, cfg)),
    )
  }

  private def rateLimitedClient[F[_]: Async](
    underlying: Resource[F, Client[F]],
    pageDelay: FiniteDuration,
  ): Resource[F, Client[F]] =
    underlying.flatMap { raw =>
      Resource.eval(Semaphore[F](1)).map { sem =>
        Client[F] { request =>
          Resource.make(sem.acquire)(_ => Temporal[F].sleep(pageDelay) >> sem.release) >>
            raw.run(request)
        }
      }
    }

}
