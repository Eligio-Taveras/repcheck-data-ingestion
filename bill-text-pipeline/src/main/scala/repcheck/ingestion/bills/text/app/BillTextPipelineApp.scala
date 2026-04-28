package repcheck.ingestion.bills.text.app

import scala.concurrent.duration.DurationInt

import cats.effect.{ExitCode, IO, IOApp, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

import io.grpc.ManagedChannelBuilder
import repcheck.ingestion.bills.text.app.BillTextPipelinePipeline.AppConfig
import repcheck.ingestion.bills.text.subscription.PubSubSubscriberResource
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.PipelineLoggerFactory

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
          // Congress.gov client: external API, rate-limited per the published 5000-req/hour budget. Wrapping
          // the underlying Ember client in `RateLimitedHttpClient.make(permits=1, pageDelay=...)` enforces a
          // single in-flight request + a configurable spacing between releases. Used by BillTextDownloader.
          EmberClientBuilder
            .default[IO]
            .withTimeout(120.seconds)
            .withIdleConnectionTime(120.seconds)
            .build
            .flatMap { raw =>
              RateLimitedHttpClient.make[IO](raw, pageDelay = config.pipeline.pageDelay, permits = 1L)
            },
          // Ollama client: local sidecar over the docker-compose network; no external quota to honor and no
          // shared throttle so a leaked Congress.gov rate-limiter permit can never block /api/embed. Used
          // exclusively by the embedder. Tighter idle-connection lifetime than the Congress.gov client because
          // local connections are cheap to re-establish and we don't want to leak file handles.
          EmberClientBuilder
            .default[IO]
            .withTimeout(120.seconds)
            .withIdleConnectionTime(60.seconds)
            .build,
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
      workflowStateUpdaterFactory =
        (xa, cfg) => sys.env.get("WORKFLOW_RUN_ID").map(_ => new WorkflowStateUpdater[IO](xa, cfg)),
    )
  }

}
