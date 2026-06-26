package repcheck.ingestion.bills.text.app

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.DurationInt

import cats.effect.{Async, ExitCode, IO, IOApp, Resource}

import org.http4s.ember.client.EmberClientBuilder

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import repcheck.ingestion.bills.text.app.BillTextPipelinePipeline.AppConfig
import repcheck.ingestion.bills.text.subscription.PubSubSubscriberResource
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.PubSubPublisherResource
import repcheck.ingestion.common.execution.{PipelineBootstrap, WorkflowStateUpdater}
import repcheck.ingestion.common.logging.PipelineLoggerFactory

object BillTextPipelineApp extends IOApp {

  /**
   * Manages the lifecycle of an emulator-targeted gRPC `ManagedChannel`.
   *
   * ==Why this exists==
   *
   * When `PUBSUB_EMULATOR_HOST` is set (local dev / docker-compose), we route the Pub/Sub subscriber stub through a
   * plaintext gRPC channel via `FixedTransportChannelProvider`. Per gRPC's contract, channels supplied through
   * `FixedTransportChannelProvider` are NOT shut down by the consuming stub on its own `close()` — the caller owns the
   * channel's lifecycle. Without explicit teardown, the channel's gRPC event-loop threads (non-daemon by default) keep
   * the JVM alive forever after the IOApp's stream completes, which (a) prevents Ofelia's `job-run` cron from launching
   * a new container with the same `container_name`, and (b) silently halts ingestion until manually restarted.
   *
   * Surfaced empirically when the local stack's bill-text-pipeline container went silent for 7 hours after its Pub/Sub
   * stream drained: `docker top` showed the JVM still running with 0.12% CPU, no new processing, ~217k bills awaiting
   * download. Resource cleanup had run (stub closed) but the leaked channel kept the JVM pinned.
   *
   * Wrapping the channel in this `Resource` ties its `shutdown()` + bounded `awaitTermination()` to the IOApp's normal
   * cleanup chain. With an Ofelia tick (`job-run` mode), a clean stream completion now leads to JVM exit, container
   * exit, and the next scheduled tick relaunches a fresh container.
   *
   * Returns `None` when the env var is unset (production GCP), since gRPC's default `InstantiatingGrpcChannelProvider`
   * manages its own channel lifecycle correctly — only the explicit `FixedTransportChannelProvider` path leaks.
   */
  private[app] def emulatorChannelResource[F[_]: Async](
    emulatorHost: Option[String]
  ): Resource[F, Option[ManagedChannel]] =
    emulatorHost match {
      case None =>
        Resource.pure[F, Option[ManagedChannel]](None)
      case Some(host) =>
        Resource
          .make(
            Async[F].blocking(ManagedChannelBuilder.forTarget(host).usePlaintext().build())
          )(channel =>
            Async[F].blocking {
              channel.shutdown()
              val _ = channel.awaitTermination(5, TimeUnit.SECONDS)
            }
          )
          .map(Some(_))
    }

  override def run(args: List[String]): IO[ExitCode] =
    for {
      runId     <- PipelineBootstrap.extractRunId[IO](args)
      stepRunId <- PipelineBootstrap.extractStepRunId[IO](args)
      exitCode  <- runPipeline(args, runId, stepRunId)
    } yield exitCode

  private[app] def runPipeline(args: List[String], runId: Long, stepRunId: Long): IO[ExitCode] =
    BillTextPipelinePipeline.runWithFactories[IO](
      configLoader = PipelineBootstrap.loadConfig[IO, AppConfig](args),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        BillTextPipelinePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          // Congress.gov client: external API, rate-limited per the published 5000-req/hour budget. Wrapping
          // the underlying Ember client in `RateLimitedHttpClient.make(permits=2, pageDelay=...)` enforces at
          // most two in-flight requests + a configurable spacing between releases. Used by BillTextDownloader.
          //
          // permits=2 was empirically chosen (2026-04-29 backfill experiment): permits=1 left the GPU idle
          // ~50% of the time waiting for the next download, with bursty 21-87% utilization. Bumping to 2
          // saturated the embedder steadily at 84-89% — sustained throughput rose from ~140 chunks/min to
          // ~199 chunks/min (+42%) with no observed increase in upstream errors. We are still well under
          // GovInfo's 5000/hour budget at this rate (peak ~13/sec → ~46k/hour potential, but the embedder
          // ceiling at ~3-4 downloads/sec keeps actual rate around 12k/hour).
          EmberClientBuilder
            .default[IO]
            .withTimeout(120.seconds)
            .withIdleConnectionTime(120.seconds)
            .build
            .flatMap { raw =>
              RateLimitedHttpClient.make[IO](raw, pageDelay = config.pipeline.pageDelay, permits = 2L)
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
            // Compose the channel resource OUTSIDE the subscriber resource so the channel is closed AFTER the
            // stub on release (innermost-acquired-released-first ordering). Without this, the leaked channel's
            // non-daemon gRPC threads keep the JVM alive after stream completion — see emulatorChannelResource
            // scaladoc for the empirical evidence.
            emulatorChannelResource[IO](sys.env.get("PUBSUB_EMULATOR_HOST")).flatMap { maybeChannel =>
              PubSubSubscriberResource.make[IO](
                subConfig,
                log,
                () => {
                  val builder = SubscriberStubSettings.newBuilder()
                  maybeChannel.foreach { channel =>
                    val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
                    builder.setTransportChannelProvider(channelProvider)
                    builder.setCredentialsProvider(NoCredentialsProvider.create())
                  }
                  GrpcSubscriberStub.create(builder.build())
                },
              )
            },
        ),
      processorFactory = BillTextPipelinePipeline.buildProcessor[IO],
      streamFactory = BillTextPipelinePipeline.buildStream[IO],
      workflowStateUpdaterFactory =
        (xa, cfg) => sys.env.get("WORKFLOW_RUN_ID").map(_ => new WorkflowStateUpdater[IO](xa, cfg)),
      runId = runId,
      stepRunId = stepRunId,
    )

}
