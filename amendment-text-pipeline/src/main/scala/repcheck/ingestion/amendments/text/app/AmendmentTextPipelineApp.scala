package repcheck.ingestion.amendments.text.app

import java.util.concurrent.TimeUnit

import cats.effect.{Async, ExitCode, IO, IOApp, Resource, Sync}

import org.http4s.ember.client.EmberClientBuilder

import pureconfig.ConfigSource

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import repcheck.ingestion.amendments.text.app.AmendmentTextPipelinePipeline.AppConfig
import repcheck.ingestion.amendments.text.subscription.PubSubSubscriberResource
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.execution.WorkflowStateUpdater
import repcheck.ingestion.common.logging.PipelineLoggerFactory

/**
 * IOApp entry point for the amendment-text-pipeline (Cloud Run Service). Pure wiring; all logic lives in the testable
 * [[AmendmentTextPipelinePipeline]] companion object. Mirror of
 * [[repcheck.ingestion.bills.text.app.BillTextPipelineApp]].
 *
 * CLI args (positional, both optional, default `0L`):
 *   - `args(0)` — `runId`: workflow-run identifier (Long); placeholder until the workflow_runs table is wired up.
 *   - `args(1)` — `stepRunId`: workflow_run_steps row identifier (Long); placeholder until that table exists.
 *
 * Both default to `0L` when missing or unparseable so the app remains runnable without a workflow registrar. The
 * `WorkflowStateUpdater` is constructed only when a non-zero `runId` is supplied — the placeholder `0L` means no
 * workflow registration is in scope and the updater would have nothing real to write to.
 */
object AmendmentTextPipelineApp extends IOApp {

  /**
   * Manages the lifecycle of an emulator-targeted gRPC `ManagedChannel`. Mirrors the bill-side defensive teardown —
   * without this, Pub/Sub emulator channels leak non-daemon gRPC threads and the JVM never exits after the stream
   * drains. See `BillTextPipelineApp.emulatorChannelResource` for the production evidence.
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

  override def run(args: List[String]): IO[ExitCode] = {
    val runId     = args.lift(0).flatMap(_.toLongOption).getOrElse(0L)
    val stepRunId = args.lift(1).flatMap(_.toLongOption).getOrElse(0L)

    AmendmentTextPipelinePipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = (config, logger) =>
        AmendmentTextPipelinePipeline.buildResources[IO](
          config,
          logger,
          TransactorResource.make[IO](_),
          // GovInfo client: external API rate-limited per the published 36000-req/hour budget. Timeouts and
          // permits come from `pipeline.gov-info-http` / `pipeline.gov-info-permits` so the operator can tune
          // both without a code change. Defaults reuse the bill-side empirical values (120s/120s, permits=2).
          EmberClientBuilder
            .default[IO]
            .withTimeout(config.pipeline.govInfoHttp.requestTimeout)
            .withIdleConnectionTime(config.pipeline.govInfoHttp.idleTimeout)
            .build
            .flatMap { raw =>
              RateLimitedHttpClient.make[IO](
                raw,
                pageDelay = config.pipeline.pageDelay,
                permits = config.pipeline.govInfoPermits,
              )
            },
          // Ollama client: local sidecar; no external quota; no rate limit so a leaked GovInfo permit can never
          // wedge the embedder. Tighter idle timeout than the GovInfo client because local connections are cheap
          // to re-establish — both timeouts come from `pipeline.ollama-http` for operator control.
          EmberClientBuilder
            .default[IO]
            .withTimeout(config.pipeline.ollamaHttp.requestTimeout)
            .withIdleConnectionTime(config.pipeline.ollamaHttp.idleTimeout)
            .build,
          (subConfig, log) =>
            // Compose the channel resource OUTSIDE the subscriber resource so the channel is closed AFTER the
            // stub on release. Same ordering as the bill-side; without it the emulator channel's gRPC threads
            // keep the JVM pinned post-stream-drain.
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
      processorFactory = AmendmentTextPipelinePipeline.buildProcessor[IO],
      streamFactory = AmendmentTextPipelinePipeline.buildStream[IO],
      // Construct the WorkflowStateUpdater only when a non-zero runId is in scope — `0L` is the placeholder used
      // when this Cloud Run Service runs without a workflow registrar (e.g., local dev or pre-§3.7 wiring), and
      // there is nothing meaningful to write in that case.
      workflowStateUpdaterFactory = (transactor, cfg) =>
        if (runId == 0L) { None }
        else { Some(new WorkflowStateUpdater[IO](transactor, cfg)) },
      runId = runId,
      stepRunId = stepRunId,
    )
  }

}
