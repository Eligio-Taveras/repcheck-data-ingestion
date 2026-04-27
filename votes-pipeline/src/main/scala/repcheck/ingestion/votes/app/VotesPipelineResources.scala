package repcheck.ingestion.votes.app

import cats.effect.{Async, Resource}

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{
  DefaultIngestionEventPublisher,
  EventPublisherConfig,
  IngestionEventPublisher,
  PubSubEventPublisher,
}
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}

/**
 * Managed-resources bundle for the votes pipeline plus the helper that composes it. Separated from [[VotesPipeline]] so
 * resource lifecycle and HTTP-pacing concerns live in their own reviewable unit.
 *
 * ==Two rate-limited HTTP clients, one raw Ember client==
 *
 * Both the House (Congress.gov JSON) and Senate (senate.gov XML) clients share a single underlying
 * [[org.http4s.ember.client.EmberClientBuilder]] resource, but each wraps it through
 * [[repcheck.ingestion.common.api.RateLimitedHttpClient.make]] with its own configurable delay (`house.pageDelay` vs.
 * `senate.requestDelay`). The shared helper uses a one-permit `Semaphore` to serialize requests on each wrapped client
 * so per-request pacing is respected even when upstream `parEvalMap(parallelism > 1)` submits calls concurrently.
 * Sharing the underlying client keeps the connection pool small; the wrappers enforce politeness per-feed.
 */
private[votes] object VotesPipelineResources {

  /**
   * The resource bundle handed to [[VotesProcessorFactory.build]]. Tests can construct this directly with mocks to
   * exercise the downstream wiring without acquiring real connections.
   *
   * `retryWrapper` is shared across every subsystem that needs retry semantics — the Pub/Sub publisher constructed
   * below, plus the Congress.gov API + senate.gov XML clients in [[VotesProcessorFactory]]. The wrapper's logging
   * callback is a no-op (per-retry logs come from each client's structured [[PipelineLogger]] instead); exposing it
   * here keeps the construction site single and testable.
   */
  final case class Resources[F[_]](
    xa: Transactor[F],
    houseClient: Client[F],
    senateClient: Client[F],
    eventPublisher: IngestionEventPublisher[F],
    retryWrapper: RetryWrapper[F],
  )

  /**
   * Compose the managed resources needed by [[VotesProcessorFactory.build]]:
   *   - a Doobie `Transactor[F]` against AlloyDB / Cloud SQL PostgreSQL;
   *   - a `Client[F]` per chamber, each pre-wrapped with [[RateLimitedHttpClient]] and its own pacing delay;
   *   - a Google Pub/Sub `PubSubEventPublisher[F]`, wrapped as the higher-level `IngestionEventPublisher[F]` so
   *     downstream collaborators only see the application-facing API.
   *
   * Accepts the low-level factories as parameters so tests can substitute fixed values without pulling in real GCP /
   * AlloyDB / HTTP libraries.
   */
  def build[F[_]: Async](
    config: VotesPipeline.AppConfig,
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, Resources[F]] =
    for {
      xa        <- transactorFactory(config.database)
      rawClient <- httpClientFactory
      houseClient <- RateLimitedHttpClient.make[F](
        rawClient,
        pageDelay = config.pipeline.house.pageDelay,
        permits = config.pipeline.house.permits.toLong,
      )
      senateClient <- RateLimitedHttpClient.make[F](
        rawClient,
        pageDelay = config.pipeline.senate.requestDelay,
        permits = config.pipeline.senate.permits.toLong,
      )
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
      retryWrapper = noOpRetryWrapper[F]
      publisher = new DefaultIngestionEventPublisher[F](
        publisher = pubSubPublisher,
        topicName = config.eventPublisher.topicName,
        source = config.eventPublisher.source,
        retryWrapper = retryWrapper,
        retryConfig = RetryConfig(),
      )
    } yield Resources(xa, houseClient, senateClient, publisher, retryWrapper)

  /**
   * Construct a [[RetryWrapper]] whose retry-logging callback is a no-op. Per-retry logs come from each subsystem's own
   * structured [[repcheck.ingestion.common.logging.PipelineLogger]] at the retry boundary — the wrapper's log hook is
   * only useful when no other logging is already in place. Extracted as a named helper so the lambda body can be
   * exercised by a direct unit test instead of requiring a real retry invocation in every consumer's suite.
   */
  private[app] def noOpRetryWrapper[F[_]: Async]: RetryWrapper[F] =
    new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)

}
