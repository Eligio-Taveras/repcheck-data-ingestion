package repcheck.ingestion.votes.app

import scala.concurrent.duration.FiniteDuration

import cats.effect.std.Semaphore
import cats.effect.{Async, Resource, Temporal}
import cats.syntax.all._

import org.http4s.client.Client

import doobie.util.transactor.Transactor

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
 * [[org.http4s.ember.client.EmberClientBuilder]] resource, but each wraps it through [[rateLimitedClient]] with its own
 * configurable delay (`house.pageDelay` vs. `senate.requestDelay`). The wrapper uses a one-permit `Semaphore` to
 * serialize requests on each wrapped client so per-request pacing is respected even when upstream
 * `parEvalMap(parallelism > 1)` submits calls concurrently. Sharing the underlying client keeps the connection pool
 * small; the wrappers enforce politeness per-feed.
 */
private[app] object VotesPipelineResources {

  /**
   * The resource bundle handed to [[VotesProcessorFactory.build]]. Tests can construct this directly with mocks to
   * exercise the downstream wiring without acquiring real connections.
   */
  final case class Resources[F[_]](
    xa: Transactor[F],
    houseClient: Client[F],
    senateClient: Client[F],
    eventPublisher: IngestionEventPublisher[F],
  )

  /**
   * Compose the managed resources needed by [[VotesProcessorFactory.build]]:
   *   - a Doobie `Transactor[F]` against AlloyDB / Cloud SQL PostgreSQL;
   *   - a `Client[F]` per chamber, each pre-wrapped with its own [[rateLimitedClient]] and pacing delay;
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
      xa              <- transactorFactory(config.database)
      rawClient       <- httpClientFactory
      houseClient     <- rateLimitedClient(rawClient, config.pipeline.house.pageDelay)
      senateClient    <- rateLimitedClient(rawClient, config.pipeline.senate.requestDelay)
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
      retryWrapper = new RetryWrapper[F]((_, _, _, _, _, _) => Async[F].unit)
      publisher = new DefaultIngestionEventPublisher[F](
        publisher = pubSubPublisher,
        topicName = config.eventPublisher.topicName,
        source = config.eventPublisher.source,
        retryWrapper = retryWrapper,
        retryConfig = RetryConfig(),
      )
    } yield Resources(xa, houseClient, senateClient, publisher)

  /**
   * Wraps an HTTP client with per-client rate limiting: a semaphore ensures only one request is in-flight at a time,
   * with `delay` inserted after each request completes. Canonical pattern across RepCheck pipelines — each HTTP client
   * gets its own wrapper with its own configured delay (`house.pageDelay` vs. `senate.requestDelay`).
   */
  private[app] def rateLimitedClient[F[_]: Async](
    underlying: Client[F],
    delay: FiniteDuration,
  ): Resource[F, Client[F]] =
    Resource.eval(Semaphore[F](1)).map { sem =>
      Client[F] { request =>
        Resource.make(sem.acquire)(_ => Temporal[F].sleep(delay) >> sem.release) >>
          underlying.run(request)
      }
    }

}
