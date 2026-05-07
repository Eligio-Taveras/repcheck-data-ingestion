package repcheck.ingestion.amendments.textcheck.app

import cats.effect.Resource

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.common.logging.PipelineLogger

/**
 * Resource bundle: managed dependencies (transactor, HTTP client, Pub/Sub publisher) wired up in one `Resource` chain
 * so the IOApp's `Resource.use` releases everything in reverse order on exit. Coverage-excluded because the file is
 * pure wiring — `AmendmentTextCheckerRun` carries the testable factory-function shape.
 */
final case class AmendmentTextCheckerResources[F[_]](
  xa: Transactor[F],
  httpClient: Client[F],
  pubSubPublisher: PubSubEventPublisher[F],
)

object AmendmentTextCheckerResources {

  def build[F[_]](
    config: AmendmentTextCheckerRun.AppConfig,
    logger: PipelineLogger[F],
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    pubSubPublisherFactory: EventPublisherConfig => Resource[F, PubSubEventPublisher[F]],
  ): Resource[F, AmendmentTextCheckerResources[F]] = {
    val _ = logger // reserved for future resource-level logging
    for {
      xa              <- transactorFactory(config.database)
      httpClient      <- httpClientFactory
      pubSubPublisher <- pubSubPublisherFactory(config.eventPublisher)
    } yield AmendmentTextCheckerResources(xa, httpClient, pubSubPublisher)
  }

}
