package repcheck.ingestion.amendments.app

import cats.effect.{Async, Resource}

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.common.api.RateLimitedHttpClient
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.pipeline.models.errors.RetryWrapper

/**
 * Managed-resources bundle for the amendments-pipeline plus the helper that composes it. Mirrors
 * `VotesPipelineResources`: keep resource-lifecycle and HTTP-pacing concerns out of the wiring code so each layer is
 * reviewable on its own.
 *
 * The HikariCP-backed transactor is sized via `DatabaseConfig.maxConnections`, which the operator-facing
 * `application.conf` defaults to `parallelism × maxRecursionDepth + 5 = 45` per P1. The +5 buffer covers the
 * workflow-state writer + bookkeeping queries running alongside the main pipeline; with the default of 10 from the
 * bills pipelines, a deep cold chain at parallelism=4 deadlocks on connection acquisition.
 */
private[app] object AmendmentsPipelineResources {

  /**
   * The resource bundle handed to [[AmendmentsProcessorFactory.build]]. Tests can construct this directly with mocks to
   * exercise downstream wiring without touching real connections.
   */
  final case class Resources[F[_]](
    xa: Transactor[F],
    httpClient: Client[F],
    retryWrapper: RetryWrapper[F],
  )

  /**
   * Compose the managed resources needed by [[AmendmentsProcessorFactory.build]]:
   *   - HikariCP-backed `Transactor[F]` against AlloyDB / Cloud SQL PostgreSQL,
   *   - a single rate-limited `Client[F]` for Congress.gov (one shared key across all five amendment + bill pipelines,
   *     per the existing `pageDelay` precedent),
   *   - a `RetryWrapper[F]` whose log callback is wired by the caller (see `AmendmentsPipeline.run`) so structured
   *     retry signals flow through the standard `PipelineLogger`.
   */
  def build[F[_]: Async](
    config: AmendmentsPipeline.AppConfig,
    transactorFactory: DatabaseConfig => Resource[F, Transactor[F]],
    httpClientFactory: Resource[F, Client[F]],
    retryWrapper: RetryWrapper[F],
  ): Resource[F, Resources[F]] =
    for {
      xa        <- transactorFactory(config.database)
      rawClient <- httpClientFactory
      client <- RateLimitedHttpClient.make[F](
        rawClient,
        pageDelay = config.congressApi.pageDelay,
        permits = 1L,
      )
    } yield Resources(xa, client, retryWrapper)

  /**
   * Delegate to [[PoolSizingValidator.validate]]. Lives here as well so existing call sites that import
   * `AmendmentsPipelineResources.validatePoolSizing` keep compiling; the actual logic lives in its own file so it stays
   * inside coverage even though this object is excluded as pure wiring.
   */
  def validatePoolSizing(
    database: DatabaseConfig,
    pipeline: AmendmentsConfig,
  ): Option[String] = PoolSizingValidator.validate(database, pipeline)

}
