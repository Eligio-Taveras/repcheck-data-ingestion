package com.repcheck.bills.textcheck.app

import cats.effect.{ExitCode, IO, IOApp, Resource, Sync}

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import doobie.util.transactor.Transactor

import pureconfig.ConfigSource

import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.events.{PubSubEventPublisher, PubSubPublisherResource}
import repcheck.ingestion.common.logging.PipelineLoggerFactory

import com.repcheck.bills.textcheck.app.BillTextCheckerPipeline.AppConfig

object BillTextAvailabilityCheckerApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val _ = args
    BillTextCheckerPipeline.runWithFactories[IO](
      configLoader = Sync[IO].delay(ConfigSource.default.loadOrThrow[AppConfig]),
      loggerFactory = (name: String) => PipelineLoggerFactory.make[IO](name),
      resourceBuilder = buildResources,
      checkerFactory = BillTextCheckerPipeline.buildChecker[IO],
    )
  }

  private def buildResources(
    config: AppConfig
  ): Resource[IO, (Transactor[IO], Client[IO], PubSubEventPublisher[IO])] =
    for {
      xa              <- TransactorResource.make[IO](config.database)
      httpClient      <- EmberClientBuilder.default[IO].build
      pubSubPublisher <- PubSubPublisherResource.make[IO](config.eventPublisher)
    } yield (xa, httpClient, pubSubPublisher)

}
