package repcheck.ingestion.amendments.text.subscription

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Pub/Sub subscriber configuration for the amendment-text-pipeline. Same shape as the bill-side subscriber's
 * `EventSubscriberConfig`. `pullTimeout` is a defensive cap on every Pull RPC — if the SDK stalls (a known emulator
 * quirk on the bill-side, expected to repeat for amendments) we treat the timeout as "subscription empty" and exit
 * cleanly so the next Ofelia tick re-runs the pipeline.
 */
final case class EventSubscriberConfig(
  projectId: String,
  subscriptionId: String,
  maxMessages: Int,
  pullTimeout: FiniteDuration,
) derives ConfigReader
