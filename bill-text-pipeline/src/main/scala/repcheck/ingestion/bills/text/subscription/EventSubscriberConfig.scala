package repcheck.ingestion.bills.text.subscription

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

/**
 * Pub/Sub subscriber configuration.
 *
 * `pullTimeout` is a defensive cap on every Pub/Sub Pull RPC. The Java SDK's blocking pull occasionally hangs
 * indefinitely against the local emulator (a known emulator quirk; see PR #83 follow-up notes). Wrapping each pull in
 * `IO.timeout(pullTimeout)` and treating a timeout as "subscription empty" lets the drain loop exit cleanly on stuck
 * pulls — Ofelia's next tick re-runs the pipeline and picks up any messages that did arrive. 30s is generous for a
 * healthy pull (typically returns in &lt;1s) but tight enough that operators notice when the SDK stalls.
 */
final case class EventSubscriberConfig(
  projectId: String,
  subscriptionId: String,
  maxMessages: Int,
  pullTimeout: FiniteDuration,
) derives ConfigReader
