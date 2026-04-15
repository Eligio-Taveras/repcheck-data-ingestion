package repcheck.ingestion.bills.text.subscription

import pureconfig.ConfigReader

final case class EventSubscriberConfig(
  projectId: String,
  subscriptionId: String,
  maxMessages: Int,
) derives ConfigReader
