package repcheck.ingestion.bills.textcheck.config

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

final case class BillTextCheckerConfig(
  parallelism: Int,
  eventPublishRetry: RetryConfig,
) derives ConfigReader
