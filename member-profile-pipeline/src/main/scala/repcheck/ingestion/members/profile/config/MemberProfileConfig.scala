package repcheck.ingestion.members.profile.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

final case class MemberProfileConfig(
  congress: Int,
  parallelism: Int,
  pageDelay: FiniteDuration,
  eventPublishRetry: RetryConfig,
) derives ConfigReader
