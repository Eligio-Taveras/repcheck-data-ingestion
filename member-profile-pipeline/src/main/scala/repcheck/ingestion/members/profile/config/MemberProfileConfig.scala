package repcheck.ingestion.members.profile.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

final case class MemberProfileConfig(
  congress: Int,
  parallelism: Int,
  pageDelay: FiniteDuration,
) derives ConfigReader
