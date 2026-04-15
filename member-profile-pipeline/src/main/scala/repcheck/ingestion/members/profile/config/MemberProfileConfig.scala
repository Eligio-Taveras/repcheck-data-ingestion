package repcheck.ingestion.members.profile.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

final case class MemberProfileConfig(
  parallelism: Int,
  pageDelay: FiniteDuration,
) derives ConfigReader
