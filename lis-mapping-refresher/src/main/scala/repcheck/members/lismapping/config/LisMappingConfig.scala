package repcheck.members.lismapping.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

final case class LisMappingConfig(
  parallelism: Int,
  requestTimeout: FiniteDuration,
  currentCongress: Int,
  congressLookbackWindow: Int,
  senatorXmlUrl: String,
) derives ConfigReader
