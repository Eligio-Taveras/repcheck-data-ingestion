package repcheck.members.committees.config

import pureconfig.ConfigReader

/** Config for the one-time historical committee-membership load. `filePath` points at the canonical TSV. */
final case class HistoricalLoaderConfig(
  filePath: String,
  parallelism: Int,
) derives ConfigReader
