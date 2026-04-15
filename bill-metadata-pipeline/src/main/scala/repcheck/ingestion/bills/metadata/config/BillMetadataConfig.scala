package repcheck.ingestion.bills.metadata.config

import pureconfig.ConfigReader

final case class BillMetadataConfig(
  lookbackDays: Int,
  parallelism: Int,
) derives ConfigReader
