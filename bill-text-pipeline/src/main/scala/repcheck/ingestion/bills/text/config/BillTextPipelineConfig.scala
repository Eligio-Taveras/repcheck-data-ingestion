package repcheck.ingestion.bills.text.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

final case class BillTextPipelineConfig(
  parallelism: Int,
  downloadTimeoutSeconds: Int,
  maxContentBytes: Long,
  pageDelay: FiniteDuration,
) derives ConfigReader
