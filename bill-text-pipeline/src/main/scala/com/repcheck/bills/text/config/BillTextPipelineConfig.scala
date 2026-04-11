package com.repcheck.bills.text.config

import pureconfig.ConfigReader

final case class BillTextPipelineConfig(
  parallelism: Int,
  downloadTimeoutSeconds: Int,
  maxContentBytes: Long,
) derives ConfigReader
