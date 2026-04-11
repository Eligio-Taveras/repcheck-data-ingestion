package com.repcheck.bills.textcheck.config

import pureconfig.ConfigReader

final case class BillTextCheckerConfig(
  parallelism: Int,
) derives ConfigReader
