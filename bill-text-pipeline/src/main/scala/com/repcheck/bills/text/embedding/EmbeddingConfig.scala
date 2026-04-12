package com.repcheck.bills.text.embedding

import pureconfig.ConfigReader

final case class EmbeddingConfig(
  baseUrl: String,
  modelName: String,
  dimensions: Int,
  timeoutSeconds: Int,
) derives ConfigReader
