package repcheck.members.committees.config

import pureconfig.ConfigReader

/** GovInfo API access for the Congressional Directory (CDIR) historical committee source. */
final case class GovInfoConfig(
  baseUrl: String,
  apiKey: String,
) derives ConfigReader
