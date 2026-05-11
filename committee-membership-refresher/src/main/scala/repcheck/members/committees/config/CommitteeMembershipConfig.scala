package repcheck.members.committees.config

import scala.concurrent.duration.FiniteDuration

import pureconfig.ConfigReader

final case class CommitteeMembershipConfig(
  parallelism: Int,
  requestTimeout: FiniteDuration,
  currentCongress: Int,
  houseMemberDataUrl: String,
  senateIdentityUrl: String,
  senateCommitteeBaseUrl: String,
) derives ConfigReader
