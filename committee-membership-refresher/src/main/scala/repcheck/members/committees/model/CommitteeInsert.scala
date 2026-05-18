package repcheck.members.committees.model

import java.time.Instant

final case class CommitteeInsert(
  naturalKey: String,
  name: String,
  chamber: String,
  committeeType: Option[String],
  parentCommitteeId: Option[Long],
  url: Option[String],
  updateDate: Option[Instant],
  isCurrent: Option[Boolean],
)
