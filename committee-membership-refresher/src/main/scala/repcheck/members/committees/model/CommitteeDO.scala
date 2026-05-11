package repcheck.members.committees.model

import java.time.Instant

/**
 * Domain object for the `committees` table. `committeeCode` is the natural primary key stored in clerk format
 * (uppercase, e.g., "SSFI00", "RU00", "JJEC00").
 */
final case class CommitteeDO(
  committeeCode: String,
  name: String,
  chamber: String,
  committeeType: String,
  parentCommitteeCode: Option[String],
  isCurrent: Boolean,
  updateDate: Option[Instant],
  createdAt: Option[Instant],
  updatedAt: Option[Instant],
)
