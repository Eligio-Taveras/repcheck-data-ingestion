package repcheck.members.committees.model

import java.time.Instant

/**
 * Domain object for the `committees` table. Field order matches the explicit SELECT column list in
 * DoobieCommitteeRepository (Doobie maps positionally).
 */
final case class CommitteeDO(
  id: Long,
  naturalKey: String,
  name: String,
  chamber: String,
  committeeType: Option[String],
  parentCommitteeId: Option[Long],
  url: Option[String],
  updateDate: Option[Instant],
  isCurrent: Option[Boolean],
  createdAt: Option[Instant],
  updatedAt: Option[Instant],
)
