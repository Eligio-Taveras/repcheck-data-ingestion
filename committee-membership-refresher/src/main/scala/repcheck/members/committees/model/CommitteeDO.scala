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

object CommitteeDO {

  def forInsert(
    naturalKey: String,
    name: String,
    chamber: String,
    committeeType: Option[String],
    parentCommitteeId: Option[Long],
    url: Option[String],
    updateDate: Option[Instant],
    isCurrent: Option[Boolean],
  ): CommitteeDO =
    CommitteeDO(
      id = 0L,
      naturalKey = naturalKey,
      name = name,
      chamber = chamber,
      committeeType = committeeType,
      parentCommitteeId = parentCommitteeId,
      url = url,
      updateDate = updateDate,
      isCurrent = isCurrent,
      createdAt = None,
      updatedAt = None,
    )

}
