package repcheck.members.common.persistence

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.MemberLisMappingDO

/**
 * Read/write contract over the `member_lis_mapping` join table. The table stores a `member_id` (BIGINT FK to
 * `members.id`) paired with a `lis_member_id` (BIGINT FK to `lis_members.id`). Writers come from the
 * `lis-mapping-refresher` pipeline; readers come from the votes pipeline (Senate path, which needs to resolve
 * senate.gov LIS ids into Congress.gov bioguide-keyed `members.id` rows before upserting vote positions).
 */
trait LisMappingRepository {
  def upsert(mapping: MemberLisMappingDO): ConnectionIO[UpsertResult]
  def upsertBatch(mappings: List[MemberLisMappingDO]): ConnectionIO[List[UpsertResult]]
  def findByLisMemberId(lisMemberId: Long): ConnectionIO[Option[MemberLisMappingDO]]
  def findByMemberId(memberId: Long): ConnectionIO[Option[MemberLisMappingDO]]

  /**
   * Batch lookup: resolve many `lis_member_id`s to their corresponding `member_id`s in a single SQL query. Returns a
   * `Map[lisMemberId, memberId]` containing only the ids that have a mapping row — callers decide how to handle the
   * unmapped remainder.
   *
   * An empty input short-circuits with `Map.empty` without touching the database.
   */
  def findByLisMemberIds(lisMemberIds: List[Long]): ConnectionIO[Map[Long, Long]]
}
