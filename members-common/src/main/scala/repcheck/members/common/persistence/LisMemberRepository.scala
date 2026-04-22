package repcheck.members.common.persistence

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.member.LisMemberDO

/**
 * Read/write contract over the `lis_members` table. A `lis_members` row represents a senator identified by the opaque
 * senate.gov LIS identifier (the natural key) — e.g. `"S428"` — along with the latest first/last name, party, and state
 * observed from the senate.gov feeds.
 *
 * Writers:
 *   - `votes-pipeline` upserts one row per senator observed on each roll-call XML (see
 *     `repcheck.ingestion.votes.lis.LisResolver`). This happens BEFORE any `members` row or `member_lis_mapping` row
 *     exists for the senator — the votes pipeline never touches those tables.
 *   - `lis-mapping-refresher` upserts one row per senator in the senator-lookup XML feed to keep `last_verified` fresh.
 *
 * Readers:
 *   - `vote-positions` writers in the votes pipeline resolve `lis_members.id` from the upsert return value and write it
 *     as the row's `lis_member_id` (Senate side of the dual-identity check constraint per migration 023).
 */
trait LisMemberRepository {

  /**
   * Upsert a `lis_members` row keyed by `naturalKey`. On conflict with an existing natural key, the mutable fields
   * (first_name, last_name, party, state, last_verified) are overwritten with the incoming values. Returns the
   * surrogate BIGSERIAL `lis_members.id` — stable across upserts because `ON CONFLICT DO UPDATE` preserves the existing
   * row's id.
   */
  def upsertByNaturalKey(lisMemberDO: LisMemberDO): ConnectionIO[Long]

  /** Look up a single `lis_members` row by its natural key (senate.gov LIS id, e.g. `"S428"`). */
  def findByNaturalKey(naturalKey: String): ConnectionIO[Option[LisMemberDO]]

  /** Look up a single `lis_members` row by its surrogate `id`. */
  def findById(id: Long): ConnectionIO[Option[LisMemberDO]]

  /**
   * Batch lookup: resolve many natural keys to their surrogate `lis_members.id`s in a single SQL query. Returns a
   * `Map[naturalKey, id]` containing only the keys that have a row — callers decide how to handle the unmapped
   * remainder. An empty input short-circuits with `Map.empty` without touching the database.
   */
  def findByNaturalKeys(naturalKeys: List[String]): ConnectionIO[Map[String, Long]]
}
