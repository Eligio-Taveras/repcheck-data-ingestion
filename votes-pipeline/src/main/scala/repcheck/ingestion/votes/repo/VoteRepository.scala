package repcheck.ingestion.votes.repo

import doobie.ConnectionIO

import repcheck.shared.models.congress.dos.vote.VoteDO

/**
 * Persistence trait for the `votes` table. All methods return Doobie `ConnectionIO` so callers can compose them with
 * other repositories (e.g., [[VotePositionRepository]] and [[VoteHistoryArchiver]]) inside a single transaction.
 *
 * The repository is natural-key addressable via `findByNaturalKey`: the caller constructs a
 * `{chamber}:{congress}:{session}:{rollNumber}` string per plan decision 20 and the repository's `upsert` rewrites the
 * row on `ON CONFLICT (natural_key)`. The BIGSERIAL `id` is assigned by the database on first insert and returned to
 * the caller so downstream tables (`vote_positions`, `stance_materialization_status`) can reference the row.
 */
trait VoteRepository {
  def findByNaturalKey(naturalKey: String): ConnectionIO[Option[VoteDO]]
  def findByBillId(billId: Long): ConnectionIO[List[VoteDO]]
  def findByCongress(congress: Int): ConnectionIO[List[VoteDO]]

  /**
   * Insert or update the row keyed by `natural_key`. Returns the row as stored, including the database-assigned `id`
   * (BIGSERIAL) and timestamps (`created_at`, `updated_at`). On conflict, all business fields and `updated_at` are
   * refreshed — the `created_at` column is left untouched so the original creation time survives an overwrite.
   */
  def upsert(vote: VoteDO): ConnectionIO[VoteDO]
}
