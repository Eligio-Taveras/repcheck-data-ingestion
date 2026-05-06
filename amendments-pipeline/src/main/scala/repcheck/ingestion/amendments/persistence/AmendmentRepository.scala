package repcheck.ingestion.amendments.persistence

import scala.concurrent.duration.FiniteDuration

import repcheck.ingestion.common.placeholders.EntityRepository
import repcheck.shared.models.congress.dos.amendment.AmendmentDO

/**
 * Persistence trait for the `amendments` table. All methods return `F[_]` so callers can compose them with other
 * repositories inside a single transaction (typical `F = ConnectionIO`).
 *
 * Natural keys follow the `{congress}-{AMENDMENT_TYPE}-{number}` shape (e.g. `"117-SAMDT-2137"`) and address the row
 * via the `natural_key` UNIQUE column. The BIGSERIAL `id` is assigned by the database on first insert and returned via
 * the full DO from [[upsert]] so downstream tables (text versions, findings, alignments) can FK to it.
 *
 * Extends [[EntityRepository]] so the placeholder pattern (`PlaceholderCreator`) can create amendment rows when a
 * future pipeline (e.g., votes resolving an amendment-linked vote) discovers a forward reference. Amendments are
 * pre-wired here for consistency with bills and members even though no pipeline currently creates amendment
 * placeholders through `EntityRepository.insertIfNotExists` — the dedicated [[upsertPlaceholder]] entry point is what
 * the amendments pipeline itself uses for sub-amendment parents discovered mid-page.
 */
trait AmendmentRepository[F[_]] extends EntityRepository[F, AmendmentDO] {

  /**
   * Insert or update the row keyed by `natural_key`. Returns the row as stored — including the database-assigned `id`
   * (BIGSERIAL) and timestamps (`created_at`, `updated_at`). On conflict, every business field plus `updated_at` is
   * refreshed; `created_at` is left untouched so the original creation time survives an overwrite.
   */
  def upsert(amendment: AmendmentDO): F[AmendmentDO]

  /**
   * Idempotent placeholder insert. Parses `naturalKey` into `(congress, amendmentType, number)`, derives `chamber` from
   * `amendmentType` (HAMDT → House, SAMDT/SUAMDT → Senate), and writes a single row with `ON CONFLICT (natural_key) DO
   * NOTHING`. Repeated calls for the same natural key are no-ops; this mirrors `BillRepository.upsertPlaceholder` and
   * lets a sub-amendment processor reserve a parent FK target without racing the page-batch flush.
   *
   * Returns `F[Unit]` (not the row) so the caller is forced to round-trip through [[findByNaturalKey]] when it needs
   * the surrogate id — that keeps the SQL deterministic (one statement per call, no `RETURNING` branch). Raises
   * [[repcheck.ingestion.amendments.errors.InvalidAmendmentNaturalKey]] when the key is malformed.
   */
  def upsertPlaceholder(naturalKey: String): F[Unit]

  def findById(id: Long): F[Option[AmendmentDO]]

  def findByNaturalKey(naturalKey: String): F[Option[AmendmentDO]]

  /**
   * Page-batch helper: looks up `naturalKeys` in a single SQL `IN (...)` query and returns a map keyed by the natural
   * key. Empty input short-circuits to the empty map without issuing a query. Used by the amendments pipeline's
   * processor to fetch the prior version of every amendment in a page in one round trip before driving the change
   * detector.
   */
  def findByNaturalKeys(naturalKeys: List[String]): F[Map[String, AmendmentDO]]

  def findByBillId(billId: Long): F[List[AmendmentDO]]

  def findByCongress(congress: Int): F[List[AmendmentDO]]

  def findByParentAmendmentId(parentId: Long): F[List[AmendmentDO]]

  /**
   * Pre-filter that selects the change-detector candidate set for the amendment-text scheduler. The SQL is
   * intentionally denormalized: `congress >= minCongress` AND `amendment_type != SUAMDT` AND `(last_text_check_at IS
   * NULL OR last_text_check_at < NOW() - staleAfter)`. Sub-amendments (SUAMDT) never have their own text — text always
   * lives on the parent — so they're excluded here rather than at the processor.
   */
  def findCandidatesForTextCheck(minCongress: Int, staleAfter: FiniteDuration): F[List[AmendmentDO]]

  /**
   * Stamp `last_text_check_at = NOW()` on the row identified by `amendmentId`. Only invoked on the success path of the
   * text-check (per L1 in the plan); transient failures don't bump the column so the next sweep retries.
   */
  def updateLastTextCheckAt(amendmentId: Long): F[Unit]

}
