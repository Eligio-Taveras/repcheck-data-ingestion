package repcheck.ingestion.amendments.persistence

/**
 * Read-only lookup over the `committees` table for amendment sponsor resolution. Committee-sponsored amendments carry a
 * Congress.gov committee URL (`/v3/committee/{chamber}/{systemCode}`); the processor parses the systemCode and resolves
 * it to a `committees.id` FK here. Deliberately separate from the committee pipeline's write-owning repository — this
 * module only reads and never creates committee rows.
 */
trait CommitteeLookupRepository[F[_]] {

  /**
   * Resolve a Congress.gov committee `systemCode` (e.g. `hsru00`) to the surrogate `committees.id`. Matches against the
   * systemCode embedded in `committees.url` rather than `natural_key`: the clerk-format natural key drops the chamber
   * prefix for House committees (`hsru00` -> `RU00`) but keeps it for Senate (`ssra00` -> `SSRA00`), so the URL path
   * segment is the one representation that round-trips unchanged from the sponsor payload. Yields `None` when no
   * committee matches — the amendment row keeps a NULL FK and the run continues.
   */
  def findIdBySystemCode(systemCode: String): F[Option[Long]]

}
