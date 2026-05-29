package repcheck.members.committees.model

/**
 * Aggregate outcome of a historical committee-membership load. `upserted` counts rows written; `skippedNoMember` counts
 * assignments whose bioguide_id was absent from the members table (that Congress's member profiles must be backfilled
 * first); `parseErrors` counts malformed CSV lines.
 */
final case class HistoricalLoadResult(
  rowsRead: Int,
  upserted: Int,
  skippedNoMember: Int,
  parseErrors: Int,
) {

  def combine(other: HistoricalLoadResult): HistoricalLoadResult =
    HistoricalLoadResult(
      rowsRead = rowsRead + other.rowsRead,
      upserted = upserted + other.upserted,
      skippedNoMember = skippedNoMember + other.skippedNoMember,
      parseErrors = parseErrors + other.parseErrors,
    )

}

object HistoricalLoadResult {
  val empty: HistoricalLoadResult = HistoricalLoadResult(0, 0, 0, 0)
}
