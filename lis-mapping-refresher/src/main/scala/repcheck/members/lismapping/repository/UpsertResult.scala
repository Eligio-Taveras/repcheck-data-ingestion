package repcheck.members.lismapping.repository

/**
 * Distinguishes insert from update when upserting LIS member mappings. PostgreSQL's `xmax` system column determines the
 * result: `xmax = 0` indicates a newly inserted row, `xmax > 0` indicates an update to an existing row.
 *
 * The distinction drives downstream event emission: `Inserted` means a senator just gained a mapping and may trigger a
 * `member.updated` event, while `Updated` means the mapping was refreshed and no event is needed.
 */
sealed trait UpsertResult

object UpsertResult {
  case object Inserted extends UpsertResult
  case object Updated  extends UpsertResult
}
