package repcheck.members.committees.model

/**
 * Aggregate outcome of a historical committee-membership load. `skippedNoMember` counts assignments whose CDIR
 * name+state didn't resolve to a member of that congress; `skippedNoCommittee` counts ones whose committee name didn't
 * match a row in the DB.
 */
final case class HistoricalLoadResult(
  assignmentsSeen: Int,
  upserted: Int,
  skippedNoMember: Int,
  skippedNoCommittee: Int,
) {

  def combine(other: HistoricalLoadResult): HistoricalLoadResult =
    HistoricalLoadResult(
      assignmentsSeen = assignmentsSeen + other.assignmentsSeen,
      upserted = upserted + other.upserted,
      skippedNoMember = skippedNoMember + other.skippedNoMember,
      skippedNoCommittee = skippedNoCommittee + other.skippedNoCommittee,
    )

}

object HistoricalLoadResult {
  val empty: HistoricalLoadResult = HistoricalLoadResult(0, 0, 0, 0)

  def single(upserted: Boolean, noMember: Boolean, noCommittee: Boolean): HistoricalLoadResult =
    HistoricalLoadResult(
      assignmentsSeen = 1,
      upserted = if (upserted) 1 else 0,
      skippedNoMember = if (noMember) 1 else 0,
      skippedNoCommittee = if (noCommittee) 1 else 0,
    )

}
