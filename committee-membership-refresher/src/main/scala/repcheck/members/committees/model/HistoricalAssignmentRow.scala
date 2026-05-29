package repcheck.members.committees.model

/**
 * One row of the canonical historical committee-assignment file: a single member's membership on one committee during
 * one Congress. This is the normalized intermediate the [[repcheck.members.committees.pipeline.CommitteeHistoryLoader]]
 * consumes; its tab-separated layout is specified on
 * [[repcheck.members.committees.client.HistoricalAssignmentTsvReader]]. Producing it from a primary source (e.g. the
 * Stewart Congressional Committee Assignments dataset — mapping ICPSR → bioguide via congress-legislators and the
 * source committee codes → our natural keys) is a one-time data-prep step done outside this pipeline.
 */
final case class HistoricalAssignmentRow(
  congress: Int,
  chamber: String,
  committeeCode: String,
  committeeName: Option[String],
  committeeType: Option[String],
  bioguideId: String,
  role: Option[String],
  rank: Option[Int],
)
