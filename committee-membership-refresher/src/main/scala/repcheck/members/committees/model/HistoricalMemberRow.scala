package repcheck.members.committees.model

/** A member who served in a given congress, with the name/state used to resolve CDIR committee-listing entries. */
final case class HistoricalMemberRow(
  firstName: Option[String],
  lastName: Option[String],
  stateName: Option[String],
  memberId: Long,
)
