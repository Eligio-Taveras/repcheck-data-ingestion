package repcheck.members.committees.model

/** Senate per-committee XML — one entry per `<member>` in the committee roster. */
final case class SenateCommitteeMemberXmlDTO(
  bioguideId: Option[String],
  firstName: String,
  lastName: String,
  state: String,
  party: String,
  position: Option[String],
  rank: Option[Int],
  committeeCode: String,
  isSubcommittee: Boolean,
)
