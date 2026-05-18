package repcheck.members.committees.model

/** Nested under a House member — one per `<committee>` or `<subcommittee>`. */
final case class HouseCommitteeAssignmentXmlDTO(
  committeeCode: String,
  committeeName: String,
  rank: Option[Int],
  side: Option[String],
)
