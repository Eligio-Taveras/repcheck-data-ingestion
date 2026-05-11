package repcheck.members.committees.model

/** House clerk `MemberData.xml` — one per `<member>` element. */
final case class HouseMemberDataXmlDTO(
  bioguideId: String,
  firstName: String,
  lastName: String,
  party: String,
  state: String,
  district: Option[Int],
  committees: List[HouseCommitteeAssignmentXmlDTO],
)

/** Nested under a House member — one per `<committee>` or `<subcommittee>`. */
final case class HouseCommitteeAssignmentXmlDTO(
  committeeCode: String,
  committeeName: String,
  rank: Option[Int],
  side: Option[String],
)

/** Senate `cvc_member_data.xml` identity record — one per `<senator>`. */
final case class SenatorIdentityXmlDTO(
  bioguideId: String,
  lisMemberId: String,
  firstName: String,
  lastName: String,
  party: String,
  state: String,
)

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

/** Congress.gov `/v3/committee/{chamber}` list item. */
final case class CommitteeListItemDTO(
  systemCode: String,
  name: String,
  chamber: String,
  committeeTypeCode: Option[String],
  updateDate: Option[String],
  url: Option[String],
  parent: Option[CommitteeParentDTO],
  subcommittees: Option[List[CommitteeSubcommitteeDTO]],
)

final case class CommitteeParentDTO(
  systemCode: Option[String],
  name: Option[String],
)

final case class CommitteeSubcommitteeDTO(
  systemCode: Option[String],
  name: Option[String],
)
