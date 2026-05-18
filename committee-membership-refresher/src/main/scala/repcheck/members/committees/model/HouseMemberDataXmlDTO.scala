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
