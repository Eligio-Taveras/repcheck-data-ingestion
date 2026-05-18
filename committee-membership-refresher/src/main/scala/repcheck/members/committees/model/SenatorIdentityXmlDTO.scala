package repcheck.members.committees.model

/** Senate `cvc_member_data.xml` identity record — one per `<senator>`. */
final case class SenatorIdentityXmlDTO(
  bioguideId: String,
  lisMemberId: String,
  firstName: String,
  lastName: String,
  party: String,
  state: String,
)
