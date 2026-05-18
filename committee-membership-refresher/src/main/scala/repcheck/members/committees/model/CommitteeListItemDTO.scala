package repcheck.members.committees.model

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
