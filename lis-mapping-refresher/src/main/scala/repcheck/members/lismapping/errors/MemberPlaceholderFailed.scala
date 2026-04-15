package repcheck.members.lismapping.errors

final case class MemberPlaceholderFailed(
  bioguideId: String,
) extends Exception(s"Failed to create placeholder for member $bioguideId")
