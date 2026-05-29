package repcheck.members.committees.model

/**
 * One parsed committee assignment from a GovInfo Congressional Directory (CDIR) committee-listing granule: a member
 * (display first/last + home state) on a given committee for the package's congress. `role` is Chair/Vice Chair/Ranking
 * when the directory marks it. The member is identified by display name + state here; the loader resolves that to a
 * `members.bioguide_id`, and `committeeName` to a `committees.natural_key`.
 */
final case class CdirAssignment(
  committeeName: String,
  isSubcommittee: Boolean,
  firstName: String,
  lastName: String,
  state: String,
  role: Option[String],
)
