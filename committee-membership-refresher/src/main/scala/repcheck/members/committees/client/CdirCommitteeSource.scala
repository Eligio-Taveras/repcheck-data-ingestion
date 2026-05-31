package repcheck.members.committees.client

/**
 * Source of plain-text committee-listing granules for a congress. Implemented by [[GovInfoCdirClient]] against the
 * GovInfo Congressional Directory; abstracted so the loader can be tested without HTTP.
 */
trait CdirCommitteeSource[F[_]] {

  /** House/Senate/Joint committee-listing texts for the directory edition covering `congress` (empty if none found). */
  def committeeListingTexts(congress: Int, runId: Long): F[List[String]]
}
