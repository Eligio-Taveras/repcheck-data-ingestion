package repcheck.ingestion.amendments.pipeline

/**
 * Extracts the Congress.gov committee `systemCode` from a committee-sponsor URL. A committee-sponsored amendment
 * carries a sponsor payload shaped like `{"name": "Rules Committee", "url":
 * ".../v3/committee/house/hsru00?format=json"}`; the final path segment (`hsru00`) is the stable, chamber-encoded key.
 * The `committees` table stores the same URL, so the systemCode is what resolves a sponsor to a `committees.id` — see
 * [[CommitteeLookupRepository]].
 */
object CommitteeSponsorSystemCode {

  private val Pattern = """/committee/[a-z]+/([a-z0-9]+)""".r

  def fromUrl(url: String): Option[String] =
    Pattern.findFirstMatchIn(url).map(_.group(1))

}
