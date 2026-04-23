package repcheck.ingestion.votes.xml

/**
 * URL construction helpers for senate.gov's Senate roll-call resources.
 *
 * Senate.gov publishes two distinct URL hierarchies under `.../legislative/LIS`:
 *
 *   - Per-session vote INDEX: `{baseUrl}/roll_call_lists/vote_menu_{congress}_{session}.xml` — a `<vote_summary>`
 *     document listing every roll-call in the session. Source of truth for `SenateVoteXmlClient.fetchVoteIndex`.
 *   - Per-vote XML: `{baseUrl}/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{voteNumber:05d}.xml`
 *     — the `<roll_call_vote>` document for a single vote, with every senator's cast. Source of truth for
 *     `SenateVoteXmlClient.fetchVote` AND `SenateVoteConverter.buildVoteDO` (as `sourceDataUrl`).
 *
 * Note the two DIFFERENT sub-paths: `roll_call_lists` for the index, `roll_call_votes` for individual votes. The
 * per-vote path also concatenates congress+session WITHOUT an underscore in the segment name (`vote1191`, not
 * `vote_menu_119_1`). These are senate.gov conventions — they are NOT symmetric, and hard-coding one pattern for both
 * breaks real-world fetches against senate.gov (every production call to the wrong path redirects to
 * `file_not_found.htm` with a 302). Verified against live senate.gov for vote 119/1/648 on 2026-04-22.
 *
 * `baseUrl` must point at the shared root (`https://www.senate.gov/legislative/LIS`). Tests override it to point at a
 * WireMock server, still respecting the two sub-paths.
 *
 * Vote numbers are zero-padded to five digits via `f"%05d"`. The client validates the range `[1, 99999]` up-front so
 * this helper does not need to defend against out-of-range inputs.
 */
private[votes] object SenateVoteUrls {

  /**
   * Assemble the per-vote XML URL for a single senate.gov roll-call.
   */
  def voteXmlUrl(baseUrl: String, congress: Int, session: Int, voteNumber: Int): String = {
    val padded = f"$voteNumber%05d"
    s"$baseUrl/roll_call_votes/vote${congress.toString}${session.toString}/" +
      s"vote_${congress.toString}_${session.toString}_$padded.xml"
  }

  /**
   * Assemble the per-session index URL.
   */
  def voteIndexUrl(baseUrl: String, congress: Int, session: Int): String =
    s"$baseUrl/roll_call_lists/vote_menu_${congress.toString}_${session.toString}.xml"

}
