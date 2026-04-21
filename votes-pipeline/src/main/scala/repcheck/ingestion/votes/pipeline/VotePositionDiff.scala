package repcheck.ingestion.votes.pipeline

import repcheck.shared.models.congress.vote.VoteCast

/**
 * A single per-member difference between two position lists for the same vote.
 *
 * Positions are matched by `memberId: Long` (the internal FK to `members.id`). That identity choice is intentional
 * (plan decision 11): a placeholder→real merge done by `lis-mapping-refresher` shows up on the next votes-pipeline run
 * as `Removed(placeholderId) + Added(realId)`, which is semantically accurate — the FK really did change, and
 * downstream consumers (scoring engine) need to reprocess. No `natural_key` column is added to `vote_positions` to
 * suppress that transition.
 *
 * `position` is the raw enum string value (via `.toString`), not `VoteCast`, because `Changed` is primarily a
 * logging/observability artifact and a stringly value keeps log output trivially human-readable without forcing the ADT
 * on every consumer.
 */
enum VotePositionDiff {

  /**
   * Member present in the incoming position list but absent from the stored list — a new voter for this roll call.
   */
  case Added(memberId: Long, voteCast: String) extends VotePositionDiff

  /**
   * Member present in the stored position list but absent from the incoming list — typically only happens on a
   * placeholder→real merge (see class docstring) or a rare upstream correction.
   */
  case Removed(memberId: Long, previousVoteCast: String) extends VotePositionDiff

  /**
   * Same member in both lists with a different cast — the canonical "member switched Yea→Nay" case that gates event
   * emission.
   */
  case Changed(memberId: Long, from: String, to: String) extends VotePositionDiff

}

object VotePositionDiff {

  /**
   * Render the position field of a [[repcheck.shared.models.congress.dos.vote.VotePositionDO]] as a log-friendly
   * string. `None` becomes the literal `"<none>"` so the absence is distinguishable in diff output from any real enum
   * value.
   */
  def castLabel(position: Option[VoteCast]): String =
    position.fold("<none>")(_.apiValue)

}
