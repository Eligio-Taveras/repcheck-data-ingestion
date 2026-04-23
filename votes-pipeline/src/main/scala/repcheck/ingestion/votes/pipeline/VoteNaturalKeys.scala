package repcheck.ingestion.votes.pipeline

import repcheck.ingestion.votes.xml.SenateVoteIndexEntry
import repcheck.shared.models.congress.dto.conversions.VoteConversions
import repcheck.shared.models.congress.dto.vote.{SenateVoteXmlDTO, VoteListItemDTO, VoteMembersDTO}

/**
 * Pure builders for vote + bill natural keys used by [[VoteProcessor]] on the chamber boundaries.
 *
 * Two groups:
 *   - Vote natural keys (`houseVote`, `senateVote`) — delegate to [[VoteConversions.buildVoteNaturalKey]] so every
 *     writer and every change-detection lookup produces the same string.
 *   - Bill natural keys (`houseBill`, `senateBill`) — used for event emission bookkeeping when the converter's
 *     `billNaturalKey` is the same or absent. `senateBill` always returns `None` because the Senate converter's
 *     `classifyDocument` is authoritative for Senate bill keys — this function exists solely to keep the two chamber
 *     paths symmetric in [[VoteProcessor.processHouseVote]] / [[VoteProcessor.processSenateVote]].
 */
private[pipeline] object VoteNaturalKeys {

  /**
   * Build the House vote natural key (`"{congress}-House-{session}-{rollCall}"`). When the DTO's `sessionNumber` is
   * absent, fall back to the processor's configured default (ordinarily the session passed to
   * [[VoteProcessor.streamAll]]).
   */
  def houseVote(listItem: VoteListItemDTO, fallbackSession: Int): String =
    VoteConversions.buildVoteNaturalKey(
      congress = listItem.congress,
      chamber = "House",
      session = listItem.sessionNumber.getOrElse(fallbackSession),
      rollCallNumber = listItem.rollCallNumber,
    )

  /**
   * Build the Senate vote natural key. Senate index entries don't carry congress/session themselves — they are fetched
   * for a specific `(congress, session)`, so the caller threads both values through.
   */
  def senateVote(entry: SenateVoteIndexEntry, congress: Int, session: Int): String =
    VoteConversions.buildVoteNaturalKey(
      congress = congress,
      chamber = "Senate",
      session = session,
      rollCallNumber = entry.voteNumber,
    )

  /**
   * Derive the House bill natural key from the DTO's legislation fields when both are populated. Returns `None` for
   * procedural votes. The `legislationType` is upper-cased to match the canonical `{congress}-{TYPE}-{number}` shape.
   */
  def houseBill(dto: VoteMembersDTO): Option[String] =
    for {
      t <- dto.legislationType
      n <- dto.legislationNumber
    } yield s"${dto.congress.toString}-${t.toUpperCase}-$n"

  /**
   * For Senate votes, the bill natural key is produced inside the converter's [[SenateVoteConverter.classifyDocument]]
   * and attached to the [[repcheck.shared.models.congress.dos.results.VoteConversionResult]] directly. This helper is
   * the no-op hook that keeps the chamber paths symmetric — the processor calls it but always gets `None` back, then
   * prefers `conversion.billNaturalKey`.
   */
  def senateBill(dto: SenateVoteXmlDTO): Option[String] = {
    val _ = dto
    None
  }

}
