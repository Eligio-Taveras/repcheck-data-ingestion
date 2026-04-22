package repcheck.ingestion.votes.xml

/**
 * One row of the senate.gov `vote_menu_{congress}_{session}.xml` index feed. The index lists every roll-call vote in a
 * session with just enough metadata (number, date, question text, outcome) for the processor to decide whether an
 * individual vote XML needs to be fetched. `voteNumber` is the integer roll-call number (leading zeros from the XML
 * body are stripped during decoding).
 */
final case class SenateVoteIndexEntry(
  voteNumber: Int,
  voteDate: String,
  question: String,
  result: String,
)
