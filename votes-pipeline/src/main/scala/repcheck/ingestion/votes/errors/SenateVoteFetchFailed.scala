package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusError

/**
 * Terminal exception raised by [[repcheck.ingestion.votes.xml.SenateVoteXmlClient]] when a Senate XML document cannot
 * be fetched or decoded after retries are exhausted. Carries congress/session context plus the optional vote number
 * (None for the index feed) so operators can correlate a failure back to a specific feed URL.
 *
 * When `cause` carries an HTTP status via [[HttpStatusError]] (for example a wrapped [[SenateVoteXmlHttpError]]), this
 * exception delegates `statusCode` to the cause so the votes-pipeline's error classifier can make retry decisions
 * uniformly across the chain.
 */
final case class SenateVoteFetchFailed(
  congress: Int,
  session: Int,
  voteNumber: Option[Int],
  detail: String,
  cause: Throwable,
) extends Exception(
      SenateVoteFetchFailed.buildMessage(congress, session, voteNumber, detail),
      cause,
    )
    with HttpStatusError {

  override def statusCode: Int = cause match {
    case h: HttpStatusError => h.statusCode
    case _                  => 0
  }

}

object SenateVoteFetchFailed {

  private def buildMessage(congress: Int, session: Int, voteNumber: Option[Int], detail: String): String = {
    val target = voteNumber match {
      case Some(n) => s"vote $congress-$session-$n"
      case None    => s"vote index $congress-$session"
    }
    s"Failed to fetch Senate $target: $detail"
  }

}
