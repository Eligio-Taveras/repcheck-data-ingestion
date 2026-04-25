package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier
import repcheck.pipeline.models.errors.ErrorClass

/**
 * Classifier for [[repcheck.ingestion.votes.api.HouseVotesApiClient]] HTTP failures. Layered on top of the shared
 * [[HttpStatusErrorClassifier]]:
 *
 *   1. The HTTP-status path inherits the standard Congress.gov transient set (429/500/502/503/504) — anything in that
 *      set is `Transient` and retried. 2. Network-level failures that don't carry an HTTP status code (TCP reset,
 *      server-side idle close, connect refused, socket timeout, http4s/ember "Reached End Of Stream While Reading") are
 *      also `Transient`. Without this layer they fell through to `Systemic` and the per-vote fetch was abandoned after
 *      one attempt — surfaced live during P6 docker-compose backfill where ~0.4% of `fetchMembersVotePositions` calls
 *      hit a transient stream close mid-body and dropped the vote.
 *
 * Anything else (4xx auth/validation/not-found, decoding failures, our own DTO→DO conversion errors, etc.) stays
 * `Systemic`.
 */
object HouseVoteApiErrorClassifier
    extends HttpStatusErrorClassifier[HouseVoteApiHttpError](Set(429, 500, 502, 503, 504)) {

  override def classify(error: Throwable): ErrorClass =
    super.classify(error) match {
      case ErrorClass.Transient => ErrorClass.Transient
      case ErrorClass.Systemic =>
        if (HouseVoteApiErrorClassifier.isTransientNetwork(error)) {
          ErrorClass.Transient
        } else {
          ErrorClass.Systemic
        }
    }

  /**
   * Recognise transient network errors thrown by http4s/ember/netty that don't carry an HTTP status code. Combines a
   * type-based match on the standard `java.net.*` exceptions with a substring match on the few http4s-specific
   * exception messages we've observed in the wild — http4s versions don't expose those classes in a stable way, so the
   * message check is the portable signal.
   */
  private[errors] def isTransientNetwork(t: Throwable): Boolean = {
    val msg = Option(t.getMessage).map(_.toLowerCase).getOrElse("")
    val byMessage =
      msg.contains("end of stream") ||
        msg.contains("connection reset") ||
        msg.contains("broken pipe") ||
        msg.contains("connection closed") ||
        msg.contains("read timed out") ||
        msg.contains("connection refused")
    val byType = t match {
      case _: java.net.SocketTimeoutException => true
      case _: java.net.ConnectException       => true
      case _                                  => false
    }
    byMessage || byType
  }

}
