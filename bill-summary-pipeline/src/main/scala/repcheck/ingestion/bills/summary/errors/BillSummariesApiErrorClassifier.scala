package repcheck.ingestion.bills.summary.errors

import scala.annotation.tailrec

import repcheck.ingestion.common.errors.{HttpStatusError, HttpStatusErrorClassifier}
import repcheck.pipeline.models.errors.ErrorClass

/**
 * Classifies HTTP failures from Congress.gov `/summaries` calls into Transient (worth retrying) vs Systemic (halt) per
 * the project-wide `HttpStatusErrorClassifier` contract. 5xx and 429 are Transient; everything else (4xx auth /
 * malformed-request errors, etc.) is Systemic.
 *
 * Additionally classifies **network-level transients** as Transient by walking the cause chain — connection drops
 * mid-stream (`org.http4s.ember.core.EmberException` and subclasses, e.g. `ReachedEndOfStream` when the server closes
 * the socket part-way through a 250-row response) and IO errors (`java.io.IOException` and subclasses, including
 * SocketTimeout / ConnectException) all retry instead of killing the run.
 *
 * The classifier walks `getCause` recursively because `BillSummaryFetchFailed` wraps the underlying network error; the
 * cause chain is `BillSummaryFetchFailed -> EmberException -> ...` and the classifier needs to see past the wrapper.
 */
object BillSummariesApiErrorClassifier
    extends HttpStatusErrorClassifier[BillSummariesApiHttpError](
      transientStatusCodes = Set(429, 500, 502, 503, 504)
    ) {

  override def classify(error: Throwable): ErrorClass =
    if (isTransientNetworkError(error)) ErrorClass.Transient
    else super.classify(error)

  /**
   * Walk the cause chain looking for a network-level transient exception. Recurses up to a depth-limit so a
   * pathological cause cycle can't infinite-loop the classifier.
   */
  @tailrec
  private def isTransientNetworkError(t: Throwable, depth: Int = 0): Boolean =
    if (t == null || depth > 16) false
    else
      t match {
        // SocketTimeoutException / ConnectException both extend IOException, so the IOException
        // case below catches them too — listing them separately would fire an "Unreachable case"
        // compiler warning.
        case _: java.io.IOException                  => true
        case _: org.http4s.ember.core.EmberException => true
        case _: HttpStatusError                      => false
        case other if other.getCause != null && other.getCause != other =>
          isTransientNetworkError(other.getCause, depth + 1)
        case _ => false
      }

}
