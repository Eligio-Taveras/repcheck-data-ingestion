package repcheck.ingestion.amendments.text.errors

import scala.annotation.tailrec

import repcheck.ingestion.common.errors.{HttpStatusError, HttpStatusErrorClassifier}
import repcheck.pipeline.models.errors.ErrorClass

/**
 * Classifies amendment-text download failures into Transient vs Systemic.
 *
 * Mirrors [[repcheck.ingestion.bills.summary.errors.BillSummariesApiErrorClassifier]] exactly:
 *
 *   - HTTP 429 (rate-limit) and 500/502/503/504 → Transient (retry per `RetryWrapper` backoff)
 *   - All other HTTP responses (401/403 — invalid api_key —, 404 already short-circuited, etc.) → Systemic (halt the
 *     run for operator attention)
 *   - Network-level exceptions (`java.io.IOException`, http4s Ember `EmberException`) → Transient via cause-chain walk
 *
 * `api.govinfo.gov` returns HTTP 429 when `X-Ratelimit-Remaining` reaches 0 — already routed to Transient. Real 401/403
 * means the API key is invalid; further retries can't fix that, so the run halts.
 */
object AmendmentTextDownloadErrorClassifier
    extends HttpStatusErrorClassifier[AmendmentTextDownloadHttpError](
      transientStatusCodes = Set(429, 500, 502, 503, 504)
    ) {

  override def classify(error: Throwable): ErrorClass =
    if (isTransientNetworkError(error)) {
      ErrorClass.Transient
    } else {
      super.classify(error)
    }

  /**
   * Walk the cause chain looking for a network-level transient exception. Recurses up to a depth-limit so a
   * pathological cause cycle can't infinite-loop the classifier. Mirrors the bill-summary classifier's helper.
   */
  @tailrec
  private[errors] def isTransientNetworkError(t: Throwable, depth: Int = 0): Boolean =
    if (t == null || depth > 16) {
      false
    } else {
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

}
