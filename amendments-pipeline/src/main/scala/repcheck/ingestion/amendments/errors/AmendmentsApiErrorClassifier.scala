package repcheck.ingestion.amendments.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

import com.repcheck.utils.errors.{ErrorClass, ErrorClassifier}

/**
 * Classifies HTTP failures from Congress.gov `/amendment` calls into Transient (worth retrying) vs Systemic (halt) per
 * the project-wide `HttpStatusErrorClassifier` contract. 5xx and 429 are Transient; everything else (4xx auth /
 * malformed-request errors, etc.) is Systemic.
 *
 * Wraps the base classifier with the shared `transientNetworkAware` helper from `ingestion-common 0.1.28+` — connection
 * drops, socket timeouts, ember stream failures, and IO errors get reclassified as Transient before the status-code
 * logic kicks in. Stops at the first `HttpStatusError` so a 401-with-IOException-cause classifies as Systemic (the auth
 * failure is authoritative).
 *
 * The wrapping helper replaces what would otherwise be a near-identical cause-chain walk in this file (mirrored from
 * the pre-0.1.28 inline version in `BillSummariesApiErrorClassifier`).
 */
private object AmendmentsApiHttpStatusClassifier
    extends HttpStatusErrorClassifier[AmendmentsApiHttpError](
      transientStatusCodes = Set(429, 500, 502, 503, 504)
    )

object AmendmentsApiErrorClassifier extends ErrorClassifier {

  private val delegate: ErrorClassifier =
    HttpStatusErrorClassifier.transientNetworkAware(AmendmentsApiHttpStatusClassifier)

  override def classify(error: Throwable): ErrorClass =
    delegate.classify(error)

}
