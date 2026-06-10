package repcheck.ingestion.amendments.textcheck.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

import com.repcheck.utils.errors.{ErrorClass, ErrorClassifier}

/**
 * Classifies HTTP failures from Congress.gov `/amendment/.../text` calls into Transient (worth retrying) vs Systemic
 * (halt) per the project-wide `HttpStatusErrorClassifier` contract. 5xx and 429 are Transient; everything else (4xx
 * auth / malformed-request errors, etc.) is Systemic.
 *
 * Wraps the base classifier with the shared `transientNetworkAware` helper from `ingestion-common 0.1.28+` — connection
 * drops, socket timeouts, ember stream failures, and IO errors get reclassified as Transient before the status-code
 * logic kicks in. Stops at the first `HttpStatusError` so a 401-with-IOException-cause classifies as Systemic (the auth
 * failure is authoritative).
 *
 * Distinct from [[repcheck.ingestion.amendments.errors.AmendmentsApiErrorClassifier]] (per §7.5 Q10): both call sites
 * hit api.congress.gov but the failure modes are scoped differently — `AmendmentsApiErrorClassifier` is the metadata
 * fetch path (§7.1), this one is the text-availability poll path (§7.5). Distinct names keep observability counters and
 * stack traces unambiguous about which call site failed.
 */
private object AmendmentTextCheckHttpStatusClassifier
    extends HttpStatusErrorClassifier[AmendmentTextCheckHttpError](
      transientStatusCodes = Set(429, 500, 502, 503, 504)
    )

object AmendmentTextCheckErrorClassifier extends ErrorClassifier {

  private val delegate: ErrorClassifier =
    HttpStatusErrorClassifier.transientNetworkAware(AmendmentTextCheckHttpStatusClassifier)

  override def classify(error: Throwable): ErrorClass =
    delegate.classify(error)

}
