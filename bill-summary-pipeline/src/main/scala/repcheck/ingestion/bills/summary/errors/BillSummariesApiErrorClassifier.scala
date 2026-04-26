package repcheck.ingestion.bills.summary.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifies HTTP failures from Congress.gov `/summaries` calls into Transient (worth retrying) vs Systemic (halt) per
 * the project-wide `HttpStatusErrorClassifier` contract. 5xx and 429 are Transient; everything else (4xx auth /
 * malformed-request errors, etc.) is Systemic.
 */
object BillSummariesApiErrorClassifier
    extends HttpStatusErrorClassifier[BillSummariesApiHttpError](
      transientStatusCodes = Set(429, 500, 502, 503, 504)
    )
