package repcheck.ingestion.bills.textcheck.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.bills.textcheck.api.BillTextApiClient]] HTTP failures. Pure wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504). The `classify`
 * logic is inherited from the base; [[BillTextApiHttpError]] provides `statusCode` via
 * [[repcheck.ingestion.common.errors.HttpStatusError]].
 */
object BillTextApiErrorClassifier extends HttpStatusErrorClassifier[BillTextApiHttpError](Set(429, 500, 502, 503, 504))
