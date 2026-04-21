package repcheck.ingestion.bills.metadata.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.bills.metadata.api.BillsApiClient]] HTTP failures. Pure wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504). The `classify`
 * logic is inherited from the base; [[BillsApiHttpError]] provides `statusCode` via
 * [[repcheck.ingestion.common.errors.HttpStatusError]].
 */
object BillsApiErrorClassifier extends HttpStatusErrorClassifier[BillsApiHttpError](Set(429, 500, 502, 503, 504))
