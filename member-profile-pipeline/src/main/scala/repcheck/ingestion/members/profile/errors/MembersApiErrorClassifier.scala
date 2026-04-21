package repcheck.ingestion.members.profile.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.members.profile.api.MembersApiClient]] HTTP failures. Pure wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504). The `classify`
 * logic is inherited from the base; [[MembersApiHttpError]] provides `statusCode` via
 * [[repcheck.ingestion.common.errors.HttpStatusError]].
 */
object MembersApiErrorClassifier extends HttpStatusErrorClassifier[MembersApiHttpError](Set(429, 500, 502, 503, 504))
