package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.votes.api.HouseVotesApiClient]] HTTP failures. Pure wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504). The `classify`
 * logic is inherited from the base; [[HouseVoteApiHttpError]] provides `statusCode` via
 * [[repcheck.ingestion.common.errors.HttpStatusError]], which lets the base `classify` return `Transient` for the
 * listed codes and `Systemic` for everything else (4xx auth/validation/not-found, decoding failures, network errors
 * that don't wrap a status code, etc.).
 */
object HouseVoteApiErrorClassifier
    extends HttpStatusErrorClassifier[HouseVoteApiHttpError](Set(429, 500, 502, 503, 504))
