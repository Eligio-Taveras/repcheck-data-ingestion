package repcheck.ingestion.votes.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.votes.xml.SenateVoteXmlClient]] HTTP failures. Pure wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the senate.gov transient status set (429/500/502/503/504). `classify` is
 * inherited; [[SenateVoteXmlHttpError]] provides `statusCode` via [[repcheck.ingestion.common.errors.HttpStatusError]].
 * Matches the status set used by the Congress.gov classifiers in bills-pipeline / bill-text-availability-checker so
 * operational retry behavior is consistent across the data- ingestion repo.
 */
object SenateVoteXmlErrorClassifier
    extends HttpStatusErrorClassifier[SenateVoteXmlHttpError](Set(429, 500, 502, 503, 504))
