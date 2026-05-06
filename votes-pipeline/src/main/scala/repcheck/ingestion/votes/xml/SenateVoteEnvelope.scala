package repcheck.ingestion.votes.xml

import repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO

/**
 * Pairs a decoded [[SenateVoteXmlDTO]] (shared-models) with the votes-pipeline-local [[SenateVoteAmendmentFields]] that
 * aren't represented on the shared DTO yet. The decoder produces this envelope so downstream callers (the converter,
 * the processor) get both pieces in one shape and don't re-walk the XML.
 */
final case class SenateVoteEnvelope(
  dto: SenateVoteXmlDTO,
  amendmentFields: SenateVoteAmendmentFields,
)
