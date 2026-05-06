package repcheck.ingestion.votes.xml

/**
 * Amendment-specific top-level fields that appear on a senate.gov `<roll_call_vote>` document when the vote is on an
 * amendment (e.g. `<document_type>S.Amdt.</document_type>`). Verified live against vote `117/1/312` on 2026-05-03 — for
 * amendment votes the bill-side fields under `<document>` are empty/blank and the actual amendment number is carried
 * top-level on `<roll_call_vote>` instead.
 *
 * Lives in votes-pipeline (not shared-models) because shared-models 0.1.43 does not yet model these fields on
 * `SenateVoteXmlDTO`. When/if shared-models gains these fields, this case class and [[SenateVoteEnvelope]] go away and
 * callers read them off the DTO directly.
 *
 * @param amendmentNumber
 *   Raw `<amendment_number>` text (e.g. `"S.Amdt. 2137"`) — type prefix + whitespace + numeric tail. Parse via
 *   [[repcheck.ingestion.votes.pipeline.SenateVoteConverter.AmendmentNumberPattern]] to extract the numeric tail.
 *   `None` when the element is missing or empty.
 * @param amendmentToDocumentNumber
 *   Raw `<amendment_to_document_number>` text (e.g. `"H.R. 3684"`) — the bill the amendment is amending. Used to seed a
 *   parent-bill placeholder so the amendments-pipeline's recursion has somewhere to attach.
 * @param amendmentToDocumentShortTitle
 *   Optional shorthand title from `<amendment_to_document_short_title>` (e.g. `"INVEST in America Act"`). Retained for
 *   log readability; not used in classification.
 */
final case class SenateVoteAmendmentFields(
  amendmentNumber: Option[String],
  amendmentToDocumentNumber: Option[String],
  amendmentToDocumentShortTitle: Option[String],
)

object SenateVoteAmendmentFields {
  val empty: SenateVoteAmendmentFields = SenateVoteAmendmentFields(None, None, None)
}
