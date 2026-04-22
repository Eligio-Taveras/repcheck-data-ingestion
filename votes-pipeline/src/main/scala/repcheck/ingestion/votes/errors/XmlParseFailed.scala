package repcheck.ingestion.votes.errors

/**
 * Raised by [[repcheck.ingestion.votes.xml.SenateVoteXmlDecoder]] when a Senate XML document cannot be decoded into a
 * [[repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO]] or a list of
 * [[repcheck.ingestion.votes.xml.SenateVoteIndexEntry]]s. `detail` is a human-readable explanation of the failure (for
 * example, "missing `<congress>` element" or "Unparseable voteDate: 'not a date'"). `rawFragment` optionally carries a
 * small excerpt of the offending XML for log triage; decoders keep it under ~200 chars to avoid dumping full feed
 * bodies into the logs.
 *
 * Distinct from [[repcheck.ingestion.common.errors.XmlParseFailed]] in ingestion-common: that one wraps
 * network/parser-level failures while this one is thrown by the votes-pipeline's own decoder when structurally valid
 * XML fails its field-level extraction rules. Votes-pipeline packages its own type so the error carries decoder-
 * specific context and so `ProjectExceptionsOnlyCheck` stays green under plugin v0.5.0.
 */
final case class XmlParseFailed(detail: String, rawFragment: Option[String])
    extends Exception(s"Senate vote XML parse failed: $detail")
