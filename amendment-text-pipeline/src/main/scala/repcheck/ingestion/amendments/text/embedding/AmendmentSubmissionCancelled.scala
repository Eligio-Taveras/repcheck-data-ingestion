package repcheck.ingestion.amendments.text.embedding

/**
 * Raised by [[CrossAmendmentEmbedder.submit]]'s `guaranteeCase` finalizer when the producing fiber is cancelled
 * mid-submission (graceful shutdown, supervisor cancel). Routed through `failAck` so the ackId is removed from state,
 * any buffered chunks are purged, and Pub/Sub gets an explicit NACK rather than relying on the ackDeadline alone.
 */
final case class AmendmentSubmissionCancelled(ackId: String)
    extends RuntimeException(s"Amendment submission cancelled for ackId=$ackId")
