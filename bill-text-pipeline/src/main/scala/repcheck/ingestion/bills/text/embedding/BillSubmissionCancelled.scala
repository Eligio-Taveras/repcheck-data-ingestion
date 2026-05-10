package repcheck.ingestion.bills.text.embedding

/**
 * Raised by [[CrossBillEmbedder.submit]]'s `guaranteeCase` finalizer when the producing fiber is cancelled
 * mid-submission (graceful shutdown, supervisor cancel). Routed through `cleanupOnSubmitError` so the ackId is removed
 * from state, any buffered chunks are purged, and Pub/Sub gets an explicit NACK rather than relying on the ackDeadline
 * alone.
 */
final case class BillSubmissionCancelled(ackId: String)
    extends RuntimeException(s"Bill submission cancelled for ackId=$ackId")
