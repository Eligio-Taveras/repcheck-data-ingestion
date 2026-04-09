// =============================================================================
// RepCheck Skeleton: Error Handling Pattern
// Repo: Each application repo defines its own errors
// =============================================================================
//
// PURPOSE: Defines flat, unique exception types per operation. Context is
// implied by the executing application — no need for BillFetchFailed vs
// MemberFetchFailed when you're already in the bill-ingestion app.
//
// KEY DECISIONS (from Q&A):
// - Flat case class exceptions extending Exception (no sealed hierarchies)
// - Each exception is unique — stack trace tells you exactly where and why
// - ErrorClassifier is per-subsystem (adapter owns classification)
// - Systemic errors halt pipeline, Transient errors log + continue
// - Every item has a correlationId for filtering in logs
// =============================================================================

package repcheck.errors

import java.util.UUID

// ---------------------------------------------------------------------------
// Flat Exception Types — one per operation, context implied by application
// ---------------------------------------------------------------------------

// Data ingestion errors
final case class FetchFailed(entityId: String, cause: Throwable)
    extends Exception(s"Failed to fetch $entityId", cause)

final case class DecodeFailed(entityId: String, rawJson: String, cause: Throwable)
    extends Exception(s"Failed to decode $entityId: ${cause.getMessage}", cause)

final case class PersistFailed(entityId: String, cause: Throwable)
    extends Exception(s"Failed to persist $entityId", cause)

// Snapshot errors
final case class SnapshotReadFailed(path: String, cause: Throwable)
    extends Exception(s"Failed to read snapshot at $path", cause)

final case class SnapshotWriteFailed(path: String, cause: Throwable)
    extends Exception(s"Failed to write snapshot to $path", cause)

// Pub/Sub errors
final case class PublishFailed(topic: String, eventId: UUID, cause: Throwable)
    extends Exception(s"Failed to publish event $eventId to $topic", cause)

final case class DeserializationFailed(subscriptionId: String, rawMessage: String, cause: Throwable)
    extends Exception(s"Failed to deserialize message from $subscriptionId", cause)

// LLM errors
final case class PromptAssemblyFailed(fragmentKey: String, cause: Throwable)
    extends Exception(s"Failed to assemble prompt fragment $fragmentKey", cause)

final case class LlmCallFailed(provider: String, model: String, cause: Throwable)
    extends Exception(s"LLM call failed for $provider/$model", cause)

final case class LlmResponseParseFailed(provider: String, rawResponse: String, cause: Throwable)
    extends Exception(s"Failed to parse LLM response from $provider", cause)

// Scoring errors
final case class ScoringFailed(userId: String, billId: String, cause: Throwable)
    extends Exception(s"Failed to score bill $billId for user $userId", cause)

// Orchestrator errors
final case class CapacityCheckFailed(cause: Throwable)
    extends Exception(s"Failed to check Cloud Run capacity", cause)

final case class JobLaunchFailed(jobName: String, cause: Throwable)
    extends Exception(s"Failed to launch Cloud Run job $jobName", cause)

// ---------------------------------------------------------------------------
// Example ErrorClassifiers — each subsystem adapter implements its own
// ---------------------------------------------------------------------------

import repcheck.pipeline.models.retry.{ErrorClassifier, ErrorSeverity}

/** Congress.gov API error classifier.
  * The adapter knows what's recoverable for this specific API.
  */
object CongressGovErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    // HTTP 429 Too Many Requests — rate limited, will recover
    case e if e.getMessage != null && e.getMessage.contains("429") =>
      ErrorSeverity.Transient

    // HTTP 500/502/503 — server error, may recover on retry
    case e if e.getMessage != null && e.getMessage.matches(".*5\\d{2}.*") =>
      ErrorSeverity.Transient

    // Timeout — transient network issue
    case _: java.util.concurrent.TimeoutException =>
      ErrorSeverity.Transient

    // HTTP 401/403 — credentials invalid, won't recover
    case e if e.getMessage != null && e.getMessage.matches(".*(401|403).*") =>
      ErrorSeverity.Systemic

    // Connection refused — service unreachable
    case _: java.net.ConnectException =>
      ErrorSeverity.Systemic

    // Unknown — default to transient (retry, then fail item)
    case _ => ErrorSeverity.Transient
  }
}

/** AlloyDB / Doobie error classifier. */
object AlloyDbErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    // Connection refused — DB unreachable
    case _: java.net.ConnectException =>
      ErrorSeverity.Systemic

    // Authentication failure
    case e if e.getMessage != null && e.getMessage.contains("authentication") =>
      ErrorSeverity.Systemic

    // Constraint violation — item-level data issue
    case e if e.getMessage != null && e.getMessage.contains("violates") =>
      ErrorSeverity.Transient

    // Deadlock — transient contention
    case e if e.getMessage != null && e.getMessage.contains("deadlock") =>
      ErrorSeverity.Transient

    case _ => ErrorSeverity.Transient
  }
}

/** Pub/Sub error classifier. */
object PubSubErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    case _: java.net.ConnectException      => ErrorSeverity.Systemic
    case _: java.util.concurrent.TimeoutException => ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.contains("PERMISSION_DENIED") =>
      ErrorSeverity.Systemic
    case _ => ErrorSeverity.Transient
  }
}

/** LLM provider error classifier. */
object LlmErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    // Rate limited — will recover
    case e if e.getMessage != null && e.getMessage.contains("429") =>
      ErrorSeverity.Transient

    // Invalid API key — won't recover
    case e if e.getMessage != null && e.getMessage.matches(".*(401|403).*") =>
      ErrorSeverity.Systemic

    // Overloaded — transient
    case e if e.getMessage != null && e.getMessage.contains("overloaded") =>
      ErrorSeverity.Transient

    // Quota exceeded — systemic
    case e if e.getMessage != null && e.getMessage.contains("quota") =>
      ErrorSeverity.Systemic

    case _: java.util.concurrent.TimeoutException => ErrorSeverity.Transient
    case _ => ErrorSeverity.Transient
  }
}
