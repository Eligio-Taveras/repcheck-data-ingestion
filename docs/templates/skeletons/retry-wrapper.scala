// =============================================================================
// RepCheck Skeleton: Centralized Retry Wrapper
// Repo: repcheck-pipeline-models (shared library)
// =============================================================================
//
// PURPOSE: Provides a reusable retry mechanism with exponential backoff that
// wraps any F[A] operation. Each subsystem (Congress.gov, Pub/Sub, AlloyDB,
// GCS, LLM) gets its own RetryConfig and ErrorClassifier.
//
// KEY DECISIONS (from Q&A):
// - Max retries: 3 default, configurable per subsystem
// - Initial backoff: 10ms, exponential with 2x multiplier
// - Max backoff cap: 60s default
// - Each subsystem explicitly decides its own retry values
// - ErrorClassifier is per-subsystem (adapter owns classification)
// - Transient errors → log + continue pipeline
// - Systemic errors → halt pipeline immediately
// - Timeout is configurable per subsystem alongside retry config
// =============================================================================

package repcheck.pipeline.models.retry

import cats.effect.{Async, Temporal}
import cats.syntax.all.*

import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Per-subsystem retry configuration.
  * Each API/service gets its own instance with values appropriate for that host.
  *
  * Examples:
  *   congress-gov: RetryConfig(maxRetries = 3, initialBackoff = 500.millis, maxBackoff = 30.seconds, timeout = 30.seconds)
  *   pub-sub:      RetryConfig(maxRetries = 5, initialBackoff = 10.millis,  maxBackoff = 10.seconds, timeout = 10.seconds)
  *   firestore:    RetryConfig(maxRetries = 3, initialBackoff = 100.millis, maxBackoff = 30.seconds, timeout = 30.seconds)
  *   gcs:          RetryConfig(maxRetries = 3, initialBackoff = 100.millis, maxBackoff = 15.seconds, timeout = 15.seconds)
  *   llm-claude:   RetryConfig(maxRetries = 2, initialBackoff = 1.second,   maxBackoff = 60.seconds, timeout = 120.seconds)
  */
final case class RetryConfig(
    maxRetries: Int = 3,
    initialBackoff: FiniteDuration = 10.millis,
    backoffMultiplier: Double = 2.0,
    maxBackoff: FiniteDuration = 60.seconds,
    timeout: FiniteDuration = 30.seconds
)

// ---------------------------------------------------------------------------
// Error Classification
// ---------------------------------------------------------------------------

/** Severity determines pipeline behavior after retry exhaustion. */
enum ErrorSeverity {
  /** Item-level failure. Log ProcessingResult as failed, continue to next item. */
  case Transient

  /** Infrastructure-level failure. Halt the pipeline immediately. */
  case Systemic
}

/** Each subsystem adapter implements this to classify its own errors.
  * The adapter knows best what's recoverable for its system.
  *
  * Example: CongressGovErrorClassifier knows that HTTP 429 is Transient
  * but HTTP 403 (API key revoked) is Systemic.
  */
trait ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity
}

// ---------------------------------------------------------------------------
// Pipeline Halt Exception
// ---------------------------------------------------------------------------

/** Thrown when a Systemic error is detected after retry exhaustion.
  * The pipeline should NOT catch this — let it propagate to halt execution.
  */
final case class SystemicFailure(
    subsystem: String,
    originalError: Throwable
) extends Exception(
      s"Systemic failure in $subsystem: ${originalError.getMessage}",
      originalError
    )

/** Thrown when a Transient error exhausts all retries.
  * The pipeline catches this and records a failed ProcessingResult.
  */
final case class TransientFailure(
    subsystem: String,
    originalError: Throwable,
    retriesAttempted: Int
) extends Exception(
      s"Transient failure in $subsystem after $retriesAttempted retries: ${originalError.getMessage}",
      originalError
    )

// ---------------------------------------------------------------------------
// Retry Wrapper
// ---------------------------------------------------------------------------

object RetryWrapper {

  /** Wrap any F[A] operation with retry + timeout + error classification.
    *
    * @param config     Per-subsystem retry configuration
    * @param classifier Per-subsystem error classifier (adapter owns this)
    * @param subsystem  Name for logging/tracing (e.g., "congress-gov", "firestore")
    * @param operation  The effectful operation to retry
    * @return F[A] that either succeeds, throws TransientFailure, or throws SystemicFailure
    */
  def withRetry[F[_]: Temporal, A](
      config: RetryConfig,
      classifier: ErrorClassifier,
      subsystem: String
  )(operation: F[A]): F[A] = {

    // Apply per-call timeout to the operation
    val timedOperation: F[A] =
      Temporal[F].timeout(operation, config.timeout)

    def attempt(retriesLeft: Int, currentBackoff: FiniteDuration): F[A] =
      timedOperation.handleErrorWith { error =>
        // Step 1: Classify the error using the subsystem's classifier
        classifier.classify(error) match {
          case ErrorSeverity.Systemic =>
            // Infrastructure is broken — halt immediately, no more retries
            // TODO: Log at ERROR level: s"Systemic failure in $subsystem: ${error.getMessage}"
            Temporal[F].raiseError(SystemicFailure(subsystem, error))

          case ErrorSeverity.Transient =>
            if (retriesLeft <= 0) {
              // All retries exhausted for this item
              // TODO: Log at WARN level: s"Transient failure in $subsystem after ${config.maxRetries} retries"
              Temporal[F].raiseError(
                TransientFailure(subsystem, error, config.maxRetries)
              )
            } else {
              // TODO: Log at INFO level: s"Retrying $subsystem in $currentBackoff (${retriesLeft} retries left)"
              Temporal[F].sleep(currentBackoff) *>
                attempt(
                  retriesLeft - 1,
                  // Exponential backoff: multiply by backoffMultiplier, cap at maxBackoff
                  (currentBackoff * config.backoffMultiplier).min(config.maxBackoff)
                )
            }
        }
      }

    attempt(config.maxRetries, config.initialBackoff)
  }
}
