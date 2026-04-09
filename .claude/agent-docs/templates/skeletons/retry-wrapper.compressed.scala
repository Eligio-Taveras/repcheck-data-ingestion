<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/retry-wrapper.scala -->

```markdown
# RepCheck Skeleton: Centralized Retry Wrapper

**Repo:** repcheck-pipeline-models (shared library)

**Purpose:** Reusable retry mechanism with exponential backoff wrapping F[A] operations. Each subsystem (Congress.gov, Pub/Sub, AlloyDB, GCS, LLM) has its own RetryConfig and ErrorClassifier.

## Key Decisions
- Max retries: 3 default, configurable per subsystem
- Initial backoff: 10ms, exponential 2x multiplier
- Max backoff cap: 60s default
- Each subsystem explicitly configures retry values
- ErrorClassifier is per-subsystem (adapter owns classification)
- Transient errors → log + continue pipeline
- Systemic errors → halt pipeline immediately
- Timeout configurable per subsystem alongside retry config

## Configuration

```scala
final case class RetryConfig(
    maxRetries: Int = 3,
    initialBackoff: FiniteDuration = 10.millis,
    backoffMultiplier: Double = 2.0,
    maxBackoff: FiniteDuration = 60.seconds,
    timeout: FiniteDuration = 30.seconds
)
```

Example configs:
- congress-gov: maxRetries=3, initialBackoff=500.millis, maxBackoff=30.seconds, timeout=30.seconds
- pub-sub: maxRetries=5, initialBackoff=10.millis, maxBackoff=10.seconds, timeout=10.seconds
- firestore: maxRetries=3, initialBackoff=100.millis, maxBackoff=30.seconds, timeout=30.seconds
- gcs: maxRetries=3, initialBackoff=100.millis, maxBackoff=15.seconds, timeout=15.seconds
- llm-claude: maxRetries=2, initialBackoff=1.second, maxBackoff=60.seconds, timeout=120.seconds

## Error Classification

```scala
enum ErrorSeverity {
  case Transient  // Item-level failure; log as failed, continue to next item
  case Systemic   // Infrastructure-level failure; halt pipeline immediately
}

trait ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity
}
```

Each subsystem adapter implements ErrorClassifier. Example: CongressGovErrorClassifier classifies HTTP 429 as Transient but HTTP 403 (revoked API key) as Systemic.

## Pipeline Halt Exceptions

```scala
final case class SystemicFailure(
    subsystem: String,
    originalError: Throwable
) extends Exception(
      s"Systemic failure in $subsystem: ${originalError.getMessage}",
      originalError
    )

final case class TransientFailure(
    subsystem: String,
    originalError: Throwable,
    retriesAttempted: Int
) extends Exception(
      s"Transient failure in $subsystem after $retriesAttempted retries: ${originalError.getMessage}",
      originalError
    )
```

SystemicFailure: thrown on Systemic error after retry exhaustion; propagates to halt pipeline.
TransientFailure: thrown on Transient error after retry exhaustion; pipeline catches and records failed ProcessingResult.

## Retry Wrapper

```scala
object RetryWrapper {

  def withRetry[F[_]: Temporal, A](
      config: RetryConfig,
      classifier: ErrorClassifier,
      subsystem: String
  )(operation: F[A]): F[A] = {

    val timedOperation: F[A] =
      Temporal[F].timeout(operation, config.timeout)

    def attempt(retriesLeft: Int, currentBackoff: FiniteDuration): F[A] =
      timedOperation.handleErrorWith { error =>
        classifier.classify(error) match {
          case ErrorSeverity.Systemic =>
            // Log ERROR: s"Systemic failure in $subsystem: ${error.getMessage}"
            Temporal[F].raiseError(SystemicFailure(subsystem, error))

          case ErrorSeverity.Transient =>
            if (retriesLeft <= 0) {
              // Log WARN: s"Transient failure in $subsystem after ${config.maxRetries} retries"
              Temporal[F].raiseError(
                TransientFailure(subsystem, error, config.maxRetries)
              )
            } else {
              // Log INFO: s"Retrying $subsystem in $currentBackoff (${retriesLeft} retries left)"
              Temporal[F].sleep(currentBackoff) *>
                attempt(
                  retriesLeft - 1,
                  (currentBackoff * config.backoffMultiplier).min(config.maxBackoff)
                )
            }
        }
      }

    attempt(config.maxRetries, config.initialBackoff)
  }
}
```

**Signature:** `withRetry[F[_]: Temporal, A](config: RetryConfig, classifier: ErrorClassifier, subsystem: String)(operation: F[A]): F[A]`

Wraps operation with per-call timeout (config.timeout). On error: classify via adapter, raise SystemicFailure on Systemic (no retry), raise TransientFailure after maxRetries on Transient, else sleep and retry with exponential backoff capped at maxBackoff.
```