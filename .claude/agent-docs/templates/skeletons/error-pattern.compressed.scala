<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/error-pattern.scala -->

```markdown
# RepCheck Error Handling Pattern

**Purpose:** Flat, unique exception types per operation. Context implied by executing application. Stack trace pinpoints exact failure location.

**Key Decisions:**
- Flat case class exceptions extending Exception (no sealed hierarchies)
- Each exception unique — one per operation
- ErrorClassifier per-subsystem (adapter owns classification)
- Systemic errors halt pipeline; Transient errors log + continue
- Every error has correlationId for log filtering

---

## Exception Types

### Data Ingestion
```scala
final case class FetchFailed(entityId: String, cause: Throwable)
    extends Exception(s"Failed to fetch $entityId", cause)

final case class DecodeFailed(entityId: String, rawJson: String, cause: Throwable)
    extends Exception(s"Failed to decode $entityId: ${cause.getMessage}", cause)

final case class PersistFailed(entityId: String, cause: Throwable)
    extends Exception(s"Failed to persist $entityId", cause)
```

### Snapshots
```scala
final case class SnapshotReadFailed(path: String, cause: Throwable)
    extends Exception(s"Failed to read snapshot at $path", cause)

final case class SnapshotWriteFailed(path: String, cause: Throwable)
    extends Exception(s"Failed to write snapshot to $path", cause)
```

### Pub/Sub
```scala
final case class PublishFailed(topic: String, eventId: UUID, cause: Throwable)
    extends Exception(s"Failed to publish event $eventId to $topic", cause)

final case class DeserializationFailed(subscriptionId: String, rawMessage: String, cause: Throwable)
    extends Exception(s"Failed to deserialize message from $subscriptionId", cause)
```

### LLM
```scala
final case class PromptAssemblyFailed(fragmentKey: String, cause: Throwable)
    extends Exception(s"Failed to assemble prompt fragment $fragmentKey", cause)

final case class LlmCallFailed(provider: String, model: String, cause: Throwable)
    extends Exception(s"LLM call failed for $provider/$model", cause)

final case class LlmResponseParseFailed(provider: String, rawResponse: String, cause: Throwable)
    extends Exception(s"Failed to parse LLM response from $provider", cause)
```

### Scoring
```scala
final case class ScoringFailed(userId: String, billId: String, cause: Throwable)
    extends Exception(s"Failed to score bill $billId for user $userId", cause)
```

### Orchestrator
```scala
final case class CapacityCheckFailed(cause: Throwable)
    extends Exception(s"Failed to check Cloud Run capacity", cause)

final case class JobLaunchFailed(jobName: String, cause: Throwable)
    extends Exception(s"Failed to launch Cloud Run job $jobName", cause)
```

---

## ErrorClassifiers

### Congress.gov API
```scala
object CongressGovErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    case e if e.getMessage != null && e.getMessage.contains("429") =>
      ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.matches(".*5\\d{2}.*") =>
      ErrorSeverity.Transient
    case _: java.util.concurrent.TimeoutException =>
      ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.matches(".*(401|403).*") =>
      ErrorSeverity.Systemic
    case _: java.net.ConnectException =>
      ErrorSeverity.Systemic
    case _ => ErrorSeverity.Transient
  }
}
```

### AlloyDB / Doobie
```scala
object AlloyDbErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    case _: java.net.ConnectException =>
      ErrorSeverity.Systemic
    case e if e.getMessage != null && e.getMessage.contains("authentication") =>
      ErrorSeverity.Systemic
    case e if e.getMessage != null && e.getMessage.contains("violates") =>
      ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.contains("deadlock") =>
      ErrorSeverity.Transient
    case _ => ErrorSeverity.Transient
  }
}
```

### Pub/Sub
```scala
object PubSubErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    case _: java.net.ConnectException      => ErrorSeverity.Systemic
    case _: java.util.concurrent.TimeoutException => ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.contains("PERMISSION_DENIED") =>
      ErrorSeverity.Systemic
    case _ => ErrorSeverity.Transient
  }
}
```

### LLM Provider
```scala
object LlmErrorClassifier extends ErrorClassifier {
  def classify(error: Throwable): ErrorSeverity = error match {
    case e if e.getMessage != null && e.getMessage.contains("429") =>
      ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.matches(".*(401|403).*") =>
      ErrorSeverity.Systemic
    case e if e.getMessage != null && e.getMessage.contains("overloaded") =>
      ErrorSeverity.Transient
    case e if e.getMessage != null && e.getMessage.contains("quota") =>
      ErrorSeverity.Systemic
    case _: java.util.concurrent.TimeoutException => ErrorSeverity.Transient
    case _ => ErrorSeverity.Transient
  }
}
```
```