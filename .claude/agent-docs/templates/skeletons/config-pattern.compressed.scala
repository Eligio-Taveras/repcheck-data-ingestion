<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/config-pattern.scala -->

```markdown
# RepCheck Skeleton: Per-Subsystem Configuration Pattern

**Repo:** Each application repo  
**Pattern:** PureConfig auto-derivation for Scala 3 with nested per-subsystem config.

## Key Decisions
- PureConfig with auto-derivation via `pureconfig-generic-scala3`
- Each subsystem has `RetryConfig` + parallelism + timeout
- Environment variable overrides supported
- Version defaults set by CI on release, overridable via config
- Parallelism always configured (sequential = parallelism 1)

---

## Retry Config

```scala
final case class RetryConfig(
    maxRetries: Int = 3,
    initialBackoff: FiniteDuration = 10.millis,
    backoffMultiplier: Double = 2.0,
    maxBackoff: FiniteDuration = 60.seconds,
    timeout: FiniteDuration = 30.seconds
) derives ConfigReader
```

Reusable retry configuration — one per subsystem.

---

## Subsystem Config

```scala
final case class SubsystemConfig(
    retry: RetryConfig = RetryConfig(),
    parallelism: Int = 1
) derives ConfigReader
```

Configuration for a single external subsystem (API/service).

---

## Version Config

```scala
final case class VersionConfig(
    promptVersion: String,       // e.g., "v1.2.0" — set by CI, overridable
    workflowVersion: String      // e.g., "v1.0.0" — set by CI, overridable
) derives ConfigReader
```

Version configuration for GCS-stored resources.

---

## Application Config Examples

### Bill Ingestion

```scala
final case class BillIngestionAppConfig(
    congressGov: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 3,
        initialBackoff = 500.millis,
        maxBackoff = 30.seconds,
        timeout = 30.seconds
      ),
      parallelism = 2
    ),
    firestore: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 3,
        initialBackoff = 100.millis,
        maxBackoff = 30.seconds,
        timeout = 30.seconds
      ),
      parallelism = 10
    ),
    pubSub: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 5,
        initialBackoff = 10.millis,
        maxBackoff = 10.seconds,
        timeout = 10.seconds
      ),
      parallelism = 5
    ),
    gcs: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 3,
        initialBackoff = 100.millis,
        maxBackoff = 15.seconds,
        timeout = 15.seconds
      ),
      parallelism = 4
    ),
    versions: VersionConfig,
    apiKey: String,
    pageSize: Int = 100,
    billLookBackInDays: Int = 120
) derives ConfigReader
```

### LLM Analysis

```scala
final case class LlmAnalysisAppConfig(
    llmClaude: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 2,
        initialBackoff = 1.second,
        maxBackoff = 60.seconds,
        timeout = 120.seconds
      ),
      parallelism = 8
    ),
    llmGpt: SubsystemConfig = SubsystemConfig(
      retry = RetryConfig(
        maxRetries = 2,
        initialBackoff = 1.second,
        maxBackoff = 60.seconds,
        timeout = 120.seconds
      ),
      parallelism = 4
    ),
    firestore: SubsystemConfig = SubsystemConfig(),
    gcs: SubsystemConfig = SubsystemConfig(),
    versions: VersionConfig
) derives ConfigReader
```

---

## HOCON Example (application.conf)

```hocon
bill-ingestion {
  congress-gov {
    retry {
      max-retries = 3
      initial-backoff = 500ms
      backoff-multiplier = 2.0
      max-backoff = 30s
      timeout = 30s
    }
    parallelism = 2
  }
  firestore {
    retry {
      max-retries = 3
      initial-backoff = 100ms
      max-backoff = 30s
      timeout = 30s
    }
    parallelism = 10
  }
  pub-sub {
    retry {
      max-retries = 5
      initial-backoff = 10ms
      max-backoff = 10s
      timeout = 10s
    }
    parallelism = 5
  }
  gcs {
    retry {
      max-retries = 3
      initial-backoff = 100ms
      max-backoff = 15s
      timeout = 15s
    }
    parallelism = 4
  }
  versions {
    prompt-version = "v1.2.0"
    workflow-version = "v1.0.0"
  }
  api-key = ${CONGRESS_GOV_API_KEY}
  page-size = 100
  bill-look-back-in-days = 120
}
```

Environment variable overrides:
```
BILL_INGESTION_CONGRESS_GOV_RETRY_MAX_RETRIES=5
BILL_INGESTION_FIRESTORE_PARALLELISM=20
```

---

## Config Loader

```scala
import cats.effect.IO

object ConfigLoader {
  def load[T: ConfigReader](namespace: String): IO[T] =
    IO.blocking {
      ConfigSource.default.at(namespace).loadOrThrow[T]
    }

  def fromJsonArg[T: ConfigReader](json: String): IO[T] =
    IO.blocking {
      ConfigSource.string(json).loadOrThrow[T]
    }
}
```

Load from `application.conf` with environment overrides, or from JSON string (CLI pattern).

---

## How to Create

1. Define `AppConfig` case class extending `SubsystemConfig` per subsystem used
2. Add default `RetryConfig` and `parallelism` values per subsystem
3. Create `application.conf` with HOCON tree matching case class field names
4. Use `ConfigLoader.load[AppConfig]("app-namespace")` to load and merge env overrides
5. Each subsystem name in HOCON matches case class field name for automatic PureConfig derivation
```