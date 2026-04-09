> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 4. Error Handling

**Pattern**: Flat, standalone exception case classes. Each error type maps to one specific failure mode. No sealed hierarchies — the running application provides context.

### Error Definition

```scala
// Each error is self-contained — includes identifying info and cause
case class FetchFailed(entityId: String, cause: Throwable)
  extends Exception(s"Failed to fetch $entityId", cause)

case class DecodeFailed(entityId: String, rawJson: String, cause: io.circe.Error)
  extends Exception(s"Failed to decode $entityId: ${cause.getMessage}", cause)

case class PersistFailed(entityId: String, cause: Throwable)
  extends Exception(s"Failed to persist $entityId", cause)

case class UnknownBillType(typeStr: String)
  extends Exception(s"Unknown bill type: $typeStr")

case class ConfigLoadFailed(source: String, cause: Throwable)
  extends Exception(s"Failed to load config from $source", cause)

case class PromptAssemblyFailed(profileName: String, cause: Throwable)
  extends Exception(s"Failed to assemble prompt profile $profileName", cause)

case class LlmCallFailed(provider: String, model: String, cause: Throwable)
  extends Exception(s"LLM call failed: provider=$provider, model=$model", cause)

case class GcsReadFailed(bucket: String, path: String, cause: Throwable)
  extends Exception(s"Failed to read GCS object gs://$bucket/$path", cause)
```

### Error Raising

Fail immediately in `F[_]` via `MonadError` / `ApplicativeError`:

```scala
import cats.syntax.all.*
import cats.ApplicativeError

// Fail immediately on individual item errors
def fetchBill[F[_]: Async: Network](billId: String): F[LegislativeBillDO] =
  client.expect[LegislativeBillApiDTO](uri)
    .map(_.toDO())
    .adaptError { case e => FetchFailed(billId, e) }

// Pattern match and raise
def loadConfig[F[_]: Sync](args: List[String]): F[AppConfig] =
  ConfigSource.string(args.head).load[AppConfig] match {
    case Right(config) => config.pure[F]
    case Left(errors)  => ApplicativeError[F, Throwable].raiseError(
      ConfigLoadFailed("cli-args", new Exception(errors.prettyPrint()))
    )
  }
```

### Rules
- Each exception case class is **flat** — no sealed trait hierarchy
- The executing application name provides domain context (e.g., `FetchFailed` in `bills-pipeline` is obviously a bill fetch failure)
- Same error classes (e.g., `FetchFailed`, `DecodeFailed`, `PersistFailed`) are reused across pipelines
- **Always** include `entityId` and `cause` so the stack trace leads directly to the broken code
- Use `.adaptError` to wrap lower-level exceptions in domain-specific errors
- Common error classes live in `repcheck-pipeline-models` since they're operational concerns
