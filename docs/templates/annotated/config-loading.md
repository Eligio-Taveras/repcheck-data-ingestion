# Pattern: Configuration Loading

## Pattern Summary
Application configuration using PureConfig with a manual `ConfigReader` via the cursor API. Config is passed as a JSON string via CLI args, parsed into a case class, and wrapped in `IO` for fail-fast error handling. Config errors surface immediately with descriptive messages.

## When To Use This Pattern
- Every pipeline application needs a config case class and loader
- Cloud Run Jobs receive config via CLI args or environment variables

## Source Files
- `bill-identifier/src/main/scala/config/BillIdentifierConfig.scala` — config case class with ConfigReader
- `bill-identifier/src/main/scala/config/ConfigLoader.scala` — IO-wrapped loader

---

## The Config Case Class

```scala
// File: bill-identifier/src/main/scala/config/BillIdentifierConfig.scala

package config

import pureconfig._

// ANNOTATION: Simple case class representing ALL configuration for this pipeline.
// Each field corresponds to a JSON key in the CLI arg string.
// No defaults here — all fields are required.
case class BillIdentifierConfig(
    apiKey: String,
    pageSize: Int,
    billLookBackInDays: Int
)

object BillIdentifierConfig {
  // ANNOTATION: Manual ConfigReader using PureConfig's cursor API.
  //
  // WHY MANUAL AND NOT AUTO-DERIVATION?
  // The current code uses PureConfig's cursor API for explicit field mapping.
  // This gives full control over field names and error messages.
  //
  // FOR NEW CODE: prefer auto-derivation via pureconfig-generic-scala3:
  //   import pureconfig.generic.derivation.default._
  //   // No manual reader needed — derives automatically from case class fields
  //
  // The cursor API shown here is useful when:
  //   - Field names in config don't match case class fields
  //   - You need custom validation during parsing
  //   - You want to combine multiple config sources
  implicit val configReader: ConfigReader[BillIdentifierConfig] = {
    (cur: ConfigCursor) =>
      {
        // ANNOTATION: for-comprehension over Either.
        // Each step returns Either[ConfigReaderFailures, T].
        // If ANY step fails, the whole chain short-circuits with
        // a descriptive error message showing which key was missing/invalid.
        for {
          objCur <- cur.asObjectCursor
          apiKey <- objCur.atKey("apiKey").flatMap(_.asString)
          pageSize <- objCur.atKey("pageSize").flatMap(_.asInt)
          billLookBack <- objCur.atKey("billLookBackInDays").flatMap(_.asInt)
        } yield BillIdentifierConfig(apiKey, pageSize, billLookBack)
      }
  }
}
```

## The Config Loader

```scala
// File: bill-identifier/src/main/scala/config/ConfigLoader.scala

package config

import cats.effect.IO
import cats.syntax.all._

import pureconfig.ConfigSource

object ConfigLoader {
  // ANNOTATION: Loads config from CLI args, returns IO[Config].
  //
  // FAIL-FAST PATTERN:
  //   - No args → IO.raiseError immediately
  //   - Parse failure → IO.raiseError with PureConfig error details
  //   - Success → .pure[IO] lifts the config into IO
  //
  // This runs FIRST in the IOApp for-comprehension.
  // If config is invalid, nothing else executes.
  def LoadConfig(args: List[String]): IO[BillIdentifierConfig] = {
    // ANNOTATION: headOption instead of .head — WartRemover enforces this.
    // .head would throw NoSuchElementException on empty list.
    // headOption returns None, which we handle explicitly.
    args.headOption match {
      case None =>
        IO.raiseError(
          IllegalArgumentException(
            "You must provide a configuration json string."
          )
        )
      case Some(jsonString) =>
        // ANNOTATION: ConfigSource.string parses an inline string.
        // PureConfig also supports ConfigSource.default (application.conf),
        // ConfigSource.file, ConfigSource.resources, etc.
        // .load[T] uses the implicit ConfigReader[T] to parse.
        ConfigSource.string(jsonString).load[BillIdentifierConfig] match {
          case Right(conf) => conf.pure[IO]
          // ANNOTATION: .pure[IO] lifts a pure value into IO.
          // Equivalent to IO.pure(conf) but works with any Applicative.
          case Left(errors) =>
            IO.raiseError(IllegalArgumentException(errors.toString))
        }
    }
  }
}
```

## Key Patterns

| Pattern | When To Use |
|---------|-------------|
| `args.headOption` | Accessing CLI args safely (WartRemover forbids `.head`) |
| `IO.raiseError(...)` | Fail fast with a descriptive error |
| `.pure[IO]` | Lift a pure value into the effect type |
| `ConfigSource.string(...)` | Parse inline JSON/HOCON strings |
| Manual `ConfigReader` | When field names differ or you need custom validation |
| Auto-derived `ConfigReader` | For new code — import `pureconfig.generic.derivation.default._` |

## How to Create a New Config

For new pipeline apps, prefer auto-derivation:

```scala
package config

import pureconfig._
import pureconfig.generic.derivation.default._

// ANNOTATION: With auto-derivation, no manual ConfigReader needed.
// PureConfig derives it from the case class field names.
// Nested case classes are also auto-derived.
case class VoteIngestionConfig(
    apiKey: String,
    pageSize: Int,
    voteLookBackInDays: Int,
    firestoreProjectId: String,
    retry: RetryConfig,          // Nested — auto-derived too
    parallelism: Int
) derives ConfigReader             // Scala 3 derives syntax

case class RetryConfig(
    maxRetries: Int,
    initialBackoffMs: Int,
    maxBackoffMs: Int,
    backoffMultiplier: Double
) derives ConfigReader
```

Usage in HOCON:
```hocon
api-key = "YOUR_KEY"
page-size = 250
vote-look-back-in-days = 120
firestore-project-id = "repcheck-421801"
retry {
  max-retries = 3
  initial-backoff-ms = 10
  max-backoff-ms = 60000
  backoff-multiplier = 2.0
}
parallelism = 4
```

Note: PureConfig auto-converts between camelCase (Scala) and kebab-case (HOCON) by default.
