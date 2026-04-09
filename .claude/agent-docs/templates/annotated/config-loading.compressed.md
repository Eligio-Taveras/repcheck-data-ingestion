<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/config-loading.md -->

# Pattern: Configuration Loading

## When To Use
- Every pipeline application needs a config case class and loader
- Cloud Run Jobs receive config via CLI args or environment variables

---

## The Config Case Class

```scala
// File: bill-identifier/src/main/scala/config/BillIdentifierConfig.scala

package config

import pureconfig._

case class BillIdentifierConfig(
    apiKey: String,
    pageSize: Int,
    billLookBackInDays: Int
)

object BillIdentifierConfig {
  // Manual ConfigReader using PureConfig's cursor API for explicit field mapping.
  // Auto-derivation via pureconfig-generic-scala3 preferred for new code:
  //   import pureconfig.generic.derivation.default._
  //   case class Config(...) derives ConfigReader
  implicit val configReader: ConfigReader[BillIdentifierConfig] = {
    (cur: ConfigCursor) =>
      {
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
  // Loads config from CLI args, returns IO[Config]. Fails fast on missing/invalid args.
  def LoadConfig(args: List[String]): IO[BillIdentifierConfig] = {
    args.headOption match {
      case None =>
        IO.raiseError(
          IllegalArgumentException(
            "You must provide a configuration json string."
          )
        )
      case Some(jsonString) =>
        // ConfigSource.string parses inline string; .load[T] uses implicit ConfigReader[T]
        ConfigSource.string(jsonString).load[BillIdentifierConfig] match {
          case Right(conf) => conf.pure[IO]
          case Left(errors) =>
            IO.raiseError(IllegalArgumentException(errors.toString))
        }
    }
  }
}
```

## Key Patterns

| Pattern | Usage |
|---------|-------|
| `args.headOption` | Safe CLI arg access (WartRemover forbids `.head`) |
| `IO.raiseError(...)` | Fail fast with descriptive error |
| `.pure[IO]` | Lift pure value into effect type |
| `ConfigSource.string(...)` | Parse inline JSON/HOCON strings |
| Manual `ConfigReader` | Custom validation or field name mapping |
| Auto-derived `ConfigReader` | Preferred for new code with `derives ConfigReader` |

## How to Create a New Config

Prefer auto-derivation for new pipeline apps:

```scala
package config

import pureconfig._
import pureconfig.generic.derivation.default._

// No manual ConfigReader needed; auto-derived from case class field names.
// Nested case classes also auto-derived. PureConfig auto-converts camelCase↔kebab-case.
case class VoteIngestionConfig(
    apiKey: String,
    pageSize: Int,
    voteLookBackInDays: Int,
    firestoreProjectId: String,
    retry: RetryConfig,
    parallelism: Int
) derives ConfigReader

case class RetryConfig(
    maxRetries: Int,
    initialBackoffMs: Int,
    maxBackoffMs: Int,
    backoffMultiplier: Double
) derives ConfigReader
```

HOCON usage:
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