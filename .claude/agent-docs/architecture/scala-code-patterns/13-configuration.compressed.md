<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/13-configuration.md -->

# 13. Configuration (PureConfig)

**Pattern**: PureConfig with auto-derivation via `pureconfig-generic-scala3`. Each application defines its own config case class.

### Dependency

```scala
// In Dependencies.scala
val pureConfig = Seq(
  "com.github.pureconfig" %% "pureconfig-core"           % Versions.pureConfig,
  "com.github.pureconfig" %% "pureconfig-generic-scala3" % Versions.pureConfig
)
```

### Config Definition

```scala
import pureconfig.*
import pureconfig.generic.derivation.default.*

case class BillIngestionConfig(
  apiKey: String,
  pageSize: Int,
  billLookBackInDays: Int = 120,
  firestoreProjectId: String,
  pubSubProjectId: String
) derives ConfigReader

case class LlmAnalysisConfig(
  firestoreProjectId: String,
  pubSubProjectId: String,
  gcsBucket: String,
  gcsPromptPath: String,
  llmProvider: String,       // "claude" | "gpt"
  llmApiKey: String,
  llmModel: String,
  maxConcurrency: Int = 5
) derives ConfigReader

case class ScoringConfig(
  firestoreProjectId: String,
  cloudSqlInstanceConnectionName: String,
  cloudSqlDatabase: String,
  cloudSqlUser: String,
  cloudSqlPassword: String,
  gcsBucket: String,
  gcsPromptPath: String,
  llmProvider: String,
  llmApiKey: String,
  llmModel: String
) derives ConfigReader
```

### Config Loading

```scala
import pureconfig.ConfigSource

def loadConfig[F[_]: Sync](args: List[String]): F[BillIngestionConfig] =
  Sync[F].delay {
    ConfigSource.string(args.headOption.getOrElse(
      throw ConfigLoadFailed("cli-args", new IllegalArgumentException("No config argument provided"))
    )).loadOrThrow[BillIngestionConfig]
  }

// Or from application.conf
def loadFromFile[F[_]: Sync]: F[BillIngestionConfig] =
  Sync[F].delay {
    ConfigSource.default.loadOrThrow[BillIngestionConfig]
  }
```

### Rules
- Use `derives ConfigReader` for auto-derivation; no manual implementations
- Each application has its own config case class
- Config loaded once at startup, threaded through application
- Secrets from environment variables (Cloud Run), mapped via `application.conf`:
  ```hocon
  llm-api-key = ${LLM_API_KEY}
  cloud-sql-password = ${CLOUD_SQL_PASSWORD}
  ```