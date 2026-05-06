<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/03-INGESTION-COMMON.md -->

# Acceptance Criteria: Component 3 — `ingestion-common`

Shared library providing reusable infrastructure for all ingestion pipelines. Contains Congress.gov API client base, XML feed parsers, change detection, event publishing, repository base patterns, placeholder entity creation, and pipeline execution helpers.
**Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2).

## System Context

`ingestion-common` is the shared foundation for all ingestion applications. It provides:

1. **Congress.gov API client base** — `CongressGovPaginatedClient[F, T]` — paginated HTTP with authentication, rate limiting, retry. Each pipeline extends with its own endpoint/DTO types.
2. **Senate/House XML feed parsers** — Base XML parsing for chamber-specific feeds (votes, members, committees). Each pipeline supplies its own XML-to-DTO mapping.
3. **Change detection** — Generic case-class diffing via `Product`. Uses `updateDate` as fast pre-filter; full field-by-field diff on change including nested case classes and list additions/removals by natural key.
4. **Event publishing** — Typed wrappers around `PipelineEvent[T]` and `PubSubPublisher[F]` from `pipeline-models`. Convenience methods for event catalog emission.
5. **Repository base patterns** — Transactor setup, upsert helpers, table name constants.
6. **Placeholder entity creation** — When cross-entity reference encountered (e.g., bill references member), create placeholder row with only natural key populated. Owning pipeline fills full data later via normal upsert + diff.
7. **Pipeline execution helpers** — Config loading from Cloud Run Job arguments, run ID extraction (from Launcher), workflow state updates, standard pipeline bootstrap.

### Data Flow

```
Cloud Scheduler → Launcher → Cloud Run Job (pipeline app)
                                    │
                                    ├── uses ingestion-common for:
                                    │     • HTTP client (Congress.gov API)
                                    │     • XML parser (Senate/House feeds)
                                    │     • Change detection
                                    │     • Repository base (Doobie transactor, upsert)
                                    │     • Event publishing (Pub/Sub)
                                    │     • Placeholder entity creation
                                    │     • Pipeline bootstrap & state tracking
                                    │
                                    ├── uses shared-models for:
                                    │     • DTOs, DOs, enums
                                    │
                                    └── uses pipeline-models for:
                                          • PipelineEvent, RetryWrapper, ErrorClassifier
                                          • ProcessingResult, PipelineRunSummary
                                          • WorkflowRunStep status updates
```

### What Lives Where

| Concern | Library |
|---------|---------|
| Domain types (DTOs, DOs, enums) | `shared-models` |
| Pipeline coordination types (events, retry, errors) | `pipeline-models` |
| Reusable ingestion infrastructure (this component) | `ingestion-common` |
| Entity-specific ingestion logic (bill ingestion, vote ingestion, etc.) | Each pipeline app |

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 3.1 Congress.gov API Client Base | Migrate + Extend | Paginated HTTP client with auth, rate limiting, retry |
| 3.2 Senate/House XML Feed Parsers | New | Base XML parsing for chamber-specific data feeds |
| 3.3 Change Detection | New | Generic case-class diffing with `updateDate` pre-filter |
| 3.4 Event Publishing | New | Typed event emission wrapping pipeline-models Pub/Sub |
| 3.5 Repository Base Patterns | Migrate + Extend | Transactor setup, upsert helpers, table constants |
| 3.6 Placeholder Entity Creation | New | Create minimal rows for referenced entities not yet ingested |
| 3.7 Pipeline Execution Helpers | Migrate + Extend | Config loading, run ID, state tracking, bootstrap |
| 3.8 Structured Logging | New | log4cats with JSON output, automatic runId/correlationId context |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Building or extending the Congress.gov API client | [3.1 API Client](03-ingestion-common/03.1-api-client.md) |
| Building XML feed parsers for Senate/House data | [3.2 XML Parsers](03-ingestion-common/03.2-xml-parsers.md) |
| Implementing change detection / entity diffing | [3.3 Change Detection](03-ingestion-common/03.3-change-detection.md) |
| Publishing pipeline events to Pub/Sub | [3.4 Event Publishing](03-ingestion-common/03.4-event-publishing.md) |
| Database transactor, upsert helpers, repository base | [3.5 Repository Base](03-ingestion-common/03.5-repository-base.md) |
| Placeholder entity creation for cross-entity references | [3.6 Placeholder Entities](03-ingestion-common/03.6-placeholder-entities.md) |
| Config loading, run ID, bootstrap, failure handling | [3.7 Execution Helpers](03-ingestion-common/03.7-execution-helpers.md) |
| Structured logging with log4cats | [3.8 Structured Logging](03-ingestion-common/03.8-structured-logging.md) |

## Cross-Cutting Concerns

### Package Structure

```
repcheck.ingestion.common
├── api
│   ├── CongressGovPaginatedClient
│   ├── CongressGovClientConfig
│   ├── HttpClientResource
│   ├── FetchParams
│   └── PagedResponse
├── xml
│   ├── XmlFeedClient
│   ├── XmlFeedConfig
│   ├── XmlParsingHelpers
│   ├── SenateXmlUrls
│   └── HouseXmlUrls
├── change
│   ├── ChangeDetector
│   └── ChangeResult
├── events
│   ├── IngestionEventPublisher
│   └── EventTypeConstants
├── db
│   ├── DatabaseConfig
│   ├── TransactorResource
│   └── UpsertHelper
├── placeholder
│   ├── HasPlaceholder
│   ├── PlaceholderCreator
│   └── EntityRepository
├── execution
│   ├── PipelineBootstrap
│   ├── WorkflowStateUpdater
│   └── PipelineFailureHandler
├── logging
│   ├── PipelineLogger
│   ├── PipelineLoggerFactory
│   ├── LogContext
│   └── LoggingConfig
├── codecs
│   └── DateTimeCodecs
└── errors
    ├── XmlParseFailed
    ├── XmlFieldMissing
    └── ConfigLoadFailed
```

### Dependencies

```
ingestion-common
├── repcheck-shared-models
├── repcheck-pipeline-models
├── http4s-ember-client
├── circe
├── doobie + hikari
├── http4s-scala-xml
├── log4cats + logback
├── pureconfig
├── fs2
└── cats-effect
```

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | All traits via mock implementations | MockitoScala for trait mocking |
| WireMock tests | `CongressGovPaginatedClient`, `XmlFeedClient` | WireMock for HTTP simulation |
| Integration tests | `TransactorResource`, `UpsertHelper`, all repository impls | `DockerPostgresSpec` trait (Docker CLI + Liquibase migrations) |
| Contract tests (dev GCP) | `IngestionEventPublisher` | Dev Pub/Sub topics |

### Migration Checklist

After `ingestion-common` is implemented:
1. `gov-apis` module: remove `PagingApiBase`, `Serializers`, `Constants`. Move to `ingestion-common`.
2. `bill-identifier` module: remove `DoobieBillRepository` base pattern, `DatabaseConfig`, `ConfigLoader`. Rewrite to depend on `ingestion-common` equivalents.
3. Both modules retain entity-specific logic (bill DTOs, bill API endpoint, bill processing) — only shared infrastructure moves.