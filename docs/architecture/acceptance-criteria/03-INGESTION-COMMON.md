# Acceptance Criteria: Component 3 — `ingestion-common`

> Shared library providing reusable infrastructure for all ingestion pipelines.
> Contains the Congress.gov API client base, XML feed parsers, change detection,
> event publishing, repository base patterns, placeholder entity creation, and
> pipeline execution helpers.
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2).

---

## System Context

`ingestion-common` is the shared foundation that all ingestion applications depend on. It does NOT contain entity-specific logic (e.g., "how to ingest a bill") — that lives in each pipeline application. Instead, it provides:

1. **Congress.gov API client base** — `CongressGovPaginatedClient[F, T]` — paginated HTTP client with authentication, rate limiting, and retry. Each entity pipeline extends this with its own endpoint and DTO types. Separate from `XmlFeedClient` which handles senate.gov/clerk.house.gov feeds.
2. **Senate/House XML feed parsers** — Base XML parsing infrastructure for chamber-specific feeds (votes, members, committees). Each pipeline supplies its own XML-to-DTO mapping.
3. **Change detection** — Generic case-class diffing via `Product`. Uses `updateDate` as a fast pre-filter; when dates indicate a change, performs full field-by-field diff (including nested case classes and list additions/removals matched by natural key). Pipelines call this to decide whether an entity needs processing and to identify exactly what changed.
4. **Event publishing** — Typed wrappers around `PipelineEvent[T]` and `PubSubPublisher[F]` from `pipeline-models`. Provides convenience methods for emitting the event catalog from BEHAVIORAL_SPECS §4.
5. **Repository base patterns** — Transactor setup, upsert helpers, and table name constants used by all pipeline repositories.
6. **Placeholder entity creation** — When a pipeline encounters a cross-entity reference to an entity not yet ingested (e.g., bill references a member), it creates a placeholder row with only the natural key populated. The owning pipeline fills in the full data later via normal upsert + diff.
7. **Pipeline execution helpers** — Config loading from Cloud Run Job arguments, run ID extraction (from Launcher), workflow state updates, and the standard pipeline bootstrap sequence.

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

---

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

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck.ingestion.common
├── api
│   ├── CongressGovPaginatedClient         (3.1)
│   ├── CongressGovClientConfig    (3.1)
│   ├── HttpClientResource         (3.1)
│   ├── FetchParams                (3.1)
│   └── PagedResponse              (3.1)
├── xml
│   ├── XmlFeedClient              (3.2)
│   ├── XmlFeedConfig              (3.2)
│   ├── XmlParsingHelpers          (3.2)
│   ├── SenateXmlUrls              (3.2)
│   └── HouseXmlUrls               (3.2)
├── change
│   ├── ChangeDetector             (3.3)
│   └── ChangeResult               (3.3)
├── events
│   ├── IngestionEventPublisher    (3.4)
│   └── EventTypeConstants         (3.4)
├── db
│   ├── DatabaseConfig             (3.5)
│   ├── TransactorResource         (3.5)
│   └── UpsertHelper               (3.5)
├── placeholder
│   ├── HasPlaceholder             (3.6)
│   ├── PlaceholderCreator         (3.6)
│   └── EntityRepository           (3.6)
├── execution
│   ├── PipelineBootstrap          (3.7)
│   ├── WorkflowStateUpdater       (3.7)
│   └── PipelineFailureHandler     (3.7)
├── logging
│   ├── PipelineLogger             (3.8)
│   ├── PipelineLoggerFactory      (3.8)
│   ├── LogContext                  (3.8)
│   └── LoggingConfig              (3.8)
├── codecs
│   └── DateTimeCodecs             (3.1, migrated)
└── errors
    ├── XmlParseFailed             (3.2)
    ├── XmlFieldMissing            (3.2)
    └── ConfigLoadFailed           (3.7)
```

### Dependencies

```
ingestion-common
├── repcheck-shared-models       (DTOs, DOs, enums)
├── repcheck-pipeline-models     (PipelineEvent, RetryWrapper, ErrorClassifier, etc.)
├── http4s-ember-client          (HTTP)
├── circe                        (JSON)
├── doobie + hikari              (PostgreSQL)
├── http4s-scala-xml             (XML parsing via http4s EntityDecoder)
├── log4cats + logback            (structured logging, JSON output)
├── pureconfig                   (config)
├── fs2                          (streaming)
└── cats-effect                  (effect system)
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
1. `gov-apis` module: remove `PagingApiBase`, `Serializers`, `Constants`. These move to `ingestion-common`.
2. `bill-identifier` module: remove `DoobieBillRepository` base pattern, `DatabaseConfig`, `ConfigLoader`. Rewrite to depend on `ingestion-common` equivalents.
3. Both modules retain entity-specific logic (bill DTOs, bill API endpoint, bill processing) — only shared infrastructure moves out.
