<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/PATTERNS_GUIDE.md -->

# RepCheck Code Templates & Pattern Guide

**Use this guide to:** Find which templates apply to your repo (see [Template Map](#template-map-which-templates-apply-to-which-repo)). Copy skeletons, read annotated examples, apply architectural decisions.

---

## Template Map — Which Templates Apply to Which Repo

| Repo | Templates Needed | Deployment Archetype |
|------|-----------------|---------------------|
| `repcheck-shared-models` | error-pattern, config-pattern | Library (GitHub Packages) |
| `repcheck-pipeline-models` | retry-wrapper, pubsub-publisher, pubsub-subscriber, alloydb-repository, gcs-reader, config-pattern, error-pattern, streaming-pipeline | Library (GitHub Packages) |
| `repcheck-data-ingestion` | pipeline-app, streaming-pipeline, config-pattern, error-pattern, test-templates + annotated: paginated-api-client, dto-do-layering, ioapp-entry-point, alloydb-persistence | Pipeline (dockerfile-pipeline, cloud-run-job, docker-compose-local-dev, github-actions-deploy) |
| `repcheck-llm-client` | llm-client-adapter, config-pattern, error-pattern, test-templates | Library (GitHub Packages) |
| `repcheck-prompt-engine-bills` | prompt-engine, gcs-reader, config-pattern, test-templates | Config (github-actions-deploy, GCS sync) |
| `repcheck-prompt-engine-users` | prompt-engine, alloydb-repository, doobie-repository, gcs-reader, config-pattern, test-templates | Config (github-actions-deploy, GCS sync) |
| `repcheck-llm-analysis` | pipeline-app, streaming-pipeline, prompt-engine, llm-client-adapter, alloydb-repository, doobie-repository, config-pattern, error-pattern, test-templates | Pipeline (dockerfile-pipeline, cloud-run-job, docker-compose-local-dev, github-actions-deploy) |
| `repcheck-scoring-engine` | pipeline-app, streaming-pipeline, prompt-engine, llm-client-adapter, alloydb-repository, doobie-repository, config-pattern, error-pattern, test-templates | Pipeline (dockerfile-pipeline, cloud-run-job, docker-compose-local-dev, github-actions-deploy) |
| `repcheck-orchestrator` | orchestrator, pubsub-subscriber, pubsub-publisher, workflow-definition, config-pattern, error-pattern, test-templates | Pipeline (dockerfile-pipeline, cloud-run-job, github-actions-deploy) |
| `repcheck-snapshot-service` | snapshot-service, gcs-reader, alloydb-repository, doobie-repository, config-pattern, test-templates | Pipeline (dockerfile-pipeline, cloud-run-job, github-actions-deploy) |
| `repcheck-api-server` | alloydb-repository, doobie-repository, config-pattern, error-pattern, test-templates | Service (future — Cloud Run Service) |

---

## Directory Structure

```
docs/
├── reference/
│   └── congress-gov-api.yaml          ← Congress.gov OpenAPI v3 spec (local copy)
└── templates/
    ├── PATTERNS_GUIDE.md                  ← You are here
    ├── annotated/
    │   ├── paginated-api-client.md        ← PagingApiBase + LegislativeBillsApi
    │   ├── dto-do-layering.md             ← 3-layer DTO conversion pattern
    │   ├── ioapp-entry-point.md           ← IOApp structure
    │   ├── alloydb-persistence.md         ← AlloyDB / Doobie persistence
    │   ├── config-loading.md              ← PureConfig pattern
    │   ├── enum-with-parsing.md           ← Either-based enum parsing
    │   ├── circe-codecs.md                ← JSON serialization patterns
    │   ├── test-patterns.md               ← Test scaffolds
    │   ├── testing-infrastructure.md      ← Isolation, mocking, e2e, auto-bug filing
    │   ├── deployment-architecture.md     ← 4 archetypes, GCP auth, promotion
    │   └── congress-entity-api.md         ← Generic Congress.gov entity spec
    └── skeletons/
        ├── retry-wrapper.scala
        ├── pubsub-publisher.scala
        ├── pubsub-subscriber.scala
        ├── orchestrator.scala
        ├── doobie-repository.scala
        ├── gcs-reader.scala
        ├── snapshot-service.scala
        ├── streaming-pipeline.scala
        ├── llm-client-adapter.scala
        ├── prompt-engine.scala
        ├── alloydb-repository.scala
        ├── pipeline-app.scala
        ├── congress-entity-api.scala
        ├── config-pattern.scala
        ├── error-pattern.scala
        ├── test-templates.scala
        ├── workflow-definition.scala
        ├── dockerfile-pipeline.txt
        ├── docker-compose-local-dev.yml
        ├── cloud-run-job.yaml
        ├── github-actions-deploy.yml
        └── github-actions-bug-on-failure.yml
```

---

## Key Architectural Decisions

### Effect System & Code Style
| Decision | Choice |
|----------|--------|
| Effect system | Tagless final `F[_]` everywhere |
| JSON | Circe with semi-auto derivation |
| HTTP | http4s Ember client |
| Streaming | FS2 with `parEvalMap(config.parallelism)` always |
| Config | PureConfig with auto-derivation (pureconfig-generic-scala3) |
| DB | Doobie for PostgreSQL (auto-derived Read/Write, pgvector for embeddings). AlloyDB in staging/prod, Cloud SQL in dev. |
| Testing | ScalaTest AnyFlatSpec with Matchers |

### Error Handling
| Decision | Choice |
|----------|--------|
| Exception style | Flat case classes extending Exception (no sealed hierarchies) |
| Error classification | Per-subsystem `ErrorClassifier` trait (adapter owns classification) |
| Error severity | `Transient` (log + continue) vs `Systemic` (halt pipeline) |
| Uniqueness | Every exception is unique — stack trace tells you exactly where and why |

### Retry & Resilience
| Decision | Choice |
|----------|--------|
| Retry wrapper | Centralized in pipeline-models, per-subsystem config |
| Default retries | 3, configurable per subsystem |
| Backoff | Exponential, 10ms initial, 2x multiplier, 60s max cap |
| Timeout | Configurable per subsystem |
| Circuit breaking | Systemic errors halt pipeline immediately |

### Streaming & Processing
| Decision | Choice |
|----------|--------|
| Parallelism | Always `parEvalMap(config.parallelism)`, sequential = parallelism 1 |
| Fail-and-continue | `ProcessingResult` per item, persisted to AlloyDB immediately, released from memory |
| Summary | `PipelineRunSummary` aggregated from AlloyDB after stream completes |
| Correlation ID | UUID per item, visible in all logs and serialization |

### Data & Storage
| Decision | Choice |
|----------|--------|
| DTO/DO layering | DTOs and DOs in separate `models/` sub-project per repo, published as library |
| Persistence | Doobie SQL with `ON CONFLICT DO UPDATE` for idempotent upserts |
| Table names | Constants in `pipeline-models` (`object Tables`) |
| ID strategy | Natural keys for legislative data, generated UUIDs for RepCheck entities |

### Pub/Sub & Orchestration
| Decision | Choice |
|----------|--------|
| Message format | `PipelineEvent[T]` envelope with metadata + ResourceRequirements |
| Orchestrator | Cloud Scheduler → reads Pub/Sub → checks Cloud Run capacity → launches Job or re-enqueues |
| Dead-letter | Separate queue, GCP Monitoring alert to free-tier alerting platform |
| Deployment | Cloud Run Jobs (batch: run, process, exit) |

### Prompts & LLM
| Decision | Choice |
|----------|--------|
| Prompt storage | ALL fragments in GCS, NONE in code |
| Prompt composition | `PromptFragment` trait, priority-ordered, `PromptBuilder` assembles |
| Versioning | Semver in filenames, version configurable per app, CI deploys default |
| LLM abstraction | Vendor-neutral `LlmRequest/LlmResponse`, pluggable `LlmAdapter` per provider |
| Multi-provider | `LlmDispatcher` fans out concurrently to all configured adapters |
| User preferences | AlloyDB (Doobie), woven into scoring prompts |

### Snapshots & State
| Decision | Choice |
|----------|--------|
| Snapshot service | Dedicated first step in state machine |
| Snapshot data | AlloyDB → JSON → GCS |
| Apps read from | Snapshots (GCS), NOT live databases |
| Only DB writes during run | Pipeline run status tracking |
| Workflow definitions | Published from GitHub CI to GCS, independent semver |

### Testing Strategy
| Decision | Choice |
|----------|--------|
| Unit mocking | MockitoScala for trait mocking |
| HTTP simulation | WireMock for Congress.gov, LLM provider responses, failure scenarios |
| Negative testing | Analyze each function line-by-line for failure inputs |
| Integration (local) | AlloyDB Omni (Docker), Pub/Sub emulator |
| Integration (WireMock) | Failure simulation only (timeouts, errors, malformed responses) |
| Integration (dev GCP) | Contract/connection validation, namespaced per developer/CI |
| Test isolation | Ephemeral schema prefix per test run (`test_{uuid}_`). Cleanup in `afterAll()` |
| E2E tests | Same modules, `taggedAs E2ETest`, excluded from `sbt test`, separate run command |
| CI | ALL test types run on every PR commit, must pass locally with Docker |
| Bug automation | GitHub Actions creates Issue on test failure, auto-closes on pass |

### Deployment & CI/CD
| Decision | Choice |
|----------|--------|
| Container runtime | Cloud Run Jobs (batch pipelines), Cloud Run Services (API, future) |
| Container base image | `gcr.io/distroless/java21-debian12` (runtime), `eclipse-temurin:21-jdk` (build) |
| Container registry | Artifact Registry (`us-central1-docker.pkg.dev/repcheck-{env}/repcheck-images`) |
| GCP auth in CI | Workload Identity Federation (keyless OIDC, no service account JSON keys) |
| Library publishing | GitHub Packages (Maven), triggered on version tags (`v*`) |
| Prompt config deploy | `gsutil rsync` to GCS, triggered on merge to main |
| Local dev | Docker Compose: google/alloydbomni (AlloyDB Omni — real engine), Pub/Sub emulator |
| Database (dev) | Cloud SQL PostgreSQL db-f1-micro (~$10/mo) — same Doobie code, pgvector via extension |
| Database (staging/prod) | AlloyDB (columnar engine, optimized pgvector, ~$390+/mo) |
| GCP projects | 3 environments: `repcheck-dev`, `repcheck-staging`, `repcheck-prod` |
| Promotion | Dev → Staging (auto if e2e pass) → Prod (manual approval gate) |

### Publishing & CI
| Decision | Choice |
|----------|--------|
| Shared models | Published via GitHub Packages (Maven) |
| Project structure | `models/` + `app/` split only when repo has both publishable types and runtime code |
| CI | GitHub Actions: compile (WartRemover + tpolecat) → scalafix → tests → coverage |

---

## Cross-References

- **System Design**: `docs/architecture/SYSTEM_DESIGN.md` — full architecture with Mermaid diagrams
- **Code Standards**: `docs/architecture/SCALA_CODE_PATTERNS.md` — Scala patterns, conventions, compile-time enforcement
- **Behavioral Specs**: `docs/architecture/BEHAVIORAL_SPECS.md` — diffing, scoring, event emission rules
- **Deployment Architecture**: `docs/templates/annotated/deployment-architecture.md` — 4 archetypes, GCP auth, promotion pipeline
- **Testing Infrastructure**: `docs/templates/annotated/testing-infrastructure.md` — isolation, mocking, e2e, auto-bug filing
- **Existing Source**: `gov-apis/` and `bill-identifier/` — working reference implementation of bill ingestion