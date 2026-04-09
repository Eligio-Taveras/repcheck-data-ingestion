# Acceptance Criteria: Component 2 — `repcheck-pipeline-models`

> Shared operational library containing pipeline infrastructure types: events, job metadata, error classification, change detection, workflow definitions, launcher execution model, and constants.
> Used by all pipeline repositories (data-ingestion, llm-analysis, scoring-engine) and the Launcher application. NOT used by the API server or shared-models.
> **Depends on**: `repcheck-shared-models` (for domain enums like `Chamber`, `VoteType`, `FindingType`).

---

## System Context

### Pipeline Architecture

RepCheck's data processing is split into **independent scheduled jobs** and **event-driven pipelines**. Scheduled jobs run on their own cron cycles. When a scheduled job detects changes, it publishes events to a Pub/Sub queue. A **Launcher application** (itself triggered on a frequent schedule) pulls messages from the queue, reads workflow definitions from GCS, checks execution state in the database, and launches the appropriate Cloud Run Job with full container configuration.

**Scheduled jobs (independent crons):**

| Job | Purpose | Publishes |
|-----|---------|-----------|
| Member Profile Pipeline | Pull latest member profiles from Congress.gov, detect changes, archive, upsert | `member.updated` (House: always; Senate: only if LIS mapping exists) |
| LIS Mapping Refresher | Refresh senator-lookup.xml LIS-to-bioguide mappings | `member.updated` (for newly mapped senators already in members table) |
| Committees Refresh | Pull committee membership from chamber XMLs | Nothing — keeps data fresh |
| Bill Metadata Ingestion | Pull latest bill metadata from Congress.gov, upsert bills table | Nothing — keeps metadata fresh |
| Bill Text Availability Checker | Scan tracked bills for new/changed text versions | `bill.text.available` per bill with new text |
| Votes Ingestion | Pull votes for already-recorded bills from Congress.gov + Senate XML | `vote.recorded` per new/updated vote |
| Amendments Ingestion | Pull amendments for already-recorded bills from Congress.gov | `amendment.recorded` per new/updated amendment |

**Event-driven pipelines (Launcher reads queue, launches Cloud Run Jobs):**

| Event | Launches | Publishes |
|-------|----------|-----------|
| `bill.text.available` | Bill Text Ingestion Pipeline (downloads text, stores in `bill_text_versions`) | `bill.text.ingested` |
| `bill.text.ingested` | Bill Analysis Pipeline (LLM analysis on the text) | `analysis.completed` |
| `analysis.completed` | Analysis Pipeline (updates `stance_materialization_status` in DB) | Nothing (terminal — stance materialization readiness is polled, not pushed) |
| `vote.recorded` | Votes Pipeline (updates `stance_materialization_status` in DB) | Nothing (terminal — stance materialization readiness is polled, not pushed) |
| `amendment.recorded` | Amendment & Bill Re-analysis Pipeline (analyzes amendment + re-summarizes parent bill) | `analysis.completed` |
| `member.updated` | Pairing Validator (§11.6 — validates user-legislator pairings) | Nothing (terminal) |
| `scoring.user.requested` | Scoring Pipeline (§11.8 — ad-hoc user scoring) | `scoring.user.completed` |

### Launcher Execution Flow

```
Cloud Scheduler (frequent, e.g., every 5 min)
    │
    ▼
Launcher Application (Cloud Run Job)
    │
    ├── 1. Pull messages from Pub/Sub
    │
    ├── 2. For each message: read workflow definition from GCS
    │
    ├── 3. Look up the step the message is requesting
    │
    ├── 4. Check workflow_run_steps in DB — are all dependencies completed?
    │
    ├── 5. Extract image, args, env, resources, networking, etc. from workflow step
    │
    ├── 6. Resolve macros ({{run_id}}, {{date}}, {{message.billId}}, etc.)
    │
    └── 7. Launch target application on Cloud Run via Jobs API
```

### Application Execution Flow

Each launched application:
1. Receives `run_id` and other args from the Launcher
2. Looks up its workflow state in `workflow_run_steps` for that `run_id`
3. Updates its step status to `running`
4. Does its work (ingest, analyze, score, etc.)
5. Updates its step status to `completed`
6. Publishes event(s) to Pub/Sub for downstream steps
7. On failure: increments `retry_count`, requeues its original Pub/Sub message, updates status. After 3 retries, sets status to `failed` and the pipeline stops.

### Placeholder Entity Pattern

When a pipeline ingests data referencing an entity that doesn't exist yet (e.g., a bill's `sponsorBioguideId` for a member we haven't ingested), it creates a **placeholder row** in the target entity's table with only the natural key populated (all other fields null/default). This ensures FK references are always valid. The owning pipeline (e.g., members-pipeline) fills in the full data later via normal upsert — the `ChangeDetector` (Component 3) diffs the placeholder against the full entity and updates all fields.

This pattern applies across all entity types and is implemented in `ingestion-common` (Component 3 §3.6).

### Service Account Boundaries

| Service Account | Used By | Access |
|----------------|---------|--------|
| Launcher SA | Launcher, Daily Upload Initializer | Pub/Sub (pull), GCS (read workflow defs), Cloud Run (launch jobs), AlloyDB (read-only workflow state) |
| Ingestion SA | bills, votes, members, amendments pipelines | Congress.gov API, senate.gov XML, clerk.house.gov XML, AlloyDB (read/write), Pub/Sub (publish) |
| Analysis SA | bill-analysis-pipeline, amendment & bill re-analysis pipeline | AlloyDB (read/write), Pub/Sub (publish), GCS (read prompts), LLM API keys (Anthropic/OpenAI) |
| Scoring SA | scoring-pipeline, score-cache | AlloyDB (read/write), Pub/Sub (publish), GCS (read snapshots) |

All pipeline SAs share: AlloyDB write access to `workflow_run_steps` and Pub/Sub publish for completion events.

### Alerting

Pipeline failures are surfaced through infrastructure, not application code:
1. Applications update `workflow_run_steps.status` to `failed` when retries are exhausted
2. Cloud Monitoring alerting policies detect `failed` status
3. Cloud Monitoring routes to PagerDuty (free tier — up to 5 users, push/email notifications)

No alerting logic in pipeline-models — applications update state honestly, monitoring layer reacts.

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 2.1 Inter-Pipeline Communication | New | Pub/Sub event envelope, typed payloads, event routing |
| 2.2 Pipeline Execution Tracking | New | Run metadata, per-item processing results, status enums |
| 2.3 Error Handling & Retry | New | Error classification, retry wrapper, dead-letter handling |
| 2.4 Change Detection | New | Detection strategies, persistence strategies, event emission rules |
| 2.5 Workflow Definition Schema | New | GCS-stored workflow spec: steps, container config, resources, networking, volumes, identity, health checks, macros |
| 2.6 Workflow Execution State | New | DB-stored runtime state: workflow_runs, workflow_run_steps, status transitions, retry tracking, message storage |
| 2.7 Launcher Execution Model | New | Message pull, dependency resolution, macro resolution, Cloud Run Job invocation |
| 2.8 Pipeline Configuration | New | Vote weights, committee attribution weights, analysis pass config |
| ~~2.9 Deferred Entity Resolution~~ | Removed | Replaced by Placeholder Entity Pattern (Component 3 §3.6) |
| 2.10 Constants | New | Table names, event type strings |

---

## Component Routing Table

| Task | Area File |
|------|-----------|
| Pub/Sub event envelope, typed payloads, event routing | [2.1 Inter-Pipeline Communication](02-pipeline-models/02.1-inter-pipeline-communication.md) |
| Run metadata, per-item processing results, status enums | [2.2 Pipeline Execution Tracking](02-pipeline-models/02.2-pipeline-execution-tracking.md) |
| Error classification, retry wrapper, dead-letter handling | [2.3 Error Handling & Retry](02-pipeline-models/02.3-error-handling-retry.md) |
| Detection strategies, persistence strategies, event rules | [2.4 Change Detection](02-pipeline-models/02.4-change-detection.md) |
| GCS-stored workflow spec: steps, container config, resources | [2.5 Workflow Definition Schema](02-pipeline-models/02.5-workflow-definition-schema.md) |
| DB-stored runtime state: runs, steps, status transitions | [2.6 Workflow Execution State](02-pipeline-models/02.6-workflow-execution-state.md) |
| Message pull, dependency resolution, macro resolution, launch | [2.7 Launcher Execution Model](02-pipeline-models/02.7-launcher-execution-model.md) |
| Vote weights, committee weights, analysis pass config | [2.8 Pipeline Configuration](02-pipeline-models/02.8-pipeline-configuration.md) |
| Deferred Entity Resolution — removed | [2.9 Removed](02-pipeline-models/02.9-removed.md) |
| Table names, event type strings | [2.10 Constants](02-pipeline-models/02.10-constants.md) |

---

## Cross-Cutting Acceptance Criteria

These apply to all areas of `repcheck-pipeline-models`:

**Package Structure:**
- Root package: `repcheck.pipeline.models`
- Sub-packages: `events`, `metadata`, `errors`, `changes`, `workflow`, `workflow.schema`, `workflow.state`, `launcher`, `config`, `constants`

**Build:**
- Published as a versioned artifact to GitHub Packages.
- Depends on `repcheck-shared-models` (for domain enums).
- Dependencies: Circe (semi-auto), Cats Effect (for `RetryWrapper` — needs `Temporal[F]`), PureConfig (auto-derivation), Doobie (Read/Write for DOs), FS2 (for streaming types).

**Database Migrations:**
- New migration required for: `workflow_runs`, `workflow_run_steps` tables.
- `score_history` table needs `reasoning TEXT` column added (noted in Component 1).
- New tables for Component 1: `bill_text_versions`, `committees`, `committee_members`, `bill_committee_referrals`.
- New tables for Component 5: `member_history`, `member_term_history` (archive-before-overwrite pattern).

**Infrastructure (Terraform, not in pipeline-models code):**
- Four service accounts: Launcher SA (read-only DB), Ingestion SA, Analysis SA, Scoring SA.
- Cloud Monitoring alerting policies on `workflow_run_steps.status = 'failed'`.
- PagerDuty integration via Cloud Monitoring notification channel (free tier).
- Secret Manager entries for API keys, DB credentials — referenced by path in workflow definitions, mounted by Cloud Run at container startup.

**Code Quality:**
- All code passes `sbt compile` with WartRemover + tpolecat.
- All code passes `sbt scalafmtCheckAll` and `sbt scalafixAll --check`.
- Test coverage above 90% on all files (enforced by Codecov patch coverage).
- No `@nowarn` or `@SuppressWarnings` annotations.
- Curly brace syntax only (no Scala 3 braceless).

**Documentation:**
- Each public class has a ScalaDoc comment describing its purpose and cross-referencing the relevant section of BEHAVIORAL_SPECS.md or SYSTEM_DESIGN.md.
