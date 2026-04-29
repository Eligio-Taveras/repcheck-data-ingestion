<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/02-PIPELINE-MODELS.md -->

# repcheck-pipeline-models

Shared operational library for pipeline infrastructure: events, job metadata, error classification, change detection, workflow definitions, launcher execution model, and constants. Used by all pipeline repos (data-ingestion, llm-analysis, scoring-engine) and Launcher app. NOT used by API server or shared-models. Depends on `repcheck-shared-models`.

## System Context

### Pipeline Architecture

RepCheck splits processing into **independent scheduled jobs** and **event-driven pipelines**. Scheduled jobs run on cron; when changes detected, they publish events to Pub/Sub. **Launcher app** (triggered frequently) pulls messages, reads workflow definitions from GCS, checks execution state in DB, and launches Cloud Run Jobs.

**Scheduled jobs:**

| Job | Purpose | Publishes |
|-----|---------|-----------|
| Member Profile Pipeline | Pull/detect changes, archive, upsert members | `member.updated` |
| LIS Mapping Refresher | Refresh senator LIS mappings | `member.updated` (newly mapped) |
| Committees Refresh | Pull committee membership | Nothing |
| Bill Metadata Ingestion | Upsert bills | Nothing |
| Bill Text Availability Checker | Scan tracked bills for new text | `bill.text.available` |
| Votes Ingestion | Pull votes for recorded bills | `vote.recorded` |
| Amendments Ingestion | Pull amendments for recorded bills | `amendment.recorded` |

**Event-driven pipelines:**

| Event | Launches | Publishes |
|-------|----------|-----------|
| `bill.text.available` | Bill Text Ingestion | `bill.text.ingested` |
| `bill.text.ingested` | Bill Analysis (LLM) | `analysis.completed` |
| `analysis.completed` | Analysis Pipeline (update materialization status) | Nothing |
| `vote.recorded` | Votes Pipeline (update status) | Nothing |
| `amendment.recorded` | Amendment & Bill Re-analysis | `analysis.completed` |
| `member.updated` | Pairing Validator | Nothing |
| `scoring.user.requested` | Scoring Pipeline | `scoring.user.completed` |

### Launcher Execution Flow

```
Cloud Scheduler (frequent, e.g., every 5 min)
    │
    ▼
Launcher Application (Cloud Run Job)
    │
    ├── 1. Pull messages from Pub/Sub
    │
    ├── 2. Read workflow definition from GCS for each message
    │
    ├── 3. Look up step in workflow
    │
    ├── 4. Check workflow_run_steps DB — all dependencies completed?
    │
    ├── 5. Extract image, args, env, resources, networking from step
    │
    ├── 6. Resolve macros ({{run_id}}, {{date}}, {{message.billId}}, etc.)
    │
    └── 7. Launch target Cloud Run Job via Jobs API
```

### Application Execution Flow

Each launched application:
1. Receives `run_id` and args from Launcher
2. Looks up workflow state in `workflow_run_steps` for `run_id`
3. Updates step status to `running`
4. Does work (ingest, analyze, score)
5. Updates step status to `completed`
6. Publishes event(s) to Pub/Sub for downstream steps
7. On failure: increments `retry_count`, requeues message, updates status. After 3 retries: sets status to `failed`, pipeline stops.

### Placeholder Entity Pattern

When pipeline ingests data referencing non-existent entity (e.g., bill's `sponsorBioguideId` for unmapped member), create **placeholder row** in target entity table with only natural key populated (other fields null/default). Ensures FK validity. Owning pipeline fills full data later via normal upsert — `ChangeDetector` diffs placeholder against full entity and updates all fields. Implemented in `ingestion-common` (Component 3).

### Service Account Boundaries

| Service Account | Used By | Access |
|----------------|---------|--------|
| Launcher SA | Launcher, Daily Upload Initializer | Pub/Sub (pull), GCS (read workflow defs), Cloud Run (launch), AlloyDB (read-only workflow state) |
| Ingestion SA | bills, votes, members, amendments pipelines | Congress.gov API, senate.gov XML, clerk.house.gov XML, AlloyDB (read/write), Pub/Sub (publish) |
| Analysis SA | bill-analysis, amendment & bill re-analysis pipelines | AlloyDB (read/write), Pub/Sub (publish), GCS (read prompts), LLM keys |
| Scoring SA | scoring-pipeline, score-cache | AlloyDB (read/write), Pub/Sub (publish), GCS (read snapshots) |

All pipeline SAs share: AlloyDB write to `workflow_run_steps`, Pub/Sub publish for completion events.

### Alerting

Pipeline failures surfaced via infrastructure, not app code: Applications update `workflow_run_steps.status` to `failed` when retries exhausted; Cloud Monitoring detects and routes to PagerDuty (free tier, 5 users). No alerting logic in pipeline-models.

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 2.1 Inter-Pipeline Communication | New | Pub/Sub event envelope, typed payloads, routing |
| 2.2 Pipeline Execution Tracking | New | Run metadata, per-item results, status enums |
| 2.3 Error Handling & Retry | New | Error classification, retry wrapper, dead-letter |
| 2.4 Change Detection | New | Detection strategies, persistence, event rules |
| 2.5 Workflow Definition Schema | New | GCS workflow spec: steps, container, resources, networking, volumes, identity, health, macros |
| 2.6 Workflow Execution State | New | DB state: runs, steps, status transitions, retries, messages |
| 2.7 Launcher Execution Model | New | Message pull, dependency resolution, macro resolution, launch |
| 2.8 Pipeline Configuration | New | Vote weights, committee weights, analysis config |
| 2.10 Constants | New | Table names, event type strings |

## Cross-Cutting Acceptance Criteria

**Package Structure:**
- Root: `repcheck.pipeline.models`
- Sub-packages: `events`, `metadata`, `errors`, `changes`, `workflow`, `workflow.schema`, `workflow.state`, `launcher`, `config`, `constants`

**Build:**
- Published to GitHub Packages, versioned.
- Depends on `repcheck-shared-models`.
- Dependencies: Circe (semi-auto), Cats Effect (for RetryWrapper), PureConfig (auto-derivation), Doobie (Read/Write), FS2.

**Database Migrations:**
- New: `workflow_runs`, `workflow_run_steps` tables.
- Add: `reasoning TEXT` column to `score_history`.
- New (Component 1): `bill_text_versions`, `committees`, `committee_members`, `bill_committee_referrals`.
- New (Component 5): `member_history`, `member_term_history` (archive-before-overwrite).

**Infrastructure (Terraform):**
- Four service accounts: Launcher (read-only DB), Ingestion, Analysis, Scoring.
- Cloud Monitoring alerting on `workflow_run_steps.status = 'failed'`.
- PagerDuty integration via Cloud Monitoring (free tier).
- Secret Manager for API keys/DB credentials — referenced in workflow defs, mounted by Cloud Run.

**Code Quality:**
- Passes `sbt compile` with WartRemover + tpolecat.
- Passes `sbt scalafmtCheckAll`, `sbt scalafixAll --check`.
- Test coverage >90% (Codecov patch).
- No `@nowarn` or `@SuppressWarnings`.
- Curly brace syntax only.

**Documentation:**
- ScalaDoc on all public classes: purpose and cross-reference to BEHAVIORAL_SPECS.md or SYSTEM_DESIGN.md.