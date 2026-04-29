<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/BEHAVIORAL_SPECS.md -->

# RepCheck Behavioral Specifications

## 1. Change Detection & Persistence Strategy

Per-entity rules for detecting and handling changes from Congress.gov:

| Entity | Natural Key | Change Detection | Persistence Strategy | Event Trigger |
|--------|------------|-----------------|---------------------|---------------|
| Bill | `{congress}-{billType}-{number}` | `updateDate` from API > stored `updateDate` | Upsert (overwrite) in AlloyDB | Emit `bill.text.available` whenever `updateDate` changed AND `textUrl` is non-null |
| Vote | `{congress}-{chamber}-{rollNumber}` | `updateDate` from API > stored `updateDate`, or vote doesn't exist yet | Upsert with history: archive prior version to `vote_history` table, then overwrite main row. Updates `stance_materialization_status.has_votes` in DB | Emit `vote.recorded` on first insert AND on updates where vote positions changed |
| Member | `{bioguideId}` | Any field differs from stored document | Upsert (overwrite) in AlloyDB | No event (no downstream consumers) |
| Amendment | `{congress}-{amendmentType}-{number}` | `updateDate` from API > stored `updateDate` | Upsert (overwrite) in AlloyDB | No event (consumed on-demand by analysis) |
| Analysis | `{billId}-{passNumber}-{version}` | N/A (always creates new version) | Insert new version (append-only, never overwrite). Updates `stance_materialization_status.has_analysis` and `all_passes_completed` in DB | Emit `analysis.completed` after final pass |

**Key Rules:**
- No field-by-field diffing — compare `updateDate` timestamps only (bills/amendments) or existence check + position comparison (votes)
- Votes are diffed and upserted — prior version saved to `vote_history` before main row overwritten. Scoring always uses latest version only; history is for audit
- Analysis is append-only — re-analyzing the same bill creates a new version, preserving full audit trail
- `bill.text.available` emits on every qualifying re-ingest for re-analysis capability
- Scoring is decoupled from data-change events — `vote.recorded` and `analysis.completed` update DB status flags in `stance_materialization_status`. Stance materialization readiness determined by scheduled DB scanner (§3.2), not by consuming events directly

---

## 2. Join Keys & Entity Linking

**Foreign Key Relationships:**
```
BILL.billId       ← VOTE.billId              (many votes per bill)
BILL.billId       ← AMENDMENT.billId          (many amendments per bill)
BILL.billId       ← ANALYSIS.billId           (many analysis versions per bill)
VOTE.voteId       ← VOTE_POSITION.voteId      (many positions per vote)
MEMBER.memberId   ← VOTE_POSITION.memberId    (many positions per member)
USER.userId       ← PREFERENCE.userId          (many preferences per user)
USER.userId + MEMBER.memberId → SCORE          (composite key)
```

**Congress Scoping:**
- Bills, votes, amendments scoped to specific congress number
- Members span multiple congresses (same `bioguideId` across terms)
- Scoring is perpetual — legislator's votes from ALL congresses contribute to aggregate score
- Two score tiers: Aggregate score (lifetime), Per-congress score (single congress)
- `congress` field on bills/votes partitions per-congress scores; aggregate spans all congresses

**Vote-to-Bill Linkage:**
- Congress.gov API includes `bill` object in each vote response with `billId`, `congress`, `type`, `number`
- Stored as `VOTE.billId` FK to `BILL.billId`
- Procedural votes (null `billId`) stored but excluded from alignment scoring
- Single bill can have multiple votes at different stages (committee, cloture, floor, conference)

**Vote Type Detection & Weights:**

| Vote Type | `question` Field Pattern | Legislative Meaning | User-Facing Explanation |
|-----------|------------------------|---------------------|------------------------|
| Committee vote | "Reported favorably" / "Ordered to be reported" | Advances bill out of committee | "Voted to advance this bill from committee for consideration by the full [House/Senate]" |
| Cloture (Senate) | "On Cloture" / "On the Cloture Motion" | Ends debate / breaks filibuster (60 votes) | "Voted to end debate on this bill, allowing it to proceed to a final vote" |
| Floor passage | "On Passage" / "On Motion to Suspend the Rules and Pass" | Approves bill in one chamber | "Voted to pass this bill in the [House/Senate]" |
| Amendment vote | "On Agreeing to the Amendment" | Modifies bill text before passage | "Voted on a proposed change to the bill's text" |
| Conference report | "On Agreeing to the Conference Report" | Approves final reconciled version | "Voted to approve the final version of this bill after both chambers agreed on the text" |
| Motion to recommit | "On Motion to Recommit" | Sends bill back to committee | "Voted to send this bill back to committee" |
| Veto override | "On Overriding the Veto" | Overrides presidential veto (2/3 majority) | "Voted to override the President's veto of this bill" |

**Vote Weight Configuration:**
```hocon
scoring.vote-weights {
  passage = 1.0
  conference-report = 1.0
  cloture = 0.8
  veto-override = 0.9
  amendment = 0.5
  committee = 0.4
  motion-to-recommit = 0.6
}
```
Weights guide LLM prompt emphasis; unknown patterns default to 0.5 and flagged for review.

### Bill Text Decomposition

Large bills undergo decomposition before analysis passes run (omnibus bills cannot fit single LLM context window).

**Decomposition Steps:**

1. **Text parsing / section identification** (Ollama sidecar) — Ollama instance as Cloud Run sidecar reads bill text (plain text, PDF-extracted, or XML) and identifies sections: boundaries, headings, numbering. XML provides structural hints; plain text uses legislative formatting conventions. Each section becomes `BillTextSectionDO` row. Cost: minimal (local Ollama, no API calls).

2. **In-process section embedding** (DJL + ONNX Runtime, no API calls) — Embed each section using sentence-transformer (e.g., `all-MiniLM-L6-v2`, ~80MB) loaded via DJL with ONNX Runtime backend. Produces 384-dimensional dense vectors for clustering. Runs in-process, no external API. Throughput: hundreds of sections/second on CPU.

3. **Semantic clustering** (Smile ML library, no AI) — Cluster section vectors using k-means or DBSCAN to produce concept groups. Sections about "transportation funding" group together. Cluster count determined dynamically (5-20 for large bills, minimum 2 sections/group). Deterministic and free.

4. **LLM-assisted simplification** (Haiku API, only external cost step) — For each concept group, call LLM for coherent summary using `concept-simplification` profile from `repcheck-prompt-engine-bills`. Cost: ~$0.001/group, 10-20 groups/large bill = $0.01-0.02.

5. **Result persisted to AlloyDB:**
   - `bill_text_sections` — one row per section (content, ordinal index, embedding)
   - `bill_concept_groups` — one row per concept group (simplified text, title, embedding)
   - `bill_concept_group_sections` — junction linking sections to groups
   - Tied to `bill_text_versions.version_id`, immutable once produced, reusable across re-analyses
   - 1536-dim embeddings generated separately for semantic search (384-dim ephemeral vectors for clustering only)

**Ollama Sidecar Architecture:**
- Model: Small, fast text-parsing model (e.g., Llama 3.2 1B). Only identifies section boundaries and headings.
- Communication: HTTP API on localhost (`http://localhost:11434`). JVM calls via http4s.
- Lifecycle: Starts with Cloud Run Job, shares network namespace. No external ingress.
- Cost: Runs on Cloud Run Job allocated CPU/memory. No per-token API charges.
- Why sidecar? Text parsing requires instruction-following LLM. Ollama provides this without external API costs.

**Key Rules:**
- Short bills fitting context window skip decomposition (raw text used directly, single concept group)
- `bill-analysis-pipeline` (Component 10) owns decomposition orchestration (when, how to parse, embed, group)
- Component 8 provides only LLM-assisted simplification prompts
- Decomposition results persisted to AlloyDB as bill text layer artifacts, tied to text version, reusable across re-analyses
- DJL embedding model (`all-MiniLM-L6-v2` or equivalent) bundled in container (~80MB). Produces 384-dim vectors for clustering only — NOT the 1536-dim pgvector embeddings stored for semantic search
- Smile clustering library runs in-process on JVM. No external dependency beyond Maven artifact.
- Ollama sidecar model pulled at container build time and cached in image.

### Embedding Generation for Semantic Search

Bill text and analysis outputs vectorized (pgvector, 1536 dimensions) for semantic search across two distinct layers:

**Bill text layer** (generated during decomposition, tied to text version):

| Source | Text Field | Target Table / Column | DO | Enables |
|--------|-----------|----------------------|-----|---------|
| Parsed sections | `BillTextSectionDO.content` | `bill_text_sections.embedding` | `BillTextSectionDO` | Find sections across all bills about broadband funding |
| Concept group summaries | `BillConceptGroupDO.simplifiedText` | `bill_concept_groups.embedding` | `BillConceptGroupDO` | Find concept groups similar to this across bills |
| Full text version | `BillTextVersionDO.content` | `bill_text_versions.embedding` | `BillTextVersionDO` | Find bills with similar full text |

**Analysis layer** (generated after each pass, tied to analysis run):

| Source | Text Field | Target Table / Column | DO | Enables |
|--------|-----------|----------------------|-----------|---------|
| Pass 1 overall summary | `BillSummaryOutput.summary` | `bill_analyses.embedding` | `BillAnalysisDO` | Find bills similar to this one |
| Pass 1 per-concept summaries | `ConceptSummaryResult.summary` | `bill_concept_summaries.embedding` | `BillConceptSummaryDO` | Find analysis results for similar concepts |
| Pass 1 topic classifications | `TopicScore.topic` values | `bill_subjects.embedding` | `BillSubjectDO` | Semantic topic similarity (beyond exact matching) |
| Pass 2 pork findings | `PorkFinding.description` | `bill_findings.embedding` | `BillFindingDO` | Find similar earmark/rider patterns |
| Pass 2 impact analysis | `ImpactItem.description` | `bill_findings.embedding` | `BillFindingDO` | Find bills with similar impact on this group |
| Amendment descriptions | `AmendmentSummary.description` | `amendment_findings.embedding` | `AmendmentFindingDO` | Amendment semantic search |

**Key Rules:**
- Embedding model configured per deployment (e.g., OpenAI `text-embedding-3-small`, 1536 dimensions)
- Embeddings generated after each pass completes, before pipeline moves to next pass
- HNSW indexes with cosine distance defined for all embedding columns
- Embedding generation failures non-fatal — results persisted but bill won't appear in semantic search until regenerated

### AlloyDB Schema — Bill Text & Analysis (Two Table Layers)

Bill text decomposition and LLM analysis results stored in **separate, normalized table layers** (no JSONB):

**Bill text layer** (tied to text version, immutable, reusable across re-analyses):

- **`bill_text_sections`** — One row per structural section. Stores content, identifier (e.g., "Sec. 101"), heading, ordinal index, embedding. Linked to `bill_text_versions.version_id`.
- **`bill_concept_groups`** — One row per concept group from decomposition. Stores simplified text (Haiku summary), title, embedding. Linked to `bill_text_versions.version_id`. Multiple analysis runs can reference same groups.
- **`bill_concept_group_sections`** — Junction linking sections to concept groups (one section per group).

**Analysis layer** (tied to analysis run, one set per analysis):

- **`bill_analyses`** — One row per analysis run. Keyed by `analysis_id` (UUID). Stores Pass 1 overall summary (summary, reading_level, key_points), topic tags (topics TEXT[] for events), per-pass model names, pass_completed (1, 2, or 3), embedding.
- **`bill_concept_summaries`** — One row per concept group per analysis. References concept group via FK. Stores per-concept Pass 1 results (summary, reading_level, key_points, topics), embedding. Linked to `bill_analyses.analysis_id`.
- **`bill_analysis_topics`** — One row per topic per analysis. Stores `TopicClassificationOutput` with confidence scores. `concept_group_id` NULL for bill-wide, set for per-concept.
- **`bill_findings`** — One row per finding (pork, impact, stance, etc.). Discriminated by `finding_type_id` FK. Columns: `concept_group_id` (nullable), severity, confidence, affected_section, affected_group. Each finding has embedding.
- **`bill_fiscal_estimates`** — One row per analysis run. Stores `FiscalEstimateOutput`: estimated_cost, timeframe, confidence, assumptions TEXT[].
- **`amendment_findings`** — Same pattern as `bill_findings` for amendments.

**Key Rules:**
- Latest analysis: query `WHERE bill_id = ? ORDER BY analyzed_at DESC LIMIT 1`
- All rows for one analysis share same `analysis_id` across tables
- Concept groups and sections shared across analysis runs — belong to text version
- Structured outputs (`Pass1Output`, `Pass2Output`) not stored as JSONB — decomposed into normalized tables after each pass
- Finding types: `finding_types` lookup table seeded with: topic_extraction, bill_summary, policy_analysis, stance_detection, impact_analysis, fiscal_estimate, pork, rider, lobbying, constitutional

**Vote Position Query Pattern:**
- Fetch all positions for vote: `vote_positions WHERE vote_id = ?`
- Fetch specific member's position: `vote_positions WHERE vote_id = ? AND member_id = ?`
- Fetch member's votes on bill: `votes WHERE bill_id = ?` then JOIN `vote_positions ON vote_id AND member_id = ?`
- Secondary index on `(bill_id)` in `votes` and `(vote_id, member_id)` PK in `vote_positions`

---

## 3. Batch Scoring Architecture

Scoring decoupled from data-change events. Uses combination of scheduled jobs and DB polling to determine when data is ready for processing.

| Process | Trigger | Input | Output |
|---------|---------|-------|--------|
| A. Pairing Validator (§11.6) | Scheduled + user location change + `member.updated` | `users`, `members`, `member_terms` | `user_legislator_pairings` |
| B. Ingestion Pipelines | Event-driven (Components 4/6/7/10) | Congress.gov API | bills, votes, amendments, findings + `stance_materialization_status` |
| C. Stance Materializer (§11.9) | Scheduled scanner (polls DB) | `vote_positions`, `bill_findings`, `stance_materialization_status` | `member_bill_stances` + `member_bill_stance_topics` |
| D. User-Bill Alignment (§11.10) | Scheduled | `users`, `bill_findings`, `member_bill_stance_topics` | `user_bill_alignments`, `user_amendment_alignments` |
| E. User-Member Scoring (§11.7, §11.8) | Scheduled + ad-hoc | `user_legislator_pairings`, `member_bill_stance_topics`, `user_bill_alignments` | `scores`, `score_topics`, `score_history` |
| F. Score Refresh Notifier (§11.11) | On ad-hoc completion | scoring results | `scoring.user.completed` event |

### 3.1 User-Legislator Pairings

- **At signup:** Look up user's state/district → query `member_terms` for current legislators (`end_year IS NULL OR end_year >= current year`) → insert `user_legislator_pairings` rows
- **Scheduled validation:** Scan all users, check each pairing against current `member_terms`, add new legislators, remove stale ones
- **On user location change:** `validateForUser(userId)` re-computes pairings
- **On `member.updated`:** Pairing Validator checks if terms changed, updates affected pairings

### 3.2 Stance Materialization

Scanner polls DB for bills where both conditions met. Does NOT subscribe to Pub/Sub events.

**Status tracking (by ingestion pipelines):**
- **Votes pipeline:** On vote position insert/update, upserts `stance_materialization_status` with `has_votes = true, votes_updated_at = NOW()`
- **Analysis pipeline:** After `completeAnalysisRun()`, upserts `stance_materialization_status` with `has_analysis = true, all_passes_completed = true, analysis_completed_at = NOW()`

**Scanner query:**
```sql
SELECT bill_id FROM stance_materialization_status
WHERE has_votes = true
  AND all_passes_completed = true
  AND (stances_materialized_at IS NULL
       OR stances_materialized_at < GREATEST(votes_updated_at, analysis_completed_at))
```

**Materialization per bill:**
1. Fetch `vote_positions` for bill
2. Fetch analysis topics + stance findings from `bill_findings`
3. For each (member, vote, topic): compute stance direction, generate per-topic reasoning (LLM), generate reasoning embedding (DJL/ONNX), link to finding
4. Upsert `member_bill_stances` parent row
5. Delete + insert `member_bill_stance_topics` children (per-topic stance with reasoning, embedding, finding FK)
6. Update `stances_materialized_at = NOW()`
7. Update `users.last_stance_change_at` for all paired users of affected members

**Key Rules:**
- Eliminates dual-event coordination complexity — scanner waits until both flags set
- Scanner runs on schedule (e.g., every 15 minutes), processes all qualifying bills in batch
- Materialization is idempotent — re-running same bill with same data produces same result

### 3.3 User-Bill Alignment

Scheduled job pre-computes per-bill, per-topic alignment between each user's stances and bill's stance findings:

- **Changed-bill optimization:** Only process bills where `stances_materialized_at` updated since last alignment run
- **Changed-user optimization:** Only process users whose stances changed (Q&A responses updated)
- For each (user, bill) pair with overlapping topics: compute alignment score, generate reasoning (LLM), generate reasoning embedding (DJL/ONNX), link to finding
- Upsert to `user_bill_alignments` / `user_amendment_alignments`

### 3.4 User-Member Scoring

Runs on schedule or triggered ad-hoc per user.

**Scheduled mode:**
1. Query users where `last_stance_change_at > last_scoring_run_at` for any paired legislator
2. For each qualifying user: `scoreUser(userId, correlationId)`

**Ad-hoc mode:**
1. Subscribe to `scoring.user.requested` events
2. For each event: `scoreUser(event.userId, event.requestId)`
3. Publish `scoring.user.completed` on completion

**Phased scoring (`scoreUser`):**
1. Read pairings → get legislators
2. Aggregate user stances (§11.1)
3. **Phase 1 — Numeric scores (parallel):** For each legislator: read profile (§11.2), read user-bill alignments, calculate alignment (§11.3), check no-overlap → persist all numeric scores (immediately visible to frontend)
4. **Phase 2 — LLM explanations (parallel):** For each legislator: fetch evidence (§11.4), generate LLM explanation, generate reasoning embedding → persist all explanations

Two-phase approach ensures numeric scores visible immediately. LLM explanation generation (slower, more expensive) does not block score visibility.

### 3.5 Ad-Hoc Scoring

Request/reply pattern for on-demand user scoring:

1. API Server (or scheduler) publishes `ScoringUserRequestedEvent(userId, requestId, source)` to Pub/Sub
2. Scoring Pipeline subscribes, processes user via `scoreUser`
3. On completion, Score Refresh Notifier publishes `ScoringUserCompletedEvent(userId, requestId, memberScoreCount, status)` to return queue
4. API Server reads return queue and notifies user

### 3.6 Skip-Unchanged Optimization

- `users.last_stance_change_at` — updated by stance materializer (§3.2 step 7) when any paired legislator's stances change, and by Q&A submission when user preferences change
- `stance_materialization_status.stances_materialized_at` — updated per bill when stances materialized
- Scoring pipeline checks: have any paired legislators had stance changes since user's last scoring run? If not, skip.

### Score Structure (Two Tiers + History)

Fully normalized tables (see Component 1 §1.5 for DOs):

- **`scores`** — Current score per (userId, memberId) pair. Overwritten on re-score. Contains aggregate_score, status, reasoning, reasoning_embedding.
- **`score_topics`** — Per-topic scores for current scoring run.
- **`score_congress`** — Per-congress aggregate scores.
- **`score_congress_topics`** — Per-congress per-topic scores.
- **`score_history`** — Append-only audit trail with trigger_event (`"scheduled"` or `"ad-hoc"`), reasoning, reasoning_embedding.
- **`score_history_congress`**, **`score_history_congress_topics`**, **`score_history_highlights`** — History detail tables.

**Scoring Computation Rules:**
- Each scoring run computes BOTH aggregate and per-congress scores
- **Aggregate** = weighted combination of all per-congress scores (more recent congresses weighted higher)
- **Per-congress** = based only on votes and analyses from that specific congress
- Latest scores overwrite `scores` table for fast frontend reads
- Each scoring run appends to `score_history` (enables "alignment over time" trend charts)
- Scoring always uses latest version of each bill and vote (historical versions not scored)
- History records what score WAS at each point in time — not re-scored independently

---

## 4. Event Emission — Precise Conditions

| Event | Emitted By | Condition | Payload |
|-------|-----------|-----------|---------|
| `bill.text.available` | bills-pipeline | `textUrl` non-null AND `updateDate` from API newer than stored `updateDate` | `{ billId, congress, textUrl, textFormat }` |
| `vote.recorded` | votes-pipeline | Vote is new (first insert) OR vote positions changed on upsert. Also updates `stance_materialization_status.has_votes` in DB. | `{ voteId, billId, chamber, date, congress, isUpdate: boolean }` |
| `analysis.completed` | bill-analysis-pipeline | Final pass completes. Also updates `stance_materialization_status.has_analysis` and `all_passes_completed` in DB. | `{ billId, analysisId, topics[], passesExecuted, modelUsed }` |
| `user.profile.updated` | api-server (future) | User submits new Q&A responses differing from stored (no-op if identical resubmission) | `{ userId, topicsChanged[] }` |
| `scoring.user.requested` | scoring scheduler / API server | Scheduled run enqueues per-user event, or user requests ad-hoc re-score | `{ userId, requestId, source }` |
| `scoring.user.completed` | scoring pipeline (§11.11) | Ad-hoc scoring run completes for user | `{ userId, requestId, memberScoreCount, status }` |

**Event Ordering & Idempotency:**
- Pub/Sub does NOT guarantee ordering — all pipelines must be idempotent
- `vote.recorded` and `analysis.completed` update DB status flags (`stance_materialization_status`). Stance materialization determined by DB scanner (§3.2), not by consuming events directly. Eliminates race condition where `vote.recorded` arrives before analysis exists — scanner waits until both flags set.
- Duplicate events safe — all pipelines use upsert semantics. Next scheduled run self-corrects
- `scoring.user.requested` events processed idempotently — re-scoring same user produces same output if data unchanged

**Dead-Letter Policy:**
- Messages failing after max retries (3) moved to dead-letter topic
- Dead-lettered messages trigger GCP Monitoring alert per topic
- Recovery: manual replay from dead-letter after root cause fixed
- No automatic retry from dead-letter — human intervention required
- No requeue-on-missing-dependency logic needed — stance materialization handles data convergence via DB polling (§3.2)

---

## 5. Workflow Execution Rules

**Snapshot Semantics:**
- Each workflow run creates fresh snapshot (point-in-time AlloyDB data to GCS)
- Snapshots NOT shared across concurrent runs
- Downstream steps read from run's snapshot, not live AlloyDB
- Snapshot path passed via env var `SNAPSHOT_PATH` to each Cloud Run Job
- Exception: pipeline run status writes go directly to AlloyDB

**Step Completion Criteria:**
- Step **complete** when Cloud Run Job exits with code 0
- Partial failures (some items failed, some succeeded) = step **succeeds** if exit code 0
- Failed items logged in `ProcessingResult`; step not retried for individual item failures
- Step **failed** only if Job exits non-zero code (systemic failure)

**Error Escalation:**
- If step N fails, all dependent steps **skipped** (not attempted)
- Workflow marked "completed with errors" (not "failed" — data from completed steps valid)
- No workflow-level retry — individual steps can be re-triggered manually via orchestrator

**Parallel Step Execution:**
- Steps whose dependencies met CAN run in parallel
- Orchestrator launches all ready steps concurrently
- Example: after snapshot step, all 4 ingestion pipelines (bills, votes, members, amendments) launch simultaneously
- Parallelism limited by Cloud Run Job concurrency quota (configurable per GCP project)

**Event Payload Propagation:**
- Orchestrator does NOT pass event payloads between steps
- Each step reads input from AlloyDB or snapshot independently
- Pub/Sub events (`bill.text.available`, `vote.recorded`, etc.) for **event-driven path**, not orchestrated batch path
- Two paths coexist: orchestrator handles scheduled batch runs; Pub/Sub handles real-time triggered scoring

**Workflow Versioning:**
- In-flight runs continue on workflow version they started with
- New runs pick up latest version from GCS
- No hot-reload of workflow definitions during a run