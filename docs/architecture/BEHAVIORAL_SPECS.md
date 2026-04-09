# RepCheck Behavioral Specifications

> Supplement to [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md). This document provides explicit, unambiguous rules for pipeline behavior, entity linking, scoring logic, event emission, and workflow execution. Agents implementing the scoring engine, orchestrator, or any pipeline should reference this document alongside the system design.

---

## 1. Change Detection & Persistence Strategy

Per-entity rules for how pipelines detect and handle changes from Congress.gov:

| Entity | Natural Key | Change Detection | Persistence Strategy | Event Trigger |
|--------|------------|-----------------|---------------------|---------------|
| Bill | `{congress}-{billType}-{number}` | `updateDate` from API > stored `updateDate` | Upsert (overwrite) in AlloyDB | Emit `bill.text.available` whenever `updateDate` changed AND `textUrl` is non-null |
| Vote | `{congress}-{chamber}-{rollNumber}` | `updateDate` from API > stored `updateDate`, or vote doesn't exist yet | Upsert with history: archive prior version to `vote_history` table, then overwrite main row. Updates `stance_materialization_status.has_votes` in DB (§3.2) | Emit `vote.recorded` on first insert AND on updates where vote positions changed |
| Member | `{bioguideId}` | Any field differs from stored document | Upsert (overwrite) in AlloyDB | No event (no downstream consumers) |
| Amendment | `{congress}-{amendmentType}-{number}` | `updateDate` from API > stored `updateDate` | Upsert (overwrite) in AlloyDB | No event (consumed on-demand by analysis) |
| Analysis | `{billId}-{passNumber}-{version}` | N/A (always creates new version) | Insert new version (append-only, never overwrite). Updates `stance_materialization_status.has_analysis` and `all_passes_completed` in DB (§3.2) | Emit `analysis.completed` after final pass |

### Key Rules

- **No field-by-field diffing** — compare `updateDate` timestamps only (for bills/amendments) or existence check + position comparison (for votes)
- **Votes are diffed and upserted** — prior version is saved to `vote_history` table before the main row is overwritten. Scoring always uses the latest version only; history is for audit purposes, not scored independently
- **Analysis is append-only** — re-analyzing the same bill creates a new analysis version, preserving the full audit trail
- **`bill.text.available` emits on every qualifying re-ingest** — this allows re-analysis when bill text is updated or amended. LLM cost is managed by the tiered pass routing (Haiku for all bills, Sonnet/Opus only for qualifying bills)
- **Scoring is decoupled from data-change events** — `vote.recorded` and `analysis.completed` events update DB status flags in `stance_materialization_status`. Stance materialization readiness is determined by a scheduled DB scanner (§3.2), not by consuming these events directly. This eliminates dual-event coordination complexity.

---

## 2. Join Keys & Entity Linking

### Foreign Key Relationships

```
BILL.billId       ← VOTE.billId              (many votes per bill)
BILL.billId       ← AMENDMENT.billId          (many amendments per bill)
BILL.billId       ← ANALYSIS.billId           (many analysis versions per bill)
VOTE.voteId       ← VOTE_POSITION.voteId      (many positions per vote)
MEMBER.memberId   ← VOTE_POSITION.memberId    (many positions per member)
USER.userId       ← PREFERENCE.userId          (many preferences per user)
USER.userId + MEMBER.memberId → SCORE          (composite key)
```

### Congress Scoping Rules

- Bills, votes, and amendments are scoped to a specific congress number
- Members span multiple congresses (same `bioguideId` across terms served)
- **Scoring is perpetual** — a legislator's votes from ALL congresses contribute to their aggregate score
- **Two score tiers are maintained:**
  - **Aggregate score**: across all congresses the legislator has served (lifetime alignment)
  - **Per-congress score**: scoped to a single congress (e.g., "How did this legislator align with you in the 118th Congress?")
- The `congress` field on bills/votes is used to partition per-congress scores, but aggregate scores span all congresses

### Vote-to-Bill Linkage

- Congress.gov API includes a `bill` object in each vote response containing `billId`, `congress`, `type`, and `number`
- Stored as `VOTE.billId` foreign key joining to `BILL.billId`
- Procedural votes (no bill attached) have a null `billId` — these are stored in AlloyDB but excluded from alignment scoring
- A single bill can have multiple votes at different stages (committee vote, cloture, floor passage, conference report)

### Vote Significance & User-Facing Context

Each vote on a bill has a distinct legislative meaning. The platform communicates this so users understand what a legislator's vote represents in the lawmaking process.

| Vote Type | `question` Field Pattern | Legislative Meaning | User-Facing Explanation |
|-----------|------------------------|---------------------|------------------------|
| Committee vote | "Reported favorably" / "Ordered to be reported" | Advances bill out of committee to full chamber | "Voted to advance this bill from committee for consideration by the full [House/Senate]" |
| Cloture (Senate) | "On Cloture" / "On the Cloture Motion" | Ends debate / breaks filibuster (requires 60 votes) | "Voted to end debate on this bill, allowing it to proceed to a final vote" |
| Floor passage | "On Passage" / "On Motion to Suspend the Rules and Pass" | Approves bill in one chamber | "Voted to pass this bill in the [House/Senate]" |
| Amendment vote | "On Agreeing to the Amendment" | Modifies bill text before passage | "Voted on a proposed change to the bill's text" |
| Conference report | "On Agreeing to the Conference Report" | Approves final reconciled version after both chambers agree | "Voted to approve the final version of this bill after both chambers agreed on the text" |
| Motion to recommit | "On Motion to Recommit" | Sends bill back to committee (often to block passage) | "Voted to send this bill back to committee" |
| Veto override | "On Overriding the Veto" | Overrides presidential veto (requires 2/3 majority) | "Voted to override the President's veto of this bill" |

### Scoring Weight by Vote Type

Not all votes carry equal weight for alignment scoring. Floor passage and conference report votes have the most direct impact on whether a bill becomes law.

Vote weight is configurable in scoring config:

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

- The scoring LLM receives vote type and weight as context — weights guide prompt emphasis, not hard mathematical multipliers
- Unknown question patterns default to weight `0.5` and are flagged for manual review

### Vote Type Detection

- Determined from the `question` field in the Congress.gov vote response
- Pattern matching against known question text patterns (see table above)
- Stored as `VOTE.voteType` enum: `Passage | ConferenceReport | Cloture | VetoOverride | Amendment | Committee | Recommit | Other`

### Bill Text Decomposition

Bill text can be extremely large (omnibus bills, infrastructure acts — thousands of pages). Raw text cannot fit within a single LLM context window. Before analysis passes run, large bills undergo decomposition.

**Important:** Not all bill text arrives as XML. Congress.gov publishes bills in multiple formats — Formatted XML, Formatted Text, and PDF (which is essentially text once extracted). The decomposition pipeline must handle all formats.

#### Decomposition Steps

1. **Text parsing / section identification** (Ollama sidecar) — an Ollama instance running as a Cloud Run sidecar reads the bill text (regardless of format — plain text, PDF-extracted text, or XML) and identifies logical sections: section boundaries, headings, numbering, and content. For XML-formatted bills, structural elements (`<section>`, `<title>`, `<part>`, `<division>`) provide reliable hints; for plain text, the model uses legislative formatting conventions (section numbering patterns, heading styles, indentation). Each identified section becomes a `BillTextSectionDO` row. Cost: minimal (Ollama runs locally in the sidecar, no external API calls).

2. **In-process section embedding** (DJL + ONNX Runtime, no API calls) — embed each section's text using a sentence-transformer model (e.g., `all-MiniLM-L6-v2`, ~80MB) loaded directly into the JVM via DJL (Deep Java Library) with ONNX Runtime backend. This produces a 384-dimensional dense vector per section for clustering. No external API call — the model runs in-process within the Cloud Run Job container. Typical throughput: hundreds of sections per second on CPU.

3. **Semantic clustering** (Smile ML library, no AI) — cluster the section embedding vectors using k-means or DBSCAN (from the Smile JVM ML library) to produce concept groups. Sections about "transportation funding" end up in the same group regardless of where they appear in the bill. The number of clusters is determined dynamically based on section count and inter-section similarity (e.g., target 5-20 groups for a large bill, minimum 2 sections per group). This step is deterministic and free.

4. **LLM-assisted simplification** (Haiku API, only step with external API cost) — for each concept group, call the LLM to produce a coherent summary using decomposition prompts from `repcheck-prompt-engine-bills` (Component 8, `concept-simplification` profile). Cost: ~$0.001 per group, 10-20 groups per large bill = $0.01-0.02.

5. **Result** — decomposition artifacts are **persisted** to AlloyDB:
   - `bill_text_sections` — one row per section, with content and ordinal index
   - `bill_concept_groups` — one row per concept group, with simplified text
   - `bill_concept_group_sections` — junction linking sections to groups
   - These are tied to the `bill_text_versions.version_id`, not to an analysis run. They are immutable once produced and reusable across re-analyses.
   - 1536-dim embeddings for semantic search are generated separately (step 2 produces ephemeral 384-dim vectors for clustering only).

#### Ollama Sidecar Architecture

The Ollama sidecar runs as a second container within the same Cloud Run Job:

- **Model**: A small, fast model suitable for text parsing (e.g., Llama 3.2 1B or similar). The model only needs to identify section boundaries and extract headings — not perform deep analysis.
- **Communication**: HTTP API on localhost (`http://localhost:11434`). The JVM bill-analysis-pipeline calls the Ollama API using http4s.
- **Lifecycle**: Starts with the Cloud Run Job, shares the same network namespace. No external ingress needed.
- **Cost**: Runs on the Cloud Run Job's allocated CPU/memory. No per-token API charges.
- **Why not in-process?** Unlike DJL embedding (which loads a small ONNX model), text parsing with section identification requires an instruction-following LLM. Ollama provides this as a lightweight sidecar without external API costs.

**Why embedding-based clustering instead of LLM classification for grouping?**
- An LLM-based `section-classification` approach would require one Ollama or API call per section (~200 calls for a large bill). Embedding-based clustering achieves the same grouping at negligible cost by computing semantic similarity in-process.
- The `section-classification` profile in Component 8 remains available as a fallback or for targeted re-classification if cluster quality is poor.

**Key rules:**
- Short bills that fit within the context window skip decomposition entirely (raw text is used directly, with a single concept group covering the whole bill)
- The bill-analysis-pipeline (Component 10) owns all decomposition orchestration (when, how to parse, how to embed, how to group). Component 8 provides only the prompts for the LLM-assisted simplification step.
- Decomposition results (sections, concept groups) are **persisted** to AlloyDB as bill text layer artifacts, tied to the text version. They are reusable across re-analyses.
- The DJL embedding model (`all-MiniLM-L6-v2` or equivalent) is bundled in the container image (~80MB). It produces 384-dimensional vectors for clustering only — these are NOT the 1536-dimensional pgvector embeddings stored in AlloyDB for semantic search.
- The Smile clustering library runs in-process on the JVM. No external dependency beyond the Maven artifact.
- The Ollama sidecar model is pulled at container build time and cached in the image.

### Embedding Generation for Semantic Search

Bill text and analysis outputs contain multiple text fields that are vectorized (pgvector, 1536 dimensions) for semantic search. There are two distinct embedding layers:

**Bill text layer** (generated during decomposition, tied to text version):

| Source | Text Field | Target Table / Column | DO | Enables |
|--------|-----------|----------------------|-----|---------|
| Parsed sections | `BillTextSectionDO.content` | `bill_text_sections.embedding` | `BillTextSectionDO` (§1.4) | "Find sections across all bills about broadband funding" |
| Concept group summaries | `BillConceptGroupDO.simplifiedText` | `bill_concept_groups.embedding` | `BillConceptGroupDO` (§1.9) | "Find concept groups similar to this across all bills" |
| Full text version | `BillTextVersionDO.content` | `bill_text_versions.embedding` | `BillTextVersionDO` (§1.4) | "Find bills with similar full text" |

**Analysis layer** (generated after each analysis pass, tied to analysis run):

| Source | Text Field | Target Table / Column | DO (§1.9) | Enables |
|--------|-----------|----------------------|-----------|---------|
| Pass 1 overall summary | `BillSummaryOutput.summary` | `bill_analyses.embedding` | `BillAnalysisDO` | "Find bills similar to this one" |
| Pass 1 per-concept summaries | `ConceptSummaryResult.summary` | `bill_concept_summaries.embedding` | `BillConceptSummaryDO` | "Find analysis results for similar concepts" |
| Pass 1 topic classifications | `TopicScore.topic` values | `bill_subjects.embedding` | `BillSubjectDO` (§1.2) | Semantic topic similarity (beyond exact matching) |
| Pass 2 pork findings | `PorkFinding.description` | `bill_findings.embedding` | `BillFindingDO` | "Find similar earmark/rider patterns" |
| Pass 2 impact analysis | `ImpactItem.description` | `bill_findings.embedding` | `BillFindingDO` | "Find bills with similar impact on this group" |
| Amendment descriptions | `AmendmentSummary.description` | `amendment_findings.embedding` | `AmendmentFindingDO` | Amendment semantic search |

**Key rules:**
- Embedding model is configured per deployment (e.g., OpenAI `text-embedding-3-small`, 1536 dimensions)
- Embeddings are generated after each pass completes, before the pipeline moves to the next pass
- HNSW indexes with cosine distance are already defined in the schema for all embedding columns
- Embedding generation failures are non-fatal — the analysis results are still persisted, but the bill won't appear in semantic search results until embeddings are regenerated

### AlloyDB Schema — Bill Text & Analysis (Two Table Layers)

Bill text decomposition and LLM analysis results are stored in **separate table layers** — both normalized (no JSONB):

**Bill text layer** (tied to text version, immutable once produced, reusable across re-analyses):

- **`bill_text_sections`** — One row per structural section parsed from a bill's text. Stores section content, identifier (e.g., "Sec. 101"), heading, and ordinal index. Each section has its own `embedding` for per-section semantic search. Linked to `bill_text_versions.version_id`.
- **`bill_concept_groups`** — One row per concept group from decomposition. Stores the group's `simplified_text` (Haiku-produced summary), `title`, and `embedding`. Linked to `bill_text_versions.version_id`. Multiple analysis runs can reference the same concept groups.
- **`bill_concept_group_sections`** — Junction table linking which sections belong to which concept group. A section belongs to exactly one group.

**Analysis layer** (tied to analysis run, one set of rows per analysis):

- **`bill_analyses`** — One row per analysis run. Keyed by `analysis_id` (UUID). Stores the bill-wide Pass 1 overall summary (`summary`, `reading_level`, `key_points`), flat topic tags (`topics TEXT[]` for event payloads), per-pass model names (`pass1_model`, `pass2_model`, `pass3_model`), `pass_completed` (highest pass reached: 1, 2, or 3), and `embedding` (pgvector, generated from the overall summary).
- **`bill_concept_summaries`** — One row per concept group per analysis. References the concept group via `concept_group_id` FK to `bill_concept_groups`. Stores per-concept Pass 1 analysis results (`summary`, `reading_level`, `key_points`, `topics`). Each row has its own `embedding` for per-concept semantic search. Linked to `bill_analyses.analysis_id`.
- **`bill_analysis_topics`** — One row per topic per analysis. Stores `TopicClassificationOutput` entries with `confidence` scores. `concept_group_id` is `NULL` for bill-wide topics, set for per-concept topics.
- **`bill_findings`** — One row per finding (pork, impact, stance, etc.). Discriminated by `finding_type_id` FK. Columns: `concept_group_id` (nullable), `severity`, `confidence`, `affected_section`, `affected_group` — all nullable, used by different finding types. Each finding has its own `embedding`.
- **`bill_fiscal_estimates`** — One row per analysis run. Stores `FiscalEstimateOutput` fields: `estimated_cost`, `timeframe`, `confidence`, `assumptions TEXT[]`.
- **`amendment_findings`** — Same pattern as `bill_findings` for amendments. Columns: `severity`, `confidence`, `affected_section`.

**Key rules:**
- `analysisId` = auto-generated UUID
- Latest analysis = query `WHERE bill_id = ? ORDER BY analyzed_at DESC LIMIT 1`
- All rows for one analysis run share the same `analysis_id` across `bill_analyses`, `bill_concept_summaries`, `bill_analysis_topics`, `bill_findings`, and `bill_fiscal_estimates`
- Concept groups and sections are shared across analysis runs — they belong to the text version
- Structured output types (`Pass1Output`, `Pass2Output`) are not stored as JSONB — they are decomposed into the normalized tables above by Component 10 after each pass completes
- Finding types: `finding_types` lookup table with seeded values: `topic_extraction`, `bill_summary`, `policy_analysis`, `stance_detection`, `impact_analysis`, `fiscal_estimate`, `pork`, `rider`, `lobbying`, `constitutional`

### Vote Position Query Pattern

- **Fetch all positions for a vote**: query `vote_positions` table `WHERE vote_id = ?`
- **Fetch a specific member's position on a vote**: query `vote_positions WHERE vote_id = ? AND member_id = ?`
- **Fetch a member's votes on a bill**: query `votes WHERE bill_id = ?`, then join with `vote_positions ON vote_id AND member_id = ?`
- Secondary index on `(bill_id)` in `votes` and `(vote_id, member_id)` PK in `vote_positions`

---

## 3. Batch Scoring Architecture

Scoring is decoupled from data-change events. Instead of reacting to individual `vote.recorded` or `analysis.completed` events, the system uses a combination of scheduled jobs and DB polling to determine when data is ready for processing. Six processes coordinate the scoring lifecycle:

| Process | Trigger | Input | Output |
|---------|---------|-------|--------|
| A. Pairing Validator (§11.6) | Scheduled + user location change + `member.updated` | `users`, `members`, `member_terms` | `user_legislator_pairings` |
| B. Ingestion Pipelines | Event-driven (unchanged, Components 4/6/7/10) | Congress.gov API | bills, votes, amendments, findings + `stance_materialization_status` |
| C. Stance Materializer (§11.9) | Scheduled scanner (polls DB) | `vote_positions`, `bill_findings`, `stance_materialization_status` | `member_bill_stances` + `member_bill_stance_topics` |
| D. User-Bill Alignment (§11.10) | Scheduled | `users`, `bill_findings`, `member_bill_stance_topics` | `user_bill_alignments`, `user_amendment_alignments` |
| E. User-Member Scoring (§11.7, §11.8) | Scheduled + ad-hoc | `user_legislator_pairings`, `member_bill_stance_topics`, `user_bill_alignments` | `scores`, `score_topics`, `score_history` |
| F. Score Refresh Notifier (§11.11) | On ad-hoc completion | scoring results | `scoring.user.completed` event |

### 3.1 User-Legislator Pairings

Pairings are persisted at user signup and validated by a scheduled job:

- **At signup:** Look up user's state/district → query `member_terms` for current legislators (`end_year IS NULL OR end_year >= current year`) → insert `user_legislator_pairings` rows
- **Scheduled validation:** Scan all users, check each pairing against current `member_terms`, add new legislators, remove stale ones
- **On user location change:** `validateForUser(userId)` re-computes pairings for the user
- **On `member.updated`:** Pairing Validator checks if the member's terms have changed and updates affected pairings

### 3.2 Stance Materialization

The stance materializer does NOT subscribe to Pub/Sub events. Instead, existing pipelines record status in the DB when votes arrive and when analysis completes. A scheduled scanner job polls the DB for bills where both conditions are met:

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
1. Fetch `vote_positions` for the bill
2. Fetch analysis topics + stance findings from `bill_findings`
3. For each (member, vote, topic): compute stance direction, generate per-topic reasoning (LLM), generate reasoning embedding (DJL/ONNX), link to finding
4. Upsert `member_bill_stances` parent row
5. Delete + insert `member_bill_stance_topics` children (per-topic stance with reasoning, embedding, finding FK)
6. Update `stances_materialized_at = NOW()`
7. Update `users.last_stance_change_at` for all paired users of affected members

**Key rules:**
- This eliminates dual-event coordination complexity — no need to wait for both `vote.recorded` and `analysis.completed` events to converge
- The scanner runs on a schedule (e.g., every 15 minutes) and processes all qualifying bills in one batch
- Materialization is idempotent — re-running for the same bill with the same data produces the same result

### 3.3 User-Bill Alignment

A scheduled job pre-computes per-bill, per-topic alignment between each user's stances and the bill's stance findings:

- **Changed-bill optimization:** Only process bills where `stances_materialized_at` has been updated since the last alignment run
- **Changed-user optimization:** Only process users whose stances have changed (Q&A responses updated)
- For each (user, bill) pair with overlapping topics: compute alignment score, generate reasoning (LLM), generate reasoning embedding (DJL/ONNX), link to finding
- Upsert results to `user_bill_alignments` / `user_amendment_alignments`

### 3.4 User-Member Scoring

Scoring runs on a schedule and can also be triggered ad-hoc per user:

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

This two-phase approach ensures numeric scores are visible immediately. LLM explanation generation (slower, more expensive) does not block score visibility.

### 3.5 Ad-Hoc Scoring

The request/reply pattern for on-demand user scoring:

1. API Server (or scheduler) publishes `ScoringUserRequestedEvent(userId, requestId, source)` to Pub/Sub
2. Scoring Pipeline subscribes, processes the user via `scoreUser`
3. On completion, Score Refresh Notifier publishes `ScoringUserCompletedEvent(userId, requestId, memberScoreCount, status)` to a return queue
4. API Server reads the return queue and notifies the user

### 3.6 Skip-Unchanged Optimization

To avoid re-scoring users whose data hasn't changed:

- `users.last_stance_change_at` — updated by stance materializer (§3.2 step 7) when any paired legislator's stances change, and by Q&A response submission when user preferences change
- `stance_materialization_status.stances_materialized_at` — updated per bill when stances are materialized
- Scoring pipeline checks: for each user, have any of their paired legislators had stance changes since the user's last scoring run? If not, skip.

### Score Structure (Two Tiers + History)

Score tables are fully normalized (see Component 1 §1.5 for DOs):

- **`scores`** — Current score per (userId, memberId) pair. Overwritten on each re-score. Contains `aggregate_score`, `status`, `reasoning`, `reasoning_embedding`.
- **`score_topics`** — Per-topic scores for the current scoring run.
- **`score_congress`** — Per-congress aggregate scores.
- **`score_congress_topics`** — Per-congress per-topic scores.
- **`score_history`** — Append-only audit trail. Contains `trigger_event` (`"scheduled"` or `"ad-hoc"`), `reasoning`, `reasoning_embedding`.
- **`score_history_congress`**, **`score_history_congress_topics`**, **`score_history_highlights`** — History detail tables.

### Scoring Computation Rules

- Each scoring run computes BOTH aggregate and per-congress scores
- **Aggregate** = weighted combination of all per-congress scores (more recent congresses weighted higher)
- **Per-congress** = based only on votes and analyses from that specific congress
- Latest scores overwrite the `scores` table for fast frontend reads
- Each scoring run also appends to `score_history` (enables "alignment over time" trend charts)
- Scoring always uses the latest version of each bill and vote (historical versions are not scored)
- History records what the score WAS at each point in time — it is not re-scored independently

---

## 4. Event Emission — Precise Conditions

### Event Catalog

| Event | Emitted By | Condition | Payload |
|-------|-----------|-----------|---------|
| `bill.text.available` | bills-pipeline | `textUrl` is non-null AND `updateDate` from API is newer than stored `updateDate` | `{ billId, congress, textUrl, textFormat }` |
| `vote.recorded` | votes-pipeline | Vote is new (first insert) OR vote positions changed on upsert (prior version archived to history). Also updates `stance_materialization_status.has_votes` in DB. | `{ voteId, billId, chamber, date, congress, isUpdate: boolean }` |
| `analysis.completed` | bill-analysis-pipeline | Final pass completes. Also updates `stance_materialization_status.has_analysis` and `all_passes_completed` in DB. | `{ billId, analysisId, topics[], passesExecuted, modelUsed }` |
| `user.profile.updated` | api-server (future) | User submits new Q&A responses that differ from stored responses (no-op if identical resubmission) | `{ userId, topicsChanged[] }` |
| `scoring.user.requested` | scoring scheduler / API server | Scheduled run enqueues per-user event, or user requests ad-hoc re-score | `{ userId, requestId, source }` |
| `scoring.user.completed` | scoring pipeline (§11.11) | Ad-hoc scoring run completes for a user | `{ userId, requestId, memberScoreCount, status }` |

### Event Ordering & Idempotency

- **Pub/Sub does NOT guarantee ordering** — all pipelines must be idempotent
- `vote.recorded` and `analysis.completed` events update DB status flags (`stance_materialization_status`). Stance materialization readiness is determined by the DB scanner (§3.2), not by consuming these events directly. This eliminates the race condition where `vote.recorded` arrives before analysis exists — the scanner simply waits until both flags are set.
- Duplicate events are safe — all pipelines use upsert semantics. The next scheduled run self-corrects any temporary inconsistency.
- `scoring.user.requested` events are processed idempotently — re-scoring the same user produces the same output if data hasn't changed.

### Dead-Letter Policy

- Messages that fail after max retries (3 for processing failures) are moved to a dead-letter topic
- Dead-lettered messages trigger a GCP Monitoring alert (configured per topic)
- Recovery: manual replay from the dead-letter topic after the root cause is fixed
- No automatic retry from dead-letter — requires human intervention
- No requeue-on-missing-dependency logic needed — stance materialization handles data convergence via DB polling (§3.2)

---

## 5. Workflow Execution Rules

### Snapshot Semantics

- Each workflow run creates a fresh snapshot (point-in-time copy of relevant AlloyDB data to GCS)
- Snapshots are NOT shared across concurrent workflow runs
- Downstream steps read from their run's snapshot, not live AlloyDB
- Snapshot path is passed via environment variable `SNAPSHOT_PATH` to each Cloud Run Job
- Exception: pipeline run status writes go directly to AlloyDB (not through the snapshot)

### Step Completion Criteria

- A step is **complete** when its Cloud Run Job exits with code 0
- Partial failures (some items failed, some succeeded) = step **succeeds** if exit code is 0
- Failed items are logged in `ProcessingResult`; the step itself is not retried for individual item failures
- A step is **failed** only if the Cloud Run Job exits with a non-zero code (systemic failure)

### Error Escalation

- If step N fails, all steps that depend on N are **skipped** (not attempted)
- The workflow is marked as "completed with errors" (not "failed" — data from completed steps is valid)
- No workflow-level retry — individual steps can be re-triggered manually via the orchestrator

### Parallel Step Execution

- Steps whose dependencies are all met CAN run in parallel
- The orchestrator launches all ready steps concurrently
- Example: after the snapshot step completes, all 4 ingestion pipelines (bills, votes, members, amendments) launch simultaneously
- Parallelism is limited by the Cloud Run Job concurrency quota (configurable per GCP project)

### Event Payload Propagation

- The orchestrator does NOT pass event payloads between steps
- Each step reads its input from AlloyDB or the snapshot independently
- Pub/Sub events (`bill.text.available`, `vote.recorded`, etc.) are for the **event-driven path**, not the orchestrated batch path
- The two paths coexist: the orchestrator handles scheduled batch runs; Pub/Sub handles real-time triggered scoring

### Workflow Versioning

- In-flight runs continue on the workflow version they started with
- New runs pick up the latest version from GCS
- No hot-reload of workflow definitions during a run
