# Acceptance Criteria: Component 11 — Scoring Engine

> Repository `repcheck-scoring-engine` containing three SBT projects: `score-cache` (publishable library for score reads/writes), `scoring-pipeline` (Cloud Run Job for user-member alignment scoring), and `stance-materializer` (Cloud Run Job for bill stance materialization and user-bill alignment).
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `repcheck-prompt-engine-users` (Component 9), `repcheck-llm-analysis` / `llm-adapter` (Component 10), `repcheck-llm-analysis` / `in-process-ml` (Component 10 §10.5/§10.8 — SemanticEmbeddingService for reasoning embeddings).

---

## System Context

### What This Component Does

Component 11 is the **alignment scoring engine** — it computes how well each legislator's voting record aligns with each user's political preferences. **Scoring is always scoped to the representative relationship: a user is only ever scored against their own representatives (their house member and their state's senators).** It uses a combination of scheduled jobs and DB polling to determine when data is ready for processing.

Six processes coordinate the scoring lifecycle:

```
Process A: Pairing Validator (§11.6) — scheduled + user location change + member.updated
    ↓ persists user_legislator_pairings

Process B: Ingestion Pipelines (unchanged, Components 4/6/7/10)
    ↓ writes bills, votes, amendments, findings
    ↓ updates stance_materialization_status (has_votes / has_analysis)

Process C: Stance Materializer (§11.9) — scheduled DB scanner
    ↓ polls stance_materialization_status for bills with both votes + analysis
    ↓ writes member_bill_stances + member_bill_stance_topics (per-topic with reasoning/embedding)

Process D: User-Bill Alignment (§11.10) — scheduled
    ↓ processes changed bills + changed users
    ↓ writes user_bill_alignments, user_amendment_alignments

Process E: User-Member Scoring (§11.7, §11.8) — scheduled + ad-hoc
    ↓ Phase 1: numeric scores (parallel) → persisted immediately
    ↓ Phase 2: LLM explanations (parallel) → persisted after scores

Process F: Score Refresh Notifier (§11.11)
    ↓ publishes scoring.user.completed for ad-hoc requests
```

### Three SBT Projects

| Project | Type | Purpose |
|---------|------|---------|
| `score-cache` | Library (publishable) | Repository for writing and reading pre-computed scores. Wraps all AlloyDB score table operations. No pipeline or Pub/Sub logic. |
| `scoring-pipeline` | Application (Cloud Run Job) | Orchestrates per-user scoring: reads pairings, aggregates stances, calculates alignment, generates LLM explanations, writes via score-cache. Scheduled + ad-hoc. |
| `stance-materializer` | Application (Cloud Run Job) | Scheduled job that materializes member bill stances (Process C) and pre-computes user-bill alignments (Process D). Polls DB for readiness. |

### The Scoring Flow

**Step 1 — User stance aggregation (§11.1)**

User stances are derived from Q&A responses using the pre-tagged question bank. The intelligence is in the question design, not the interpretation:

- **Multiple-choice answers** → algorithmic extraction (deterministic, no LLM):
  - Per-topic `stanceScore` = weighted average of contributions: `Σ(agreeDir × multiplier × weight) / Σ(weight)`
  - `agreeStance_direction = +1.0` if `agreeStance = "Progressive"`, `-1.0` if `agreeStance = "Conservative"`
  - Mathematically bounded to [-1.0, +1.0] — no clamping needed
  - Example: "Expand Medicare?" with `agreeStance = "Progressive"`, user selects "Agree" (`stanceMultiplier = 0.6`, `weight = 1.0`) → contribution of +0.6 toward healthcare stanceScore
- **Custom fill-in answers** → LLM interpretation via `preference-interpretation` profile (Component 9 §9.2), batched into a single call — stance adjustments only, no importance signal
- **No LLM for pure multiple-choice flows** — the common case requires zero LLM calls
- **Topic importance** — explicit user-provided weights from the "Prioritize your topics" screen completed after Q&A (stored in `user_topic_priorities`). Importance is NOT derived from answer frequency or `importanceSignal` values baked into questions.

Output: `UserTopicProfile` — list of `(topic, stanceScore, importance)` where `stanceScore` is -1.0 (strongly conservative) to +1.0 (strongly progressive), and `importance` is normalized 0.0-1.0 (sourced from explicit user topic prioritization).

**Step 2 — Legislator profile construction (§11.2)**

A query over pre-materialized `member_bill_stance_topics` — no LLM, no finding lookups:

For each topic the user cares about:
1. Fetch `member_bill_stance_topics` rows where the topic matches
2. Each row already contains: `stanceDirection`, `reasoning`, `reasoningEmbedding`, `findingId` (proof)
3. Contribution = `stanceDirection × voteWeight` (weight per vote type, per BEHAVIORAL_SPECS.md §2)
4. Average contributions across all votes for the topic → `stanceScore` per topic

The `member_bill_stances` table is a materialized cache — the stance materializer (§11.9) owns it and updates it when the DB scanner finds bills with both votes and completed analysis.

**Step 3 — Alignment score calculation + LLM explanation (§11.3, §11.4)**

- **Algorithmic base scoring (§11.3)**: Per-topic alignment = `1.0 - abs(userStance - legislatorStance) / 2.0`. Weighted average across topics using user importance. This is the **authoritative score** stored in `scores.aggregate_score`.
- **LLM explanation (§11.4)**: Sonnet/Haiku call via Component 9 prompt engine → `AlignmentScoreOutput`. The LLM's `overallScore` is NOT stored as the alignment score — it is for explanation context only. Reasoning is stored in both `scores.reasoning` and `score_history.reasoning`. Evidence is sourced from pre-materialized `member_bill_stance_topics` and `user_bill_alignments`.

### Score Storage (Two Tiers)

**Current scores** (overwritten on each re-score, optimized for fast frontend reads):

| Table | Key | Content |
|-------|-----|---------|
| `scores` | (user_id, member_id) | `aggregate_score`, `status`, `last_updated`, `llm_model`, `total_bills`, `total_votes`, `non_overlapping_topics`, `reasoning`, `reasoning_embedding` |
| `score_topics` | (user_id, member_id, topic) | `score` per topic |
| `score_congress` | (user_id, member_id, congress) | `overall_score`, `bills_considered`, `votes_analyzed` per congress |
| `score_congress_topics` | (user_id, member_id, congress, topic) | `score` per congress per topic |

**Score history** (append-only, for trend charts):

| Table | Key | Content |
|-------|-----|---------|
| `score_history` | score_id (UUID) | `aggregate_score`, `status`, `trigger_event` ("scheduled" / "ad-hoc"), `reasoning`, `reasoning_embedding` |
| `score_history_congress` | (score_id, congress) | `overall_score`, `bills_considered`, `votes_analyzed` |
| `score_history_congress_topics` | (score_id, congress, topic) | `score` |
| `score_history_highlights` | (score_id, bill_id, topic) | `stance`, `vote`, `alignment` |

### Persistence Mapping

| Table | Written By | When |
|-------|-----------|------|
| `user_legislator_pairings` | `PairingValidator` (§11.6) | At signup, scheduled validation, user location change |
| `stance_materialization_status` | Votes pipeline, Analysis pipeline | On vote position change, analysis completion |
| `member_bill_stances` + `member_bill_stance_topics` | `StanceMaterializer` (§11.9) | Scheduled scanner finds bills with both votes + analysis |
| `user_bill_alignments` + `user_amendment_alignments` | `UserBillAligner` (§11.10) | Scheduled, for changed bills and changed users |
| `scores` + `score_topics` + `score_congress` + `score_congress_topics` | `ScorePersister.upsertScore` (§11.5) | After each scoring run |
| `score_history` + sub-tables | `ScorePersister.appendHistory` (§11.5) | After each scoring run (append) |

### Event Contracts

| Event | Direction | Payload | Reference |
|-------|-----------|---------|-----------|
| `member.updated` | **Consumes** (Pairing Validator) | `{ memberId }` | Component 2 §2.1 |
| `user.profile.updated` | **Consumes** (Pairing Validator + User-Bill Alignment) | `{ userId, topicsChanged[] }` | Component 2 §2.1 |
| `scoring.user.requested` | **Consumes** (Scoring Pipeline) | `{ userId, requestId, source }` | Component 2 §2.1 |
| `scoring.user.completed` | **Emits** (Score Refresh Notifier) | `{ userId, requestId, memberScoreCount, status }` | Component 2 §2.1 |

> The scoring engine does NOT consume `analysis.completed` or `vote.recorded` events. Those events update `stance_materialization_status` in the DB (by their respective pipelines). The stance materializer (§11.9) polls the DB for readiness.

---

## Implementation Areas

| Area | Project | Status | Description |
|------|---------|--------|-------------|
| 11.1 User Stance Aggregation | `scoring-pipeline` | New | Extracts per-topic stance and importance from Q&A responses. Algorithmic for multiple-choice. LLM batch call for custom fill-in via Component 9. |
| 11.2 Legislator Profile Construction | `scoring-pipeline` | New | Queries pre-materialized `member_bill_stance_topics` → per-topic stance profile per legislator. |
| 11.3 Alignment Score Calculator | `scoring-pipeline` | New | Pure algorithmic per-topic alignment: linear distance on [-1.0, +1.0] stance scale, weighted by user importance. |
| 11.4 Score Explainer | `scoring-pipeline` | New | LLM explanation layer via Component 9 prompt engine + Component 10 llm-adapter. Evidence from pre-materialized tables. Returns `AlignmentScoreOutput`. Non-fatal on failure. |
| 11.5 Score Persistence | `score-cache` | New | AlloyDB score repository. Upserts current scores (with DELETE+INSERT for sub-tables), appends history. |
| 11.6 Pairing Validator | `scoring-pipeline` | New | Persists `user_legislator_pairings` at signup. Scheduled validation job. Handles user location change and `member.updated` events. |
| 11.7 Scoring Orchestrator | `scoring-pipeline` | New | Per-user orchestration: reads pairings → aggregates stances → scores all legislators (Phase 1: numeric, Phase 2: LLM explanation). |
| 11.8 Scoring Pipeline Entry Point | `scoring-pipeline` | New | Cloud Run Job entry point. Scheduled + ad-hoc modes. Subscribes to `scoring.user.requested` for ad-hoc. |
| 11.9 Stance Materializer | `stance-materializer` | New | Scheduled DB scanner. Polls `stance_materialization_status` for ready bills. Materializes per-topic stances with reasoning/embedding. |
| 11.10 User-Bill Alignment | `stance-materializer` | New | Scheduled job. Pre-computes per-bill, per-topic alignment between users and bills. Writes `user_bill_alignments`. |
| 11.11 Score Refresh Notifier | `scoring-pipeline` | New | Publishes `scoring.user.completed` event on ad-hoc completion. |
| 11.12 Amendment Scoring Integration | `stance-materializer` + `scoring-pipeline` | New | **Net-additions** for amendment-typed votes. **Bill-driven scan via denormalized `effective_bill_id` cache** — §7.3 resolves the sub-amendment → bill chain at ingest time (with end-of-run sweep retry for sub-amendments whose parents weren't yet hydrated). §11.12 then queries `findByEffectiveBillId(billId)` (simple WHERE, no recursion at scoring time) for transitive children. **Daily `AmendmentBackfillScanner`** finds amendments with `effective_bill_id IS NOT NULL`, votes, findings, no stances, and triggers `materializeBill(effective_bill_id)`. NO `amendment_materialization_status` table (per Q8). New `member_amendment_stances` / `member_amendment_stance_topics` / `user_amendment_alignments` tables. §11.2 profile construction unions amendment stances. New vote_weight_type values: `AMENDMENT_SUBSTANTIVE` (0.7) / `AMENDMENT_PROCEDURAL` (0.2). **Sub-amendments scored via the eagerly-resolved `effective_bill_id` cache** (per user direction this turn). Treaty + procedural amendments skipped with observability counters. Driven by Component 7 §7.4 + §7.6 + Component 10 §10.11. |

## Component Routing Table

| Task | Area File |
|------|-----------|
| User Q&A stance extraction (algorithmic + LLM for custom) | [11.1 User Stance Aggregation](11-scoring-engine/11.1-user-stance-aggregation.md) |
| Legislator vote-to-topic profile from pre-materialized data | [11.2 Legislator Profile Construction](11-scoring-engine/11.2-legislator-profile-construction.md) |
| Per-topic alignment formula, per-congress scores | [11.3 Alignment Score Calculator](11-scoring-engine/11.3-alignment-score-calculator.md) |
| LLM explanation, highlight selection, evidence fetching | [11.4 Score Explainer](11-scoring-engine/11.4-score-explainer.md) |
| Upsert current scores, append history, score-cache module | [11.5 Score Persistence](11-scoring-engine/11.5-score-persistence.md) |
| User-legislator pairing management | [11.6 Pairing Validator](11-scoring-engine/11.6-pairing-validator.md) |
| Per-user orchestration of all scoring steps (phased) | [11.7 Scoring Orchestrator](11-scoring-engine/11.7-scoring-orchestrator.md) |
| IOApp, scheduled + ad-hoc modes, config | [11.8 Scoring Pipeline Entry Point](11-scoring-engine/11.8-scoring-pipeline-entry-point.md) |
| Stance materialization from DB scanner | [11.9 Stance Materializer](11-scoring-engine/11.9-stance-materializer.md) |
| Pre-computed user-bill alignment | [11.10 User-Bill Alignment](11-scoring-engine/11.10-user-bill-alignment.md) |
| Ad-hoc scoring completion notification | [11.11 Score Refresh Notifier](11-scoring-engine/11.11-score-refresh-notifier.md) |
| Amendment-typed vote scoring: stance materialization, profile union, weight rules, user-amendment alignment | [11.12 Amendment Scoring Integration](11-scoring-engine/11.12-amendment-scoring-integration.md) |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck-scoring-engine/
├── score-cache/
│   └── repcheck.scoring.cache
│       ├── repository
│       │   ├── ScorePersister             (11.5)
│       │   └── ScoreReader                (11.5)
│       ├── config
│       │   └── ScoreCacheConfig           (11.5)
│       └── errors
│           └── ScorePersistenceFailed     (11.5)
│
├── scoring-pipeline/
│   └── repcheck.scoring.pipeline
│       ├── aggregation
│       │   ├── UserStanceAggregator       (11.1)
│       │   ├── UserTopicProfile           (11.1)
│       │   └── TopicStanceEntry           (11.1)
│       ├── profile
│       │   ├── LegislatorProfileBuilder   (11.2)
│       │   ├── LegislatorTopicProfile     (11.2)
│       │   ├── LegislatorTopicEntry       (11.2)
│       │   └── VoteWeightConfig           (11.2)
│       ├── scoring
│       │   ├── AlignmentScoreCalculator   (11.3)
│       │   ├── PerTopicAlignment          (11.3)
│       │   ├── AlignmentCalculationResult (11.3)
│       │   ├── ScoreExplainer             (11.4)
│       │   ├── AlignmentEvidenceFetcher   (11.4)
│       │   └── ScoringContext             (11.4)
│       ├── pairing
│       │   └── PairingValidator           (11.6)
│       ├── orchestration
│       │   ├── ScoringOrchestrator        (11.7)
│       │   └── ScoringResult              (11.7)
│       ├── notification
│       │   └── ScoreRefreshNotifier       (11.11)
│       ├── config
│       │   └── ScoringPipelineConfig      (11.8)
│       ├── app
│       │   └── ScoringPipelineApp         (11.8)
│       └── errors
│           ├── ScoringFailed              (11.7)
│           ├── StanceExtractionFailed     (11.1)
│           ├── ProfileBuildFailed         (11.2)
│           └── ExplanationFailed          (11.4)
│
└── stance-materializer/
    └── repcheck.scoring.materializer
        ├── scanner
        │   └── StanceMaterializationScanner (11.9)
        ├── materializer
        │   └── StanceMaterializer         (11.9)
        ├── alignment
        │   └── UserBillAligner            (11.10)
        ├── config
        │   └── StanceMaterializerConfig   (11.9)
        ├── app
        │   └── StanceMaterializerApp      (11.9)
        └── errors
            ├── MaterializationFailed      (11.9)
            └── AlignmentFailed            (11.10)
```

### Dependencies

```
repcheck-scoring-engine/
├── score-cache (publishable library)
│   ├── repcheck-shared-models            (Component 1 — score DOs §1.5)
│   ├── repcheck-pipeline-models          (Component 2 — Tables constants §2.10)
│   └── Doobie                            (AlloyDB persistence)
│
├── scoring-pipeline (Cloud Run Job application)
│   ├── score-cache                       (internal SBT dependency)
│   ├── repcheck-shared-models            (Component 1 — DOs §1.5, output schemas §1.6)
│   ├── repcheck-pipeline-models          (Component 2 — events §2.1, pipeline tracking)
│   ├── repcheck-prompt-engine-users      (Component 9 — UserPromptAssembler, scoring profiles)
│   ├── repcheck-llm-analysis (llm-adapter) (Component 10 — LlmProvider[F], LlmRetryWrapper)
│   ├── repcheck-llm-analysis (in-process-ml) (Component 10 §10.5/§10.8 — SemanticEmbeddingService for reasoning embeddings)
│   ├── Doobie                            (AlloyDB reads)
│   ├── Google Cloud Pub/Sub SDK          (event subscription — scoring.user.requested)
│   └── http4s Ember                      (LLM API calls via llm-adapter)
│
└── stance-materializer (Cloud Run Job application)
    ├── repcheck-shared-models            (Component 1 — DOs §1.5, §1.9)
    ├── repcheck-pipeline-models          (Component 2 — Tables constants, pipeline tracking)
    ├── repcheck-llm-analysis (llm-adapter) (Component 10 — LlmProvider[F] for stance reasoning)
    ├── repcheck-llm-analysis (in-process-ml) (Component 10 §10.5/§10.8 — SemanticEmbeddingService for embeddings)
    ├── repcheck-prompt-engine-users      (Component 9 — stance reasoning profiles)
    ├── Doobie                            (AlloyDB reads + writes)
    └── http4s Ember                      (LLM API calls)
```

### Cross-Component Updates Required

| Component | Update Needed | Reason |
|-----------|--------------|--------|
| **Component 1 §1.5** | Add new DOs: `UserLegislatorPairingDO`, `MemberBillStanceTopicDO`, `UserBillAlignmentDO`, `UserAmendmentAlignmentDO`, `StanceMaterializationStatusDO` | New tables for batch scoring architecture |
| **Component 1 §1.5** | Update `ScoreDO` with `status`, `nonOverlappingTopics`, `reasoning`, `reasoningEmbedding` | Score status and explanation fields |
| **Component 1 §1.5** | Update `ScoreHistoryDO` with `status`, `reasoningEmbedding` | History audit trail enhancements |
| **Component 2 §2.1** | Add `ScoringUserRequestedEvent`, `ScoringUserCompletedEvent`; update consumers for `VoteRecordedEvent`, `AnalysisCompletedEvent`, `UserProfileUpdatedEvent`, `MemberUpdatedEvent` | Batch scoring replaces event-driven scoring |
| **Component 2 §2.10** | Add `Tables` constants for new tables: `user_legislator_pairings`, `member_bill_stance_topics`, `user_bill_alignments`, `user_amendment_alignments`, `stance_materialization_status` | New tables need constants |
| **BEHAVIORAL_SPECS.md §3** | Rewrite "Incremental Scoring" → "Batch Scoring Architecture" | Fundamental architecture change |
| **Votes pipeline §6.5** | Add `stance_materialization_status` upsert after vote position persistence | Votes pipeline updates status flags for DB scanner |
| **Analysis pipeline §10.10** | Add `stance_materialization_status` upsert after analysis completion | Analysis pipeline updates status flags for DB scanner |

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | `UserStanceAggregator` MC extraction formula, importance normalization | MockitoScala (mock DB) |
| Unit tests | `AlignmentScoreCalculator` formula, edge cases (no shared topics, boundary stances) | No infrastructure (pure class) |
| Unit tests | `LegislatorProfileBuilder` stance profile from pre-materialized data | MockitoScala |
| Unit tests | `PairingValidator` pairing creation and validation logic | MockitoScala |
| Unit tests | `ScoringOrchestrator` step coordination, skip logic, phased scoring | MockitoScala |
| Unit tests | `StanceMaterializationScanner` query logic | MockitoScala |
| WireMock tests | LLM API calls from `ScoreExplainer` (request structure, profile selection) | WireMock |
| WireMock tests | LLM API calls from `StanceMaterializer` (stance reasoning generation) | WireMock |
| Integration tests | `ScorePersister` upsert + history append round-trip | AlloyDB Omni (Docker) |
| Integration tests | `LegislatorProfileBuilder` query against seeded `member_bill_stance_topics` | AlloyDB Omni (Docker) |
| Integration tests | `UserStanceAggregator` with Q&A data from AlloyDB | AlloyDB Omni (Docker) |
| Integration tests | `StanceMaterializer` full materialization for seeded bill | AlloyDB Omni (Docker) |
| Integration tests | `UserBillAligner` alignment computation for seeded data | AlloyDB Omni (Docker) |
| Integration tests | `PairingValidator` pairing creation and validation against seeded terms | AlloyDB Omni (Docker) |
| E2E tests | Full scoring run from `scoring.user.requested` to scores written | AlloyDB Omni (Docker) + WireMock (mock LLM) |
| E2E tests | Stance materialization scanner → materialize → scoring | AlloyDB Omni (Docker) + WireMock |
| E2E tests | Ad-hoc scoring request → completion event published | AlloyDB Omni (Docker) + WireMock + mock Pub/Sub |
