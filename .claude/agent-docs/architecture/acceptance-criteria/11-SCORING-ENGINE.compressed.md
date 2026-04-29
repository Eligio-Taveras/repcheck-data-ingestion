<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/11-SCORING-ENGINE.md -->

# Component 11 — Scoring Engine

## System Context

Component 11 computes how well each legislator's voting record aligns with each user's political preferences. **Scoring is scoped to representative relationship: users are only scored against their own representatives (house member + state senators).**

Six processes coordinate the scoring lifecycle:
- **Process A**: Pairing Validator (§11.6) — persists `user_legislator_pairings`
- **Process B**: Ingestion Pipelines (Components 4/6/7/10) — updates `stance_materialization_status`
- **Process C**: Stance Materializer (§11.9) — materializes `member_bill_stances` + `member_bill_stance_topics`
- **Process D**: User-Bill Alignment (§11.10) — writes `user_bill_alignments`, `user_amendment_alignments`
- **Process E**: User-Member Scoring (§11.7, §11.8) — Phase 1: numeric scores (parallel), Phase 2: LLM explanations (parallel)
- **Process F**: Score Refresh Notifier (§11.11) — publishes `scoring.user.completed`

### Three SBT Projects

| Project | Type | Purpose |
|---------|------|---------|
| `score-cache` | Library | Repository for score reads/writes. Wraps AlloyDB score tables. No pipeline/Pub/Sub. |
| `scoring-pipeline` | Cloud Run Job | Orchestrates per-user scoring: reads pairings, aggregates stances, calculates alignment, generates LLM explanations. Scheduled + ad-hoc. |
| `stance-materializer` | Cloud Run Job | Materializes member bill stances. Polls DB for readiness. Pre-computes user-bill alignment. |

### Scoring Flow

**Step 1 — User stance aggregation (§11.1)**

Multiple-choice answers → algorithmic extraction (deterministic):
- Per-topic `stanceScore` = weighted average: `Σ(agreeDir × multiplier × weight) / Σ(weight)`
- `agreeStance_direction = +1.0` if "Progressive", `-1.0` if "Conservative"`
- Bounded to [-1.0, +1.0]

Custom fill-in answers → LLM batch call via Component 9 `preference-interpretation` profile (stance adjustments only).

Topic importance from explicit user prioritization (`user_topic_priorities`), not answer frequency.

Output: `UserTopicProfile` — `(topic, stanceScore, importance)` where `stanceScore ∈ [-1.0, +1.0]`, `importance ∈ [0.0, 1.0]`.

**Step 2 — Legislator profile construction (§11.2)**

Query over pre-materialized `member_bill_stance_topics`:
1. Fetch rows where topic matches
2. Each row: `stanceDirection`, `reasoning`, `reasoningEmbedding`, `findingId`
3. Contribution = `stanceDirection × voteWeight` (per BEHAVIORAL_SPECS.md §2)
4. Average across all votes → `stanceScore` per topic

**Step 3 — Alignment score calculation + LLM explanation (§11.3, §11.4)**

Algorithmic base: Per-topic alignment = `1.0 - abs(userStance - legislatorStance) / 2.0`. Weighted average using user importance. **This is the authoritative score** (stored in `scores.aggregate_score`).

LLM explanation: Sonnet/Haiku via Component 9 → `AlignmentScoreOutput`. LLM's `overallScore` is for context only, not stored. Evidence from pre-materialized `member_bill_stance_topics` and `user_bill_alignments`.

### Score Storage (Two Tiers)

**Current scores** (overwritten on re-score):

| Table | Key | Content |
|-------|-----|---------|
| `scores` | (user_id, member_id) | `aggregate_score`, `status`, `last_updated`, `llm_model`, `total_bills`, `total_votes`, `non_overlapping_topics`, `reasoning`, `reasoning_embedding` |
| `score_topics` | (user_id, member_id, topic) | `score` per topic |
| `score_congress` | (user_id, member_id, congress) | `overall_score`, `bills_considered`, `votes_analyzed` |
| `score_congress_topics` | (user_id, member_id, congress, topic) | `score` per congress per topic |

**Score history** (append-only):

| Table | Key | Content |
|-------|-----|---------|
| `score_history` | score_id (UUID) | `aggregate_score`, `status`, `trigger_event` ("scheduled"/"ad-hoc"), `reasoning`, `reasoning_embedding` |
| `score_history_congress` | (score_id, congress) | `overall_score`, `bills_considered`, `votes_analyzed` |
| `score_history_congress_topics` | (score_id, congress, topic) | `score` |
| `score_history_highlights` | (score_id, bill_id, topic) | `stance`, `vote`, `alignment` |

### Persistence Mapping

| Table | Written By | When |
|-------|-----------|------|
| `user_legislator_pairings` | PairingValidator (§11.6) | At signup, scheduled validation, user location change |
| `stance_materialization_status` | Votes/Analysis pipelines | On vote position change, analysis completion |
| `member_bill_stances` + `member_bill_stance_topics` | StanceMaterializer (§11.9) | Scheduled scanner finds bills with both votes + analysis |
| `user_bill_alignments` + `user_amendment_alignments` | UserBillAligner (§11.10) | Scheduled, for changed bills and changed users |
| `scores` + `score_topics` + `score_congress` + `score_congress_topics` | ScorePersister.upsertScore (§11.5) | After each scoring run |
| `score_history` + sub-tables | ScorePersister.appendHistory (§11.5) | After each scoring run (append) |

### Event Contracts

| Event | Direction | Payload |
|-------|-----------|---------|
| `member.updated` | **Consumes** (Pairing Validator) | `{ memberId }` |
| `user.profile.updated` | **Consumes** (Pairing Validator + User-Bill Alignment) | `{ userId, topicsChanged[] }` |
| `scoring.user.requested` | **Consumes** (Scoring Pipeline) | `{ userId, requestId, source }` |
| `scoring.user.completed` | **Emits** (Score Refresh Notifier) | `{ userId, requestId, memberScoreCount, status }` |

> Scoring engine does NOT consume `analysis.completed` or `vote.recorded` events. Those pipelines update `stance_materialization_status` in DB. Stance materializer polls DB for readiness.

---

## Implementation Areas

| Area | Project | Description |
|------|---------|-------------|
| 11.1 User Stance Aggregation | `scoring-pipeline` | Extracts per-topic stance and importance from Q&A. Algorithmic for MC. LLM batch for custom via Component 9. |
| 11.2 Legislator Profile Construction | `scoring-pipeline` | Queries pre-materialized `member_bill_stance_topics` → per-topic stance per legislator. |
| 11.3 Alignment Score Calculator | `scoring-pipeline` | Linear distance on [-1.0, +1.0] stance scale, weighted by user importance. |
| 11.4 Score Explainer | `scoring-pipeline` | LLM explanation via Component 9 + Component 10 llm-adapter. Non-fatal on failure. |
| 11.5 Score Persistence | `score-cache` | AlloyDB score repository. Upserts current scores, appends history. |
| 11.6 Pairing Validator | `scoring-pipeline` | Persists `user_legislator_pairings`. Scheduled + location change + `member.updated`. |
| 11.7 Scoring Orchestrator | `scoring-pipeline` | Per-user: reads pairings → aggregates stances → scores all legislators (Phase 1 numeric, Phase 2 LLM). |
| 11.8 Scoring Pipeline Entry Point | `scoring-pipeline` | Cloud Run Job. Scheduled + ad-hoc. Subscribes to `scoring.user.requested`. |
| 11.9 Stance Materializer | `stance-materializer` | Scheduled DB scanner. Polls for ready bills. Materializes per-topic stances with reasoning/embedding. |
| 11.10 User-Bill Alignment | `stance-materializer` | Scheduled. Pre-computes per-bill, per-topic alignment. |
| 11.11 Score Refresh Notifier | `scoring-pipeline` | Publishes `scoring.user.completed` on ad-hoc completion. |

---

## Package Structure

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

---

## Dependencies

```
repcheck-scoring-engine/
├── score-cache (publishable library)
│   ├── repcheck-shared-models            (Component 1 § 1.5 — score DOs)
│   ├── repcheck-pipeline-models          (Component 2 § 2.10 — Tables constants)
│   └── Doobie                            (AlloyDB)
│
├── scoring-pipeline (Cloud Run Job)
│   ├── score-cache                       (internal SBT)
│   ├── repcheck-shared-models            (Component 1)
│   ├── repcheck-pipeline-models          (Component 2 — events, tracking)
│   ├── repcheck-prompt-engine-users      (Component 9 — UserPromptAssembler)
│   ├── repcheck-llm-analysis (llm-adapter) (Component 10 — LlmProvider, LlmRetryWrapper)
│   ├── repcheck-llm-analysis (in-process-ml) (Component 10 § 10.5/10.8 — SemanticEmbeddingService)
│   ├── Doobie                            (AlloyDB)
│   ├── Google Cloud Pub/Sub               (scoring.user.requested subscription)
│   └── http4s Ember                      (LLM API calls)
│
└── stance-materializer (Cloud Run Job)
    ├── repcheck-shared-models            (Component 1)
    ├── repcheck-pipeline-models          (Component 2)
    ├── repcheck-llm-analysis (llm-adapter) (Component 10)
    ├── repcheck-llm-analysis (in-process-ml) (Component 10 § 10.5/10.8)
    ├── repcheck-prompt-engine-users      (Component 9)
    ├── Doobie                            (AlloyDB)
    └── http4s Ember                      (LLM API calls)
```

---

## Cross-Component Updates

| Component | Update | Reason |
|-----------|--------|--------|
| **Component 1 § 1.5** | Add DOs: `UserLegislatorPairingDO`, `MemberBillStanceTopicDO`, `UserBillAlignmentDO`, `UserAmendmentAlignmentDO`, `StanceMaterializationStatusDO` | New tables for batch scoring |
| **Component 1 § 1.5** | Update `ScoreDO`: add `status`, `nonOverlappingTopics`, `reasoning`, `reasoningEmbedding` | Score status + explanation |
| **Component 1 § 1.5** | Update `ScoreHistoryDO`: add `status`, `reasoningEmbedding` | History enhancements |
| **Component 2 § 2.1** | Add `ScoringUserRequestedEvent`, `ScoringUserCompletedEvent`; update consumers for `VoteRecordedEvent`, `AnalysisCompletedEvent`, `UserProfileUpdatedEvent`, `MemberUpdatedEvent` | Batch scoring replaces event-driven |
| **Component 2 § 2.10** | Add `Tables` constants: `user_legislator_pairings`, `member_bill_stance_topics`, `user_bill_alignments`, `user_amendment_alignments`, `stance_materialization_status` | New table constants |
| **BEHAVIORAL_SPECS.md § 3** | Rewrite "Incremental Scoring" → "Batch Scoring Architecture" | Fundamental architecture change |
| **Votes pipeline § 6.5** | Add `stance_materialization_status` upsert after vote position persistence | Update status flags |
| **Analysis pipeline § 10.10** | Add `stance_materialization_status` upsert after analysis completion | Update status flags |

---

## Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit | `UserStanceAggregator` MC extraction, importance normalization | MockitoScala |
| Unit | `AlignmentScoreCalculator` formula, edge cases | No infrastructure |
| Unit | `LegislatorProfileBuilder` stance profile from pre-materialized data | MockitoScala |
| Unit | `PairingValidator` logic | MockitoScala |
| Unit | `ScoringOrchestrator` coordination, phasing | MockitoScala |
| Unit | `StanceMaterializationScanner` query logic | MockitoScala |
| WireMock | LLM calls from `ScoreExplainer`, `StanceMaterializer` | WireMock |
| Integration | `ScorePersister` upsert + history round-trip | AlloyDB Omni (Docker) |
| Integration | `LegislatorProfileBuilder` against seeded `member_bill_stance_topics` | AlloyDB Omni |
| Integration | `UserStanceAggregator` with Q&A data | AlloyDB Omni |
| Integration | `StanceMaterializer` full materialization | AlloyDB Omni |
| Integration | `UserBillAligner` alignment computation | AlloyDB Omni |
| Integration | `PairingValidator` pairing creation | AlloyDB Omni |
| E2E | Full scoring from `scoring.user.requested` to scores written | AlloyDB Omni + WireMock |
| E2E | Stance materialization → scoring | AlloyDB Omni + WireMock |
| E2E | Ad-hoc scoring → completion event published | AlloyDB Omni + WireMock + mock Pub/Sub |