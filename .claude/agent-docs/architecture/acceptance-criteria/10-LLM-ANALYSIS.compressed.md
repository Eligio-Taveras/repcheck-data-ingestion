<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/10-LLM-ANALYSIS.md -->

# Component 10 — LLM Analysis

Repository `repcheck-llm-analysis`: three SBT projects (`llm-adapter`, `bill-decomposition-pipeline`, `bill-analysis-pipeline`).
**Depends on**: Component 1, Component 2, Component 8.

## System Context

**Execution engine for bill analysis, split into two pipelines:**

1. **Decomposition pipeline**: Receives `bill.text.ingested` events → decomposes bill texts into sections and concept groups via Ollama + DJL + Smile + Haiku simplification → emits `bill.decomposition.completed`
2. **Analysis pipeline**: Receives `bill.decomposition.completed` → runs tiered multi-pass LLM analysis (Pass 1: Haiku all bills, Pass 2: Sonnet filtered, Pass 3: Opus rare) on simplified concept groups → persists results → emits `analysis.completed`

Analysis always operates on simplified concept groups, never raw text. This reduces LLM call volume and cost.

### Three SBT Projects

| Project | Type | Purpose |
|---------|------|---------|
| `llm-adapter` | Library | `LlmProvider[F]` trait + `ClaudeProvider` (Anthropic Java SDK) + `OllamaProvider` (HTTP to localhost sidecar). Structured output only (`completeStructured[A]`). Handles retry, rate limiting. |
| `bill-decomposition-pipeline` | Cloud Run Job | Subscribes `bill.text.ingested` → Ollama parse → DJL embed → Smile cluster → Haiku simplify → persist sections/concept groups → emit `bill.decomposition.completed` |
| `bill-analysis-pipeline` | Cloud Run Job | Subscribes `bill.decomposition.completed` → Pass 1/2/3 analysis on simplified concept groups → persist results → emit `analysis.completed` |

### Decomposition: Hybrid Local + LLM

All bills decomposed; all analysis operates on simplified concept groups.

| Step | Tech | Cost | Output |
|------|------|------|--------|
| 1. Section parsing | Ollama sidecar (Llama 3.2 1B) | Free | Parsed sections |
| 2. Section search embeddings | DJL + ONNX (in-process) | Free | Embedding vectors |
| 3. Persist sections | AlloyDB | — | `bill_text_sections` + embeddings |
| 4. Clustering embeddings | DJL + ONNX (all-MiniLM-L6-v2, 384-dim) | Free | Ephemeral vectors (clustering only) |
| 5. Semantic clustering | Smile (k-means/DBSCAN) | Free | Concept group assignments |
| 6. Persist concept groups | AlloyDB | — | `bill_concept_groups`, `bill_concept_group_sections` |
| 7. Concept simplification | Haiku API | ~$0.001/group | `bill_concept_groups.simplified_text` |
| 8. Concept search embeddings | DJL + ONNX | Free | Embedding vectors |
| 9. Persist simplified + embeddings | AlloyDB | — | `bill_concept_groups.embedding` |

**Key rules:**
- All bills decomposed, even short ones — uniform input for analysis pipeline
- Decomposition artifacts tied to text version, reusable across re-analyses
- All search embeddings generated locally (DJL/ONNX) — zero embedding API cost
- If decomposition exists for version, reuse it
- Large bills → many concept groups → multiple LLM calls (per-concept-group simplification)

### Multi-Pass Analysis

Operates on simplified concept groups. Finding types from `finding_types` table guide output classification.

| Pass | Model | Applies To | Input | Output |
|------|-------|-----------|-------|--------|
| 1 | Haiku | All | Simplified groups + finding types | Summary, topics, stance, pork, impact, fiscal + routing scores (high-profile, media level, appropriations, stance confidence) |
| 2 | Sonnet | Filtered | Groups + Pass 1 results + finding types | Same schemas, higher quality + cross-concept contradiction score, expected vote contention |
| 3 | Opus | Rare | Groups + Pass 1/2 results + finding types | Same schemas, highest quality + cross-concept resolution |

**Why all outputs in Pass 1:** Every bill needs complete analysis (summary, topics, stance, pork, impact, fiscal). Stance critical for Component 11 scoring. Pass 2/3 produce *better* versions, not different outputs.

**Finding types in prompts:** `finding_types` table entries included in every LLM prompt. Model classifies along established categories, not ad-hoc labels.

**Pass routing (all thresholds configurable, data-driven from LLM output):**
- **Pass 2 trigger:** Pass 1 high-profile score exceeds threshold OR media coverage level high OR appropriations estimate exceeds threshold OR stance confidence below threshold
- **Pass 3 trigger:** Pass 2 confidence scores below threshold on any output OR expected vote contention high OR cross-concept contradiction score high

**Routing scores per pass:**
- **Pass 1:** high-profile score, media coverage level, appropriations estimate, stance confidence → feed Pass 2 routing
- **Pass 2:** cross-concept contradiction score, expected vote contention (LLM-assessed, not from actual votes), overall confidence → feed Pass 3 routing

**Vote contention is LLM-assessed** (no dependency on Component 6 actual vote data for newly introduced bills).

**Idempotency:** Re-analysis of same bill version creates new analysis run (append-only), preserving history.

### Persistence: Two Layers

| Layer | Tables | Tied To | Written During |
|-------|--------|---------|-----------------|
| Text | `bill_text_sections`, `bill_concept_groups`, `bill_concept_group_sections` | Text version | Decomposition |
| Analysis | `bill_analyses`, `bill_concept_summaries`, `bill_analysis_topics`, `bill_findings`, `bill_fiscal_estimates` | Analysis run | Analysis (after each pass) |

### Ollama Sidecar

Second container in decomposition pipeline Cloud Run Job only:
- **Model:** Llama 3.2 1B (small, fast)
- **Comm:** HTTP on `localhost:11434`
- **Lifecycle:** Starts with Cloud Run Job, shares network namespace
- **Cost:** Allocated Cloud Run CPU/memory, no per-token charges
- **Note:** Analysis pipeline does NOT have sidecar

### Event Contracts

| Event | Direction | Pipeline | Payload |
|-------|-----------|----------|---------|
| `bill.text.ingested` | Consumed | Decomposition | `{ billId, versionId, congress, versionCode, previousVersionCode, committeeCode }` |
| `bill.decomposition.completed` | Produced/Consumed | Decomposition → Analysis | `{ billId, versionId, conceptGroupCount, sectionCount }` |
| `analysis.completed` | Produced | Analysis | `{ billId, analysisId, topics[], modelUsed }` |

---

## Implementation Areas

| Area | Project | Description |
|------|---------|-------------|
| 10.1 | `llm-adapter` | `LlmProvider[F]` trait + `ClaudeProvider` + `OllamaProvider`. Structured output only. |
| 10.2 | `llm-adapter` | Provider config, API key mgmt, retry w/ exponential backoff, rate limiting, fallback chains |
| 10.3 | `llm-adapter` | `OllamaProviderConfig`, `SectionParseResult` — Ollama-specific config & output types |
| 10.4 | `bill-decomposition-pipeline` | Orchestrates full pipeline: Ollama parse → DJL embed → Smile cluster → persist → Haiku simplify → embed |
| 10.5 | `bill-decomposition-pipeline` | DJL/ONNX embedding (search + clustering) + Smile clustering — all local |
| 10.6 | `bill-analysis-pipeline` | Pass 1/2/3 analysis chain using Component 8 prompts + 10.1 LLM providers. Includes finding type descriptions. Pass routing logic. |
| 10.7 | `bill-analysis-pipeline` | Maps LLM output schemas to DOs, persists to AlloyDB analysis layer |
| 10.8 | Shared | Local DJL/ONNX embedding service (called at each pipeline stage) — sections, concept groups, findings. Zero API cost. |
| 10.9 | `bill-decomposition-pipeline` | Cloud Run Job entry point, Pub/Sub `bill.text.ingested` subscriber, `bill.decomposition.completed` emitter |
| 10.10 | `bill-analysis-pipeline` | Cloud Run Job entry point, Pub/Sub `bill.decomposition.completed` subscriber, `analysis.completed` emitter |

---

## Package Structure

```
repcheck-llm-analysis/
├── llm-adapter/
│   └── repcheck.llm.adapter
│       ├── core
│       │   ├── LlmProvider (10.1)
│       │   ├── LlmRequest (10.1)
│       │   ├── LlmResponse (10.1)
│       │   └── StructuredOutputEnforcer (10.1)
│       ├── providers
│       │   ├── ClaudeProvider (10.1)
│       │   ├── OllamaProvider (10.1)
│       │   ├── OllamaProviderConfig (10.3)
│       │   └── SectionParseResult (10.3)
│       ├── config
│       │   ├── LlmAdapterConfig (10.2)
│       │   └── ClaudeProviderConfig (10.2)
│       ├── retry
│       │   ├── LlmRetryWrapper (10.2)
│       │   └── LlmErrorClassifier (10.2)
│       └── errors
│           ├── LlmCallFailed (10.2)
│           ├── StructuredOutputParseFailed (10.1)
│           └── ProviderUnavailable (10.2)
│
├── bill-decomposition-pipeline/
│   └── repcheck.decomposition.pipeline
│       ├── orchestration
│       │   ├── DecompositionOrchestrator (10.4)
│       │   └── SimplifiedConceptOutput (10.4)
│       ├── ml
│       │   ├── SectionEmbedder (10.5)
│       │   └── ConceptClusterer (10.5)
│       ├── persistence
│       │   └── DecompositionPersister (10.4)
│       ├── config
│       │   └── DecompositionPipelineConfig (10.9)
│       ├── app
│       │   └── BillDecompositionPipelineApp (10.9)
│       └── errors
│           ├── DecompositionFailed (10.4)
│           └── ClusteringFailed (10.5)
│
├── bill-analysis-pipeline/
│   └── repcheck.analysis.pipeline
│       ├── analysis
│       │   ├── AnalysisOrchestrator (10.6)
│       │   ├── PassExecutor (10.6)
│       │   ├── PassRouter (10.6)
│       │   ├── ConceptGroupPassOutput (10.6)
│       │   ├── RoutingAssessment (10.6)
│       │   ├── AnalysisConceptGroup (10.6)
│       │   ├── AnalysisFindingType (10.6)
│       │   ├── Pass1RoutingScores (10.6)
│       │   ├── Pass2RoutingScores (10.6)
│       │   └── PassRoutingConfig (10.6)
│       ├── persistence
│       │   ├── AnalysisResultPersister (10.7)
│       │   └── AnalysisRunCreator (10.7)
│       ├── config
│       │   └── AnalysisPipelineConfig (10.10)
│       ├── app
│       │   └── BillAnalysisPipelineApp (10.10)
│       └── errors
│           ├── AnalysisPassFailed (10.6)
│           └── PersistenceFailed (10.7)
│
└── shared
    └── repcheck.analysis.embedding
        ├── SemanticEmbeddingService (10.8)
        └── EmbeddingGenerationFailed (10.8)
```

---

## Dependencies

```
llm-adapter (publishable library)
├── repcheck-shared-models (Component 1 — LLM output schemas §1.6, base traits §1.7)
├── Anthropic Java SDK
└── http4s Ember

bill-decomposition-pipeline (Cloud Run Job)
├── llm-adapter
├── repcheck-shared-models (DOs §1.9, §1.4)
├── repcheck-pipeline-models (Component 2 — events §2.1, tables, tracking)
├── Doobie (AlloyDB)
├── Google Cloud Pub/Sub SDK
├── DJL + ONNX Runtime (embedding)
├── Smile ML (clustering)
└── http4s Ember (Ollama HTTP)

bill-analysis-pipeline (Cloud Run Job)
├── llm-adapter
├── repcheck-shared-models (DOs §1.9, §1.4)
├── repcheck-pipeline-models (Component 2 — events §2.1, tables, tracking)
├── repcheck-prompt-engine-bills (Component 8 — prompt assembly)
├── Doobie (AlloyDB)
├── Google Cloud Pub/Sub SDK
├── DJL + ONNX Runtime (embedding)
└── http4s Ember (LLM APIs)
```

---

## Cross-Component Updates Required

| Component | Update | Reason |
|-----------|--------|--------|
| **1 §1.9** | Add `code` column to `FindingTypeDO` | 10.7 looks up finding types by code (e.g., "pork_barrel") |
| **1 §1.9** | Add ~15 fields to `BillAnalysisDO` | 10.7 writes `status`, `versionId`, `createdAt`, `completedAt`, `passesExecuted`, `highestModelUsed`, routing score columns |
| **1 §1.9** | Add `passNumber` to `BillFindingDO`, `BillAnalysisTopicDO`, `BillConceptSummaryDO`, `BillFiscalEstimateDO` | Multi-pass analysis appends per-pass findings |
| **1 §1.9** | Add `topics`, `readingLevel`, `keyPoints` to `BillConceptSummaryDO` | Richer per-concept summaries |
| **2 §2.1** | Add `versionId: UUID` to `BillTextIngestedEvent` | Decomposition pipeline needs text version PK |
| **2 §2.1** | Update `analysis.completed`: `passCompleted: Int` → `passesExecuted: List[Int]`, `llmModel` → `modelUsed` | Multi-pass analysis |
| **2 §2.1** | Register `bill.decomposition.completed` as new event type | New event connecting pipelines |
| **4 §4.8** | Include `versionId` in `BillTextIngestedEvent` emission | Must include text version PK |

---

## Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit | LLM adapter trait, Claude provider, retry, structured output parsing | MockitoScala (mock HTTP) |
| Unit | Decomposition orchestrator, pass router, result persister, embedding service | MockitoScala (mock providers, repos) |
| WireMock | Claude API, Ollama sidecar HTTP contracts | WireMock |
| Integration | Full decomposition with real DJL + Smile + PostgreSQL | AlloyDB Omni (Docker), DJL models loaded |
| Integration | Analysis result persistence round-trip | AlloyDB Omni (Docker) |
| Integration | DJL embedding generation (search + clustering models) | DJL models loaded |
| E2E | Full decomposition from `bill.text.ingested` to `bill.decomposition.completed` | AlloyDB Omni + WireMock (mock Haiku) |
| E2E | Full analysis from `bill.decomposition.completed` to `analysis.completed` | AlloyDB Omni + WireMock (mock LLMs) |