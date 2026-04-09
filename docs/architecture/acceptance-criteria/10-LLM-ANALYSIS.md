# Acceptance Criteria: Component 10 — LLM Analysis

> Repository `repcheck-llm-analysis` containing three SBT projects: `llm-adapter` (vendor-neutral LLM client library), `bill-decomposition-pipeline` (Cloud Run Job that decomposes bill text into searchable concept groups), and `bill-analysis-pipeline` (Cloud Run Job that orchestrates multi-pass LLM analysis on decomposed bills).
> **Depends on**: `repcheck-shared-models` (Component 1), `repcheck-pipeline-models` (Component 2), `repcheck-prompt-engine-bills` (Component 8).

---

## System Context

### What This Component Does

Component 10 is the **execution engine** for bill analysis, split into two pipelines connected by events. The **decomposition pipeline** receives `bill.text.ingested` events (after text has been downloaded and stored by Component 4), decomposes all bill texts into sections and concept groups, and emits `bill.decomposition.completed` events. The **analysis pipeline** receives those events, runs a tiered multi-pass LLM analysis chain with finding types guiding the output structure, persists results, and emits `analysis.completed` events for downstream scoring.

```
Pub/Sub: bill.text.ingested
    |
    v
bill-decomposition-pipeline
    |
    |-- 1. Fetch bill text + amendments from AlloyDB
    |-- 2. Decomposition (ALL bills):
    |       a. Ollama sidecar: parse text into sections (any format)
    |       b. DJL/ONNX: generate embeddings per section (in-process, free)
    |       c. Persist sections + embeddings to AlloyDB
    |       d. DJL/ONNX: embed sections for clustering (384-dim, ephemeral)
    |       e. Smile: cluster into concept groups
    |       f. Persist concept groups to AlloyDB
    |       g. LLM (Haiku): simplify each concept group
    |       h. DJL/ONNX: generate embeddings per concept group (in-process, free)
    |       i. Persist simplified text + embeddings to AlloyDB
    |
    v
Pub/Sub: bill.decomposition.completed
    |
    v
bill-analysis-pipeline
    |
    |-- 3. Fetch simplified concept groups from AlloyDB
    |-- 4. Pass 1 (Haiku — all bills): full analysis per concept group
    |       (summary, topics, stance, pork, impact, fiscal — classified by finding type)
    |       → Persist Pass 1 results → DJL/ONNX embeddings for findings
    |-- 5. Pass 2 (Sonnet — filtered): same outputs, higher quality
    |       → Persist Pass 2 results → DJL/ONNX embeddings for findings
    |-- 6. Pass 3 (Opus — rare): same outputs, highest quality + cross-concept resolution
    |       → Persist Pass 3 results → DJL/ONNX embeddings for findings
    |
    v
DB: stance_materialization_status (has_analysis = true, all_passes_completed = true)
    |
    v
Pub/Sub: analysis.completed (informational — stance materialization is DB-polled, not event-driven)
```

### Three SBT Projects

| Project | Type | Purpose |
|---------|------|---------|
| `llm-adapter` | Library (publishable) | LLM client library: `LlmProvider[F]` trait + `ClaudeProvider` (Anthropic Java SDK) + `OllamaProvider` (HTTP to localhost sidecar). Structured output only (`completeStructured[A]`). Handles structured output enforcement, retry, rate limiting. No pipeline logic. |
| `bill-decomposition-pipeline` | Application (Cloud Run Job) | Subscribes to `bill.text.ingested`, decomposes all bill texts into sections and concept groups using Ollama + DJL + Smile + Haiku simplification, persists decomposition artifacts, emits `bill.decomposition.completed`. Has Ollama sidecar container. |
| `bill-analysis-pipeline` | Application (Cloud Run Job) | Subscribes to `bill.decomposition.completed`, runs multi-pass LLM analysis on simplified concept groups, persists analysis results, emits `analysis.completed`. No sidecar needed. |

### Decomposition Architecture

All bills go through decomposition. Analysis always operates on simplified concept groups, never raw bill text. This reduces the number and cost of LLM analysis calls — the analysis pipeline works with pre-digested, coherent summaries rather than raw text of arbitrary size.

The decomposition pipeline handles any format (XML, plain text, PDF-extracted text) using a hybrid local + LLM approach that minimizes cost:

| Step | Technology | Cost | Output |
|------|-----------|------|--------|
| 1. Text parsing / section identification | Ollama sidecar (Llama 3.2 1B) | Free (local) | Parsed section boundaries |
| 2. Section search embeddings | DJL + ONNX Runtime (in-process) | Free (local) | Embedding vectors for semantic search |
| 3. Persist sections + embeddings | AlloyDB write | N/A | `bill_text_sections` rows with `embedding` column populated |
| 4. Section embedding for clustering | DJL + ONNX Runtime (all-MiniLM-L6-v2, 384-dim) | Free (in-process) | Ephemeral 384-dim vectors (clustering only, not persisted) |
| 5. Semantic clustering | Smile ML library (k-means/DBSCAN) | Free (in-process) | Concept group assignments |
| 6. Persist concept groups | AlloyDB write | N/A | `bill_concept_groups`, `bill_concept_group_sections` |
| 7. Concept simplification | Haiku API via llm-adapter | ~$0.001/group | `bill_concept_groups.simplified_text` |
| 8. Concept group search embeddings | DJL + ONNX Runtime (in-process) | Free (local) | Embedding vectors for semantic search |
| 9. Persist simplified text + embeddings | AlloyDB write | N/A | `bill_concept_groups.embedding` column populated |

**Key rules:**
- **All bills go through decomposition** — even short bills. Analysis always operates on simplified concept groups. This provides a uniform input format for the analysis pipeline and ensures every bill has searchable sections and concept groups.
- Decomposition artifacts are tied to text version (`version_id`), not analysis run — reusable across re-analyses
- The 384-dim DJL embeddings are ephemeral (clustering only)
- **All search embeddings are generated locally via DJL/ONNX** — no API calls for embedding generation. This keeps embedding costs at zero regardless of volume.
  - After section persistence: each `bill_text_sections` row gets a search embedding (enables "find this specific part of the bill")
  - After simplification: each `bill_concept_groups` row gets a search embedding (enables "find simplified summaries about X")
  - After analysis: findings get search embeddings (in analysis pipeline)
- If decomposition has already been done for a text version, it is reused (check `bill_text_sections` for existing rows)
- Large bills may produce many concept groups, and individual concepts may themselves be large — the simplification step (Haiku) is per-concept-group, so LLM call count scales with bill complexity

### Multi-Pass Analysis Chain

Analysis operates on simplified concept groups. For bills with many concepts, this means **multiple LLM calls per pass** — one per concept group (or batched where context window allows). Finding types from the `finding_types` table are described in the prompt to guide the LLM's output classification.

| Pass | Model | Applies To | Input | Output Schemas (§1.6) |
|------|-------|-----------|-------|----------------------|
| Pass 1 | Haiku | All bills | Simplified concept groups + finding type descriptions | `BillSummaryOutput`, `TopicClassificationOutput`, `StanceClassificationOutput`, `PorkDetectionOutput`, `ImpactAnalysisOutput`, `FiscalEstimateOutput` — results classified by finding type. **Also produces routing scores:** high-profile score, media coverage level, appropriations estimate. |
| Pass 2 | Sonnet | Filtered bills | Concept groups + Pass 1 results + finding type descriptions | Same schemas, higher quality — deeper reasoning, better confidence scores. **Also produces cross-concept contradiction score** for Pass 3 routing. |
| Pass 3 | Opus | Rare/flagged | Concept groups + Pass 1 + Pass 2 results + finding type descriptions | Same schemas, highest quality — cross-concept resolution, ambiguity resolution |

**Why all outputs are in Pass 1:** Every bill needs a complete analysis — summary, topics, stance, pork detection, impact, and fiscal estimates — because not all bills receive Pass 2. Stance classification is especially critical: it determines what a bill DOES to each topic (e.g., "Progressive on healthcare"), which the scoring pipeline (Component 11) needs to interpret legislator votes. Pass 2 and Pass 3 don't produce *different* outputs — they produce *better versions* of the same outputs using more capable models.

**Finding types in prompts:** The `finding_types` table (seeded in migration 001/002) defines the categories of findings the LLM should produce (e.g., pork_barrel, fiscal_impact, civil_liberties, environmental). These finding type definitions are included in every LLM analysis prompt, so the model classifies its results along these established categories rather than inventing ad-hoc labels.

**Pass routing rules** (all thresholds configurable):

Routing is entirely data-driven from LLM output — no external flags. Each pass produces scores that feed the next pass's routing decision.

- **Pass 2 criteria** (any match triggers Pass 2):
  - Pass 1 high-profile score exceeds threshold (LLM assesses whether sponsor is leadership, bill has national significance, etc.)
  - Pass 1 media coverage level exceeds threshold (LLM assesses likely media attention based on bill content and context)
  - Pass 1 appropriations estimate exceeds configurable dollar threshold
  - Pass 1 stance confidence is below threshold (needs deeper analysis)
- **Pass 3 criteria** (any match triggers Pass 3):
  - Pass 2 confidence scores are below threshold on any output
  - Pass 2 expected vote contention exceeds threshold (LLM-assessed — no dependency on actual vote data)
  - Pass 2 cross-concept contradiction score exceeds threshold

**Routing scores produced by each pass:**
- **Pass 1 produces:** high-profile score, media coverage level, appropriations estimate, stance confidence — these feed Pass 2 routing
- **Pass 2 produces:** cross-concept contradiction score, expected vote contention score, overall confidence — these feed Pass 3 routing

**Vote contention is LLM-assessed.** Rather than depending on actual vote data from Component 6 (which may not exist for newly introduced bills), the LLM assesses expected vote contention as part of Pass 2. This eliminates cross-pipeline data dependencies.

**Idempotency**: Re-analysis of the same bill version creates a new analysis run (append-only), preserving history.

### Persistence Mapping

Two distinct table layers (per Component 1 §1.9):

| Layer | Tables | Tied To | When Written |
|-------|--------|---------|--------------|
| Bill text | `bill_text_sections`, `bill_concept_groups`, `bill_concept_group_sections` | Text version | During decomposition pipeline |
| Analysis | `bill_analyses`, `bill_concept_summaries`, `bill_analysis_topics`, `bill_findings`, `bill_fiscal_estimates` | Analysis run | During analysis pipeline (after each pass) |

### Ollama Sidecar

A second container in the **decomposition pipeline** Cloud Run Job:
- **Model**: Llama 3.2 1B (small, fast, sufficient for section identification)
- **Communication**: HTTP on `localhost:11434` (Ollama API)
- **Lifecycle**: Starts with the Cloud Run Job, shares network namespace
- **Cost**: Runs on allocated Cloud Run CPU/memory, no per-token charges
- **Note**: Only the decomposition pipeline has the Ollama sidecar. The analysis pipeline does not need it.

### Event Contracts

| Event | Direction | Pipeline | Payload | Reference |
|-------|-----------|----------|---------|-----------|
| `bill.text.ingested` | **Consumes** | Decomposition | `{ billId, versionId, congress, versionCode, previousVersionCode, committeeCode }` | Component 2 §2.1 (`versionId` added) |
| `bill.decomposition.completed` | **Produces / Consumes** | Decomposition → Analysis | `{ billId, versionId, conceptGroupCount, sectionCount }` | New event |
| `analysis.completed` | **Produces** | Analysis | `{ billId, analysisId, topics[], modelUsed }` | Component 2 §2.1 |

---

## Implementation Areas

| Area | Project | Status | Description |
|------|---------|--------|-------------|
| 10.1 LLM Provider Adapter | `llm-adapter` | New | `LlmProvider[F]` trait + `ClaudeProvider` (Anthropic Java SDK) + `OllamaProvider` (HTTP to sidecar). Structured output only — `completeStructured[A]`. |
| 10.2 LLM Adapter Configuration & Retry | `llm-adapter` | New | Provider config, API key management, retry with exponential backoff, rate limiting, fallback chains |
| 10.3 Ollama Provider Configuration | `llm-adapter` | New | `OllamaProviderConfig` and `SectionParseResult` — Ollama-specific configuration and output types for the `OllamaProvider` defined in 10.1 |
| 10.4 Decomposition Orchestrator | `bill-decomposition-pipeline` | New | Orchestrates the full decomposition pipeline: Ollama parsing → DJL embedding → Smile clustering → persistence → Haiku simplification → embedding |
| 10.5 In-Process ML (DJL + Smile) | `bill-decomposition-pipeline` | New | DJL/ONNX embedding (both search embeddings and clustering embeddings) and Smile clustering — all local, no external API dependencies |
| 10.6 Multi-Pass Analysis Orchestrator | `bill-analysis-pipeline` | New | Runs Pass 1/2/3 analysis chain using prompt profiles from Component 8 and LLM providers from 10.1, with pass routing logic. Includes finding type descriptions in prompts. |
| 10.7 Analysis Result Persistence | `bill-analysis-pipeline` | New | Maps LLM output schemas to DOs and persists to AlloyDB analysis layer tables |
| 10.8 Embedding Generation Service | Shared (both pipelines) | New | Local DJL/ONNX utility service for generating search embeddings at each pipeline stage — sections (after parsing), concept groups (after simplification), and analysis findings (after each pass). All local, zero API cost. |
| 10.9 Decomposition Pipeline Entry Point | `bill-decomposition-pipeline` | New | Cloud Run Job entry point, Pub/Sub subscriber for `bill.text.ingested`, event emission for `bill.decomposition.completed` |
| 10.10 Analysis Pipeline Entry Point | `bill-analysis-pipeline` | New | Cloud Run Job entry point, Pub/Sub subscriber for `bill.decomposition.completed`, event emission for `analysis.completed` |

## Component Routing Table

| Task | Area File |
|------|-----------|
| LLM adapter trait, Claude provider, Ollama provider | [10.1 LLM Provider Adapter](10-llm-analysis/10.1-llm-provider-adapter.md) |
| LLM configuration, retry, rate limiting, fallback chains | [10.2 LLM Adapter Configuration & Retry](10-llm-analysis/10.2-llm-adapter-config-retry.md) |
| Ollama provider configuration and output types | [10.3 Ollama Provider Configuration](10-llm-analysis/10.3-ollama-sidecar-client.md) |
| Decomposition orchestration (parse → embed → cluster → persist → simplify → embed) | [10.4 Decomposition Orchestrator](10-llm-analysis/10.4-decomposition-orchestrator.md) |
| In-process DJL embedding (search + clustering) and Smile clustering | [10.5 In-Process ML](10-llm-analysis/10.5-in-process-ml.md) |
| Multi-pass analysis chain (Pass 1/2/3) with routing and finding types | [10.6 Multi-Pass Analysis Orchestrator](10-llm-analysis/10.6-multi-pass-analysis-orchestrator.md) |
| Mapping LLM outputs to DOs and persisting to AlloyDB | [10.7 Analysis Result Persistence](10-llm-analysis/10.7-analysis-result-persistence.md) |
| Local DJL/ONNX embedding generation service (called at each pipeline stage) | [10.8 Embedding Generation Service](10-llm-analysis/10.8-embedding-generation-service.md) |
| Decomposition pipeline entry point, Pub/Sub subscriber (`bill.text.ingested`), event emission | [10.9 Decomposition Pipeline Entry Point](10-llm-analysis/10.9-decomposition-pipeline-entry-point.md) |
| Analysis pipeline entry point, Pub/Sub subscriber, event emission | [10.10 Analysis Pipeline Entry Point](10-llm-analysis/10.10-analysis-pipeline-entry-point.md) |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck-llm-analysis/
├── llm-adapter/
│   └── repcheck.llm.adapter
│       ├── core
│       │   ├── LlmProvider                 (10.1)
│       │   ├── LlmRequest                  (10.1)
│       │   ├── LlmResponse                 (10.1)
│       │   └── StructuredOutputEnforcer     (10.1)
│       ├── providers
│       │   ├── ClaudeProvider               (10.1)
│       │   ├── OllamaProvider               (10.1)
│       │   ├── OllamaProviderConfig         (10.3)
│       │   └── SectionParseResult           (10.3)
│       ├── config
│       │   ├── LlmAdapterConfig             (10.2)
│       │   └── ClaudeProviderConfig          (10.2)
│       ├── retry
│       │   ├── LlmRetryWrapper              (10.2)
│       │   └── LlmErrorClassifier           (10.2)
│       └── errors
│           ├── LlmCallFailed                (10.2)
│           ├── StructuredOutputParseFailed   (10.1)
│           └── ProviderUnavailable           (10.2)
│
├── bill-decomposition-pipeline/
│   └── repcheck.decomposition.pipeline
│       ├── orchestration
│       │   ├── DecompositionOrchestrator     (10.4)
│       │   └── SimplifiedConceptOutput       (10.4)
│       ├── ml
│       │   ├── SectionEmbedder              (10.5)
│       │   └── ConceptClusterer             (10.5)
│       ├── persistence
│       │   └── DecompositionPersister        (10.4)
│       ├── config
│       │   └── DecompositionPipelineConfig   (10.9)
│       ├── app
│       │   └── BillDecompositionPipelineApp  (10.9)
│       └── errors
│           ├── DecompositionFailed           (10.4)
│           └── ClusteringFailed             (10.5)
│
├── bill-analysis-pipeline/
│   └── repcheck.analysis.pipeline
│       ├── analysis
│       │   ├── AnalysisOrchestrator          (10.6)
│       │   ├── PassExecutor                  (10.6)
│       │   ├── PassRouter                    (10.6)
│       │   ├── ConceptGroupPassOutput        (10.6)
│       │   ├── RoutingAssessment             (10.6)
│       │   ├── AnalysisConceptGroup          (10.6)
│       │   ├── AnalysisFindingType           (10.6)
│       │   ├── Pass1RoutingScores            (10.6)
│       │   ├── Pass2RoutingScores            (10.6)
│       │   └── PassRoutingConfig             (10.6)
│       ├── persistence
│       │   ├── AnalysisResultPersister       (10.7)
│       │   └── AnalysisRunCreator            (10.7)
│       ├── config
│       │   └── AnalysisPipelineConfig        (10.10)
│       ├── app
│       │   └── BillAnalysisPipelineApp       (10.10)
│       └── errors
│           ├── AnalysisPassFailed           (10.6)
│           └── PersistenceFailed            (10.7)
│
└── shared (or inline in each pipeline)
    └── repcheck.analysis.embedding
        ├── SemanticEmbeddingService          (10.8)
        └── EmbeddingGenerationFailed         (10.8)
```

### Dependencies

```
repcheck-llm-analysis/
├── llm-adapter (publishable library)
│   ├── repcheck-shared-models            (Component 1 — LLM output schemas §1.6, base traits §1.7)
│   ├── Anthropic Java SDK                (Claude adapter)
│   └── http4s Ember                      (HTTP client)
│
├── bill-decomposition-pipeline (Cloud Run Job application)
│   ├── llm-adapter                       (internal SBT dependency — for Haiku simplification)
│   ├── repcheck-shared-models            (Component 1 — DOs §1.9, §1.4)
│   ├── repcheck-pipeline-models          (Component 2 — events §2.1, tables, pipeline tracking)
│   ├── Doobie                            (AlloyDB persistence)
│   ├── Google Cloud Pub/Sub SDK          (event subscribe/publish)
│   ├── DJL + ONNX Runtime                (in-process embedding — search + clustering)
│   ├── Smile ML                          (clustering)
│   └── http4s Ember                      (Ollama sidecar HTTP)
│
└── bill-analysis-pipeline (Cloud Run Job application)
    ├── llm-adapter                       (internal SBT dependency)
    ├── repcheck-shared-models            (Component 1 — DOs §1.9, §1.4)
    ├── repcheck-pipeline-models          (Component 2 — events §2.1, tables, pipeline tracking)
    ├── repcheck-prompt-engine-bills      (Component 8 — prompt assembly)
    ├── Doobie                            (AlloyDB persistence)
    ├── Google Cloud Pub/Sub SDK          (event subscribe/publish)
    ├── DJL + ONNX Runtime                (in-process embedding for analysis findings)
    └── http4s Ember                      (LLM API calls)
```

### Cross-Component Updates Required

The following changes in other components are required for Component 10 to work correctly:

| Component | Update Needed | Reason |
|-----------|--------------|--------|
| **Component 1 §1.9** | Add `code` column to `FindingTypeDO` | 10.7 looks up finding types by `code` (e.g., "pork_barrel"), not by numeric ID |
| **Component 1 §1.9** | Add ~15 fields to `BillAnalysisDO` | 10.7 writes `status`, `versionId`, `createdAt`, `completedAt`, `passesExecuted`, `highestModelUsed`, routing score columns, etc. |
| **Component 1 §1.9** | Add `passNumber` to `BillFindingDO`, `BillAnalysisTopicDO`, `BillConceptSummaryDO`, `BillFiscalEstimateDO` | Multi-pass analysis appends findings per pass |
| **Component 1 §1.9** | Add `topics`, `readingLevel`, `keyPoints` to `BillConceptSummaryDO` | 10.7 writes richer per-concept summaries |
| **Component 2 §2.1** | Add `versionId: UUID` to `BillTextIngestedEvent` payload | Decomposition pipeline subscribes to `bill.text.ingested` and needs the text version PK. `versionId` is available because §4.8 creates the `bill_text_versions` row before emitting this event. |
| **Component 2 §2.1** | Update `analysis.completed` event: `passCompleted: Int` → `passesExecuted: List[Int]`, `llmModel` → `modelUsed` | Multi-pass analysis produces multiple passes per run |
| **Component 2 §2.1** | Register `bill.decomposition.completed` as new event type | New event connecting decomposition → analysis pipelines |
| **Component 4 §4.8** | Include `versionId` in `BillTextIngestedEvent` emission | Component 4 §4.8 creates the `bill_text_versions` row and must include its PK in the event |

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | LLM adapter trait, Claude provider logic, retry, structured output parsing | MockitoScala (mock HTTP responses) |
| Unit tests | Decomposition orchestrator, pass router, result persister, embedding service | MockitoScala (mock providers, repos) |
| WireMock tests | Claude API HTTP interactions (request/response contracts) | WireMock |
| WireMock tests | Ollama sidecar API contract | WireMock |
| Integration tests | Full decomposition pipeline with real DJL + Smile + PostgreSQL | AlloyDB Omni (Docker), DJL model loaded |
| Integration tests | Analysis result persistence round-trip | AlloyDB Omni (Docker) |
| Integration tests | DJL embedding generation (search + clustering models) | DJL model loaded |
| E2E tests | Full decomposition pipeline from `bill.text.ingested` to `bill.decomposition.completed` | AlloyDB Omni (Docker) + WireMock (mock Haiku for simplification) |
| E2E tests | Full analysis pipeline from `bill.decomposition.completed` to `analysis.completed` | AlloyDB Omni (Docker) + WireMock (mock LLM APIs) |
