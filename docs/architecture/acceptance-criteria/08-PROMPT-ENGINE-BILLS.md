# Acceptance Criteria: Component 8 — Prompt Engine Bills

> Standalone library (`repcheck-prompt-engine-bills`) that composes LLM prompts for bill analysis.
> Loads versioned instruction blocks from GCS, assembles them into prompt chains using profiles, and injects dynamic bill context at runtime.
> This is a **library**, not a pipeline — it is consumed by the bill-analysis-pipeline (Component 10).
> **Depends on**: `repcheck-shared-models` (Component 1 — base traits from §1.7, output schemas from §1.6).

---

## System Context

### What This Component Does

The prompt engine is a **composable instruction framework**. It does NOT call the LLM — that is the LLM adapter's job (Component 10). The prompt engine's sole responsibility is assembling the prompt string that gets sent to the LLM.

```
GCS (instruction blocks + profiles)
    |
    v
Prompt Engine Bills (this component)
    |-- Load instruction blocks from GCS by stage (system, persona, lens, etc.)
    |-- Resolve a profile (which blocks to use, in what order, with what weights)
    |-- Inject dynamic context (bill text, amendments, metadata)
    |-- Apply weight translation (emphasis markers)
    |-- Assemble final prompt string
    |
    v
Assembled prompt string → bill-analysis-pipeline (Component 10) → LLM adapter → LLM
```

### No Hardcoded Prompts

Per CLAUDE.md universal rules: **all prompt fragments live in GCS. Prompt engines are loaders + assemblers only.** The prompt engine contains zero prompt text in its source code — all instructional content is stored in GCS and loaded at runtime.

This means:
- Prompt wording can be tuned without redeploying code
- A/B testing of prompt variations requires only GCS file changes
- Prompt versioning uses semver in filenames (e.g., `fiscal-lens-v1.2.0.yaml`)

### Tiered Analysis Profiles

The bill-analysis-pipeline (Component 10) uses three LLM passes with different model tiers (per LLM Cost Strategy §6):

| Pass | Model | Applies To | Analysis Type |
|------|-------|-----------|---------------|
| Pass 1 | Haiku | All bills | Extraction, classification, summary |
| Pass 2 | Sonnet | Filtered bills | Pork detection, impact analysis, stance, fiscal |
| Pass 3 | Opus | Rare/flagged bills | Ambiguity resolution, cross-bill analysis |

Each pass has its own **prompt profile** — a different combination of instruction blocks assembled in a different way. The prompt engine provides one profile per pass. The bill-analysis-pipeline selects which profile to use based on pass routing rules.

### Bill Text Decomposition Support

Bill text can be extremely large (omnibus bills, infrastructure acts — thousands of pages). Raw text cannot be injected into a single LLM context window. Not all bills arrive as XML — some are plain text or PDF (essentially text). The bill-analysis-pipeline (Component 10) owns the **decomposition orchestration** — text parsing (Ollama sidecar), in-process embedding (DJL/ONNX), and semantic clustering (Smile) are handled by Component 10 with zero external API cost. The only decomposition step that calls an external LLM is **concept simplification**, which uses prompts from this component:

```
Raw bill text (any format: XML, plain text, PDF-extracted text)
    |
    v
Component 10: Text parsing / section identification (Ollama sidecar — free, local)
    |
    v
Component 10: Section embedding (DJL + ONNX Runtime, 384-dim — free, in-process)
    |
    v
Component 10: Semantic clustering (Smile k-means/DBSCAN — free, in-process)
    |
    v
Component 10: Persist sections + groups to AlloyDB (bill text layer)
    |
    v
Component 10 calls LLM with Component 8 prompt: "Simplify this concept group"
    |   (uses concept-simplification profile from this component — Haiku, ~$0.001/group)
    v
Simplified concept summaries (List[ConceptGroup]) → persisted to bill_concept_groups
    |
    v
Component 10 calls LLM with Component 8 prompt: Pass 1/2/3 analysis
    |   (uses analysis profiles from this component, with simplified concepts as context)
    v
Structured analysis output → persisted to analysis layer tables
```

The prompt engine provides **decomposition and analysis profiles**:

| Profile | Purpose | Used By |
|---------|---------|---------|
| `concept-simplification` | Simplify a group of related bill sections into a coherent summary | Component 10 decomposition step (Haiku) |
| `section-classification` | Classify a bill section by topic/policy area — **fallback** for when embedding-based clustering is insufficient | Component 10 grouping step (Haiku, rarely used) |
| `pass1-extraction` | Full extraction/classification/summary from simplified concepts | Component 10 Pass 1 |
| `pass2-deep-analysis` | Pork, impact, stance, fiscal analysis from simplified concepts | Component 10 Pass 2 |
| `pass3-ambiguity-resolution` | Cross-concept ambiguity resolution | Component 10 Pass 3 |

> **Decomposition logic vs decomposition prompts:** Component 10 decides *when* to decompose (based on text length), *how* to parse (Ollama sidecar for any text format), *how* to embed (DJL in-process), and *how* to cluster (Smile). Component 8 provides the *prompts* for the LLM-assisted simplification step and the fallback section-classification step. This keeps all instructional content in GCS while keeping orchestration in the pipeline.

### Base Traits (from Component 1 §1.7)

The prompt engine builds on these types defined in `repcheck-shared-models`:

| Type | Source | Role |
|------|--------|------|
| `PromptStage` | §1.7 | Enum: System, Persona, Lens, Context, Guardrails, Output, Custom |
| `InstructionBlock` | §1.7 | Atomic prompt fragment: name, stage, weight, version, content |
| `StageConfig` | §1.7 | Stage + block names + weight for a profile entry |
| `PromptProfile` | §1.7 | Named chain of `StageConfig` entries defining a complete prompt |
| `ChainAssembler` | §1.7 | Trait that orders stages, applies weights, merges blocks, injects context |
| `WeightTranslator` | §1.7 | Converts weight (0.0-1.0) to emphasis markers in prompt text |

### Output Schemas (from Component 1 §1.6)

The prompt engine references these output schemas in its Output stage blocks (telling the LLM what JSON structure to return):

| Schema | Used By |
|--------|---------|
| `BillSummaryOutput` | Pass 1 — summary generation |
| `TopicClassificationOutput` | Pass 1 — topic/policy classification |
| `StanceClassificationOutput` | Pass 2 — political stance analysis |
| `PorkDetectionOutput` | Pass 2 — pork/rider detection |
| `ImpactAnalysisOutput` | Pass 2 — who benefits/harmed |
| `FiscalEstimateOutput` | Pass 2 — cost estimates |

The prompt engine does not import these schemas directly — it references them by name in Output stage blocks stored in GCS. The LLM adapter (Component 10) uses the actual schema types for response parsing.

### GCS Layout

```
gs://repcheck-prompt-configs/
  └── bills/
      ├── blocks/                          # Individual instruction blocks
      │   ├── system/
      │   │   └── base-legislative-analyst-v1.0.0.yaml
      │   ├── persona/
      │   │   ├── general-audience-v1.0.0.yaml
      │   │   └── expert-audience-v1.0.0.yaml
      │   ├── lens/
      │   │   ├── fiscal-lens-v1.0.0.yaml
      │   │   ├── civil-liberties-lens-v1.0.0.yaml
      │   │   ├── healthcare-lens-v1.0.0.yaml
      │   │   └── pork-detector-v1.0.0.yaml
      │   ├── context/
      │   │   ├── bill-text-v1.0.0.yaml         # placeholder: {{bill_text}}
      │   │   ├── amendments-v1.0.0.yaml         # placeholder: {{amendments}}
      │   │   └── related-bills-v1.0.0.yaml      # placeholder: {{related_bills}}
      │   ├── guardrails/
      │   │   ├── nonpartisan-constraint-v1.0.0.yaml
      │   │   └── accuracy-constraint-v1.0.0.yaml
      │   └── output/
      │       ├── pass1-extraction-schema-v1.0.0.yaml
      │       ├── pass2-deep-analysis-schema-v1.0.0.yaml
      │       └── pass3-resolution-schema-v1.0.0.yaml
      └── profiles/                        # Assembled prompt profiles
          ├── concept-simplification-v1.0.0.yaml     # decomposition support
          ├── section-classification-v1.0.0.yaml     # decomposition support
          ├── pass1-extraction-v1.0.0.yaml           # analysis
          ├── pass2-deep-analysis-v1.0.0.yaml        # analysis
          ├── pass3-ambiguity-resolution-v1.0.0.yaml # analysis
          ├── summary-only-v1.0.0.yaml               # utility
          └── pork-detection-v1.0.0.yaml             # utility
```

### Deployment Pipeline

```
repo: prompt-configs/bills/  ── git push ──→  GitHub Actions  ──→  gs://repcheck-prompt-configs/bills/
```

- Instruction blocks and profiles are version-controlled in the repo under `prompt-configs/bills/`
- GitHub Actions deploys updated configs to GCS on merge to main
- The prompt engine reads from GCS at runtime
- Local file fallback for development (reads from `prompt-configs/bills/` on filesystem)

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 8.1 GCS Block Loader | New | Loads instruction blocks and profiles from GCS with version filtering and local fallback |
| 8.2 Bill Analysis Profiles | New | Defines decomposition and analysis profiles, and the bill-specific block catalog |
| 8.3 Bill Prompt Assembler | New | Bill-specific `ChainAssembler` implementation with context injection for bill concepts, amendments, and metadata |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Loading instruction blocks and profiles from GCS | [8.1 GCS Block Loader](08-prompt-engine-bills/08.1-gcs-block-loader.md) |
| Decomposition and analysis profile definitions, block catalog | [8.2 Bill Analysis Profiles](08-prompt-engine-bills/08.2-bill-analysis-profiles.md) |
| Bill-specific prompt assembly with context injection | [8.3 Bill Prompt Assembler](08-prompt-engine-bills/08.3-bill-prompt-assembler.md) |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck-prompt-engine-bills/
└── repcheck.prompt.bills
    ├── loader
    │   ├── BlockLoader                    (8.1)
    │   └── ProfileLoader                  (8.1)
    ├── profiles
    │   └── BillAnalysisProfiles           (8.2)
    ├── assembler
    │   ├── BillPromptAssembler            (8.3)
    │   └── BillContextInjector            (8.3)
    ├── config
    │   └── BillPromptEngineConfig         (8.1)
    └── errors
        ├── BlockLoadFailed                (8.1)
        ├── ProfileLoadFailed              (8.1)
        └── ContextInjectionFailed         (8.3)
```

### Dependencies

```
repcheck-prompt-engine-bills
├── repcheck-shared-models               (published artifact — Component 1)
│   ├── PromptStage, InstructionBlock, StageConfig, PromptProfile   (§1.7)
│   ├── ChainAssembler, WeightTranslator                            (§1.7)
│   └── BillSummaryOutput, TopicClassificationOutput, etc.          (§1.6, referenced by name only)
└── GCS Java SDK                         (runtime dependency)
    └── Wrapped in Sync[F] per project conventions
```

> **No dependency on pipeline-models or ingestion-common.** This is a pure library — it has no Pub/Sub, no Doobie, no pipeline execution infrastructure. Its only RepCheck dependency is `shared-models`.

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | `ChainAssembler` integration, weight translation, context injection, profile validation | MockitoScala (mock GCS client) |
| GCS integration tests | Block loading, version filtering, profile resolution | Testcontainers (fake GCS) or local file fallback |
| Prompt assembly tests | Full profile assembly with real blocks → verify prompt structure and content ordering | Local file fallback (no GCS needed) |
| Contract tests | Assembled prompts contain expected output schema references | Unit tests |
