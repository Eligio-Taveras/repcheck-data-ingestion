<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/08-PROMPT-ENGINE-BILLS.md -->

# Acceptance Criteria: Component 8 — Prompt Engine Bills

Standalone library (`repcheck-prompt-engine-bills`) that composes LLM prompts for bill analysis. Loads versioned instruction blocks from GCS, assembles them into prompt chains using profiles, and injects dynamic bill context at runtime. **Depends on**: `repcheck-shared-models` (Component 1 — base traits from §1.7, output schemas from §1.6).

## System Context

### What This Component Does

The prompt engine assembles prompt strings for consumption by the LLM adapter (Component 10). It loads instruction blocks from GCS, resolves profiles, injects dynamic context, applies weight translation, and returns the final prompt string.

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

**All prompt fragments live in GCS. Prompt engines are loaders + assemblers only.** Zero prompt text in source code. All instructional content stored in GCS and loaded at runtime. This enables:
- Prompt tuning without redeployment
- A/B testing via GCS file changes only
- Semver-based prompt versioning (e.g., `fiscal-lens-v1.2.0.yaml`)

### Tiered Analysis Profiles

Three LLM passes with different model tiers:

| Pass | Model | Applies To | Analysis Type |
|------|-------|-----------|---------------|
| Pass 1 | Haiku | All bills | Extraction, classification, summary |
| Pass 2 | Sonnet | Filtered bills | Pork detection, impact analysis, stance, fiscal |
| Pass 3 | Opus | Rare/flagged bills | Ambiguity resolution, cross-bill analysis |

Each pass has its own prompt profile — different combination of instruction blocks assembled differently. Bill-analysis-pipeline selects profile based on pass routing rules.

### Bill Text Decomposition Support

Bill text decomposition (parsing, embedding, clustering) owned by Component 10 using local, free tools (Ollama sidecar, DJL/ONNX, Smile). Only **concept simplification** step calls external LLM using prompts from this component.

```
Raw bill text (any format: XML, plain text, PDF-extracted)
    |
    v
Component 10: Text parsing / section identification (Ollama sidecar)
    |
    v
Component 10: Section embedding (DJL + ONNX, 384-dim)
    |
    v
Component 10: Semantic clustering (Smile k-means/DBSCAN)
    |
    v
Component 10 calls LLM with Component 8 prompt: "Simplify this concept group" (Haiku)
    |
    v
Simplified concept summaries → persisted to bill_concept_groups
    |
    v
Component 10 calls LLM with Component 8 prompt: Pass 1/2/3 analysis (using simplified concepts as context)
    |
    v
Structured analysis output → persisted to analysis layer tables
```

Prompt engine provides **decomposition and analysis profiles**:

| Profile | Purpose | Used By |
|---------|---------|---------|
| `concept-simplification` | Simplify related bill sections into coherent summary | Component 10 decomposition step (Haiku) |
| `section-classification` | Classify bill section by topic/policy area — fallback for insufficient embedding clustering | Component 10 grouping step (Haiku, rare) |
| `pass1-extraction` | Full extraction/classification/summary from simplified concepts | Component 10 Pass 1 |
| `pass2-deep-analysis` | Pork, impact, stance, fiscal analysis from simplified concepts | Component 10 Pass 2 |
| `pass3-ambiguity-resolution` | Cross-concept ambiguity resolution | Component 10 Pass 3 |

Component 10 owns decomposition *logic* and *orchestration*; Component 8 provides decomposition *prompts*. All instructional content in GCS, orchestration in pipeline.

### Base Traits (from Component 1 §1.7)

| Type | Role |
|------|------|
| `PromptStage` | Enum: System, Persona, Lens, Context, Guardrails, Output, Custom |
| `InstructionBlock` | Atomic prompt fragment: name, stage, weight, version, content |
| `StageConfig` | Stage + block names + weight for profile entry |
| `PromptProfile` | Named chain of `StageConfig` entries |
| `ChainAssembler` | Trait: order stages, apply weights, merge blocks, inject context |
| `WeightTranslator` | Convert weight (0.0-1.0) to emphasis markers |

### Output Schemas (from Component 1 §1.6)

Referenced by name in Output stage blocks (tell LLM what JSON to return):

| Schema | Used By |
|--------|---------|
| `BillSummaryOutput` | Pass 1 |
| `TopicClassificationOutput` | Pass 1 |
| `StanceClassificationOutput` | Pass 2 |
| `PorkDetectionOutput` | Pass 2 |
| `ImpactAnalysisOutput` | Pass 2 |
| `FiscalEstimateOutput` | Pass 2 |

Prompt engine references by name in GCS; LLM adapter uses actual schema types for response parsing.

### GCS Layout

```
gs://repcheck-prompt-configs/
  └── bills/
      ├── blocks/
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
      │   │   ├── bill-text-v1.0.0.yaml
      │   │   ├── amendments-v1.0.0.yaml
      │   │   └── related-bills-v1.0.0.yaml
      │   ├── guardrails/
      │   │   ├── nonpartisan-constraint-v1.0.0.yaml
      │   │   └── accuracy-constraint-v1.0.0.yaml
      │   └── output/
      │       ├── pass1-extraction-schema-v1.0.0.yaml
      │       ├── pass2-deep-analysis-schema-v1.0.0.yaml
      │       └── pass3-resolution-schema-v1.0.0.yaml
      └── profiles/
          ├── concept-simplification-v1.0.0.yaml
          ├── section-classification-v1.0.0.yaml
          ├── pass1-extraction-v1.0.0.yaml
          ├── pass2-deep-analysis-v1.0.0.yaml
          ├── pass3-ambiguity-resolution-v1.0.0.yaml
          ├── summary-only-v1.0.0.yaml
          └── pork-detection-v1.0.0.yaml
```

### Deployment Pipeline

```
repo: prompt-configs/bills/  ── git push ──→  GitHub Actions  ──→  gs://repcheck-prompt-configs/bills/
```

Blocks and profiles version-controlled in repo under `prompt-configs/bills/`. GitHub Actions deploys to GCS on merge. Prompt engine reads from GCS at runtime. Local file fallback for development.

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 8.1 GCS Block Loader | New | Loads instruction blocks and profiles from GCS with version filtering and local fallback |
| 8.2 Bill Analysis Profiles | New | Decomposition and analysis profiles, bill-specific block catalog |
| 8.3 Bill Prompt Assembler | New | Bill-specific `ChainAssembler` implementation with context injection |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck-prompt-engine-bills/
└── repcheck.prompt.bills
    ├── loader
    │   ├── BlockLoader
    │   └── ProfileLoader
    ├── profiles
    │   └── BillAnalysisProfiles
    ├── assembler
    │   ├── BillPromptAssembler
    │   └── BillContextInjector
    ├── config
    │   └── BillPromptEngineConfig
    └── errors
        ├── BlockLoadFailed
        ├── ProfileLoadFailed
        └── ContextInjectionFailed
```

### Dependencies

```
repcheck-prompt-engine-bills
├── repcheck-shared-models (Component 1)
│   ├── PromptStage, InstructionBlock, StageConfig, PromptProfile
│   ├── ChainAssembler, WeightTranslator
│   └── Output schemas (referenced by name only)
└── GCS Java SDK (wrapped in Sync[F])
```

**No dependency on pipeline-models or ingestion-common.** Pure library with only `shared-models` as RepCheck dependency.

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | `ChainAssembler` integration, weight translation, context injection, profile validation | MockitoScala |
| GCS integration tests | Block loading, version filtering, profile resolution | Testcontainers (fake GCS) or local file fallback |
| Prompt assembly tests | Full profile assembly with real blocks, verify prompt structure and content ordering | Local file fallback |
| Contract tests | Assembled prompts contain expected output schema references | Unit tests |