<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/09-PROMPT-ENGINE-USERS.md -->

# Acceptance Criteria: Component 9 — Prompt Engine Users

Standalone library composing LLM prompts for user-legislator alignment scoring. Loads versioned instruction blocks from GCS, assembles them into prompt chains using profiles, and injects dynamic user/legislator context at runtime. **Library, not pipeline** — consumed by scoring-pipeline (Component 11). **Depends on**: `repcheck-shared-models` (Component 1 — base traits §1.7, output schemas §1.6).

## System Context

Assembles prompt strings (does NOT call LLM — scoring pipeline's job). Loads instruction blocks from GCS by stage, resolves profile (which blocks, order, weights), injects dynamic context (user preferences, legislator voting, bill analyses), applies weight translation (emphasis markers), outputs final prompt string.

```
GCS (instruction blocks + profiles)
    |
    v
Prompt Engine Users (this component)
    |-- Load instruction blocks from GCS by stage (system, persona, lens, etc.)
    |-- Resolve a profile (which blocks to use, in what order, with what weights)
    |-- Inject dynamic context (user preferences, legislator voting record, bill analyses)
    |-- Apply weight translation (emphasis markers)
    |-- Assemble final prompt string
    |
    v
Assembled prompt string → scoring-pipeline (Component 11) → LLM adapter → LLM
```

### Relationship to Component 8

Components 8 and 9 share identical assembly mechanism (base traits from Component 1 §1.7). Differences:

| Aspect | Component 8 (Bills) | Component 9 (Users) |
|--------|---------------------|---------------------|
| GCS path | `bills/` | `users/` |
| Context types | `BillAnalysisContext`, `ConceptGroup`, amendments | `UserScoringContext`, preferences, voting records |
| Profiles | `pass1-extraction`, `pass2-deep-analysis`, `pass3-ambiguity` | `full-alignment`, `topic-breakdown`, `quick-score` |
| Consumer | Bill-analysis-pipeline (Component 10) | Scoring-pipeline (Component 11) |
| Output schemas | `BillSummaryOutput`, `PorkDetectionOutput`, etc. | `AlignmentScoreOutput` (§1.6) |

### No Hardcoded Prompts

All prompt fragments live in GCS. Prompt engines are loaders + assemblers only.

### Scoring Profiles

| Profile | Model | Purpose |
|---------|-------|---------|
| `full-alignment` | Sonnet | Comprehensive topic-by-topic alignment with reasoning, highlights, per-congress breakdown |
| `topic-breakdown` | Haiku | Lightweight topic-level scoring without detailed reasoning — faster/cheaper for batch re-scoring |
| `quick-score` | Haiku | Single aggregate score with minimal explanation — rapid feedback when score delta small |

Profile selection is Component 11's responsibility based on score-delta thresholds.

### Base Traits (from Component 1 §1.7)

| Type | Role |
|------|------|
| `PromptStage` | Enum: System, Persona, Lens, Context, Guardrails, Output, Custom |
| `InstructionBlock` | Atomic prompt fragment: name, stage, weight, version, content |
| `StageConfig` | Stage + block names + weight for profile entry |
| `PromptProfile` | Named chain of `StageConfig` entries |
| `ChainAssembler` | Orders stages, applies weights, merges blocks, injects context |
| `WeightTranslator` | Converts weight (0.0-1.0) to emphasis markers |

### Output Schema (from Component 1 §1.6)

`AlignmentScoreOutput`: topicScores (List[TopicAlignmentScore]), overallScore (Double), highlights (List[AlignmentHighlight]), reasoning (String). Prompt engine references by name in Output stage blocks stored in GCS.

### GCS Layout

```
gs://repcheck-prompt-configs/
  └── users/
      ├── blocks/
      │   ├── system/
      │   │   └── base-scoring-analyst-v1.0.0.yaml
      │   ├── persona/
      │   │   ├── plain-language-explainer-v1.0.0.yaml
      │   │   └── data-driven-analyst-v1.0.0.yaml
      │   ├── lens/
      │   │   ├── topic-alignment-lens-v1.0.0.yaml
      │   │   ├── voting-consistency-lens-v1.0.0.yaml
      │   │   └── bipartisan-lens-v1.0.0.yaml
      │   ├── context/
      │   │   ├── user-preferences-v1.0.0.yaml
      │   │   ├── voting-record-v1.0.0.yaml
      │   │   ├── bill-analyses-v1.0.0.yaml
      │   │   └── legislator-profile-v1.0.0.yaml
      │   ├── guardrails/
      │   │   ├── fairness-constraint-v1.0.0.yaml
      │   │   └── no-party-bias-v1.0.0.yaml
      │   └── output/
      │       ├── full-alignment-schema-v1.0.0.yaml
      │       ├── topic-breakdown-schema-v1.0.0.yaml
      │       └── quick-score-schema-v1.0.0.yaml
      └── profiles/
          ├── full-alignment-v1.0.0.yaml
          ├── topic-breakdown-v1.0.0.yaml
          └── quick-score-v1.0.0.yaml
```

### Deployment Pipeline

```
repo: prompt-configs/users/  ── git push ──→  GitHub Actions  ──→  gs://repcheck-prompt-configs/users/
```

Blocks and profiles version-controlled in repo, deployed to GCS on merge. Engine reads from GCS at runtime with local file fallback for development.

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 9.1 GCS Block Loader | New | Loads instruction blocks and profiles from GCS with version filtering and local fallback |
| 9.2 User Scoring Profiles | New | Scoring profiles and user-specific block catalog |
| 9.3 User Prompt Assembler | New | User-specific `ChainAssembler` with context injection for preferences, voting records, bill analyses |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Loading instruction blocks and profiles from GCS | [9.1 GCS Block Loader](09-prompt-engine-users/09.1-gcs-block-loader.md) |
| Scoring profile definitions, block catalog | [9.2 User Scoring Profiles](09-prompt-engine-users/09.2-user-scoring-profiles.md) |
| User-specific prompt assembly with context injection | [9.3 User Prompt Assembler](09-prompt-engine-users/09.3-user-prompt-assembler.md) |

## Cross-Cutting Concerns

### Package Structure

```
repcheck-prompt-engine-users/
└── repcheck.prompt.users
    ├── loader
    │   ├── BlockLoader
    │   └── ProfileLoader
    ├── profiles
    │   └── UserScoringProfiles
    ├── assembler
    │   ├── UserPromptAssembler
    │   └── UserContextInjector
    ├── config
    │   └── UserPromptEngineConfig
    └── errors
        ├── BlockLoadFailed
        ├── ProfileLoadFailed
        └── ContextInjectionFailed
```

### Dependencies

```
repcheck-prompt-engine-users
├── repcheck-shared-models (Component 1)
│   ├── PromptStage, InstructionBlock, StageConfig, PromptProfile (§1.7)
│   ├── ChainAssembler, WeightTranslator (§1.7)
│   └── AlignmentScoreOutput, TopicAlignmentScore, etc. (§1.6)
└── GCS Java SDK (Sync[F] wrapped)
```

No dependency on pipeline-models or ingestion-common. Pure library — no Pub/Sub, Doobie, or pipeline execution infrastructure.

### Testing Strategy

| Test Type | Scope | Infrastructure |
|-----------|-------|---------------|
| Unit tests | `ChainAssembler` integration, weight translation, context injection, profile validation | MockitoScala (mock GCS) |
| GCS integration tests | Block loading, version filtering, profile resolution | Testcontainers (fake GCS) or local file fallback |
| Prompt assembly tests | Full profile assembly with real blocks → verify prompt structure and content ordering | Local file fallback |
| Contract tests | Assembled prompts contain expected output schema references | Unit tests |