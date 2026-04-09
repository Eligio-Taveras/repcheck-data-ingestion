# Acceptance Criteria: Component 9 — Prompt Engine Users

> Standalone library (`repcheck-prompt-engine-users`) that composes LLM prompts for user-legislator alignment scoring.
> Loads versioned instruction blocks from GCS, assembles them into prompt chains using profiles, and injects dynamic user/legislator context at runtime.
> This is a **library**, not a pipeline — it is consumed by the scoring-pipeline (Component 11).
> **Depends on**: `repcheck-shared-models` (Component 1 — base traits from §1.7, output schemas from §1.6).

---

## System Context

### What This Component Does

The user prompt engine is a **composable instruction framework** for scoring prompts. It does NOT call the LLM — that is the scoring pipeline's job (Component 11). The prompt engine's sole responsibility is assembling the prompt string that gets sent to the LLM.

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

Components 8 and 9 share the **identical assembly mechanism** — both build on the base traits from Component 1 §1.7 (`ChainAssembler`, `WeightTranslator`, `InstructionBlock`, `PromptProfile`). The differences are:

| Aspect | Component 8 (Bills) | Component 9 (Users) |
|--------|---------------------|---------------------|
| GCS path | `bills/` | `users/` |
| Context types | `BillAnalysisContext`, `ConceptGroup`, amendments | `UserScoringContext`, preferences, voting records |
| Profiles | `pass1-extraction`, `pass2-deep-analysis`, `pass3-ambiguity` | `full-alignment`, `topic-breakdown`, `quick-score` |
| Consumer | Bill-analysis-pipeline (Component 10) | Scoring-pipeline (Component 11) |
| Output schemas | `BillSummaryOutput`, `PorkDetectionOutput`, etc. | `AlignmentScoreOutput` (§1.6) |

### No Hardcoded Prompts

Per CLAUDE.md universal rules: **all prompt fragments live in GCS. Prompt engines are loaders + assemblers only.** The prompt engine contains zero prompt text in its source code.

### Scoring Profiles

The scoring-pipeline (Component 11) uses different profiles depending on the scoring scenario:

| Profile | Model | Purpose | When Used |
|---------|-------|---------|-----------|
| `full-alignment` | Sonnet | Comprehensive alignment scoring — topic-by-topic analysis with reasoning, highlights, and per-congress breakdown | Default for all scoring runs |
| `topic-breakdown` | Haiku | Lightweight topic-level scoring without detailed reasoning — faster and cheaper for re-scoring many pairs at once | Batch re-scoring when many users need updated scores |
| `quick-score` | Haiku | Single aggregate score with minimal explanation — for rapid feedback when score delta is small | When the score change between runs is below a configured threshold |

> **Profile selection is Component 11's responsibility.** The prompt engine assembles whatever profile is requested. Component 11 decides which profile to use based on the score delta from the previous run and cost considerations. In the batch scoring model, profile selection is not driven by trigger event type — it is driven by score-delta thresholds (large change → richer profile, small change → quick profile).

### Base Traits (from Component 1 §1.7)

| Type | Source | Role |
|------|--------|------|
| `PromptStage` | §1.7 | Enum: System, Persona, Lens, Context, Guardrails, Output, Custom |
| `InstructionBlock` | §1.7 | Atomic prompt fragment: name, stage, weight, version, content |
| `StageConfig` | §1.7 | Stage + block names + weight for a profile entry |
| `PromptProfile` | §1.7 | Named chain of `StageConfig` entries defining a complete prompt |
| `ChainAssembler` | §1.7 | Trait that orders stages, applies weights, merges blocks, injects context |
| `WeightTranslator` | §1.7 | Converts weight (0.0-1.0) to emphasis markers in prompt text |

### Output Schema (from Component 1 §1.6)

The prompt engine references the `AlignmentScoreOutput` schema in its Output stage blocks:

| Schema | Fields | Used By |
|--------|--------|---------|
| `AlignmentScoreOutput` | topicScores: List[TopicAlignmentScore], overallScore: Double, highlights: List[AlignmentHighlight], reasoning: String | All scoring profiles |

The prompt engine does not import this schema directly — it references it by name in Output stage blocks stored in GCS. The scoring pipeline (Component 11) uses the actual schema type for response parsing.

### GCS Layout

```
gs://repcheck-prompt-configs/
  └── users/
      ├── blocks/                          # Individual instruction blocks
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
      │   │   ├── user-preferences-v1.0.0.yaml       # placeholder: {{user_preferences}}
      │   │   ├── voting-record-v1.0.0.yaml           # placeholder: {{voting_record}}
      │   │   ├── bill-analyses-v1.0.0.yaml           # placeholder: {{bill_analyses}}
      │   │   └── legislator-profile-v1.0.0.yaml      # placeholder: {{legislator_profile}}
      │   ├── guardrails/
      │   │   ├── fairness-constraint-v1.0.0.yaml
      │   │   └── no-party-bias-v1.0.0.yaml
      │   └── output/
      │       ├── full-alignment-schema-v1.0.0.yaml
      │       ├── topic-breakdown-schema-v1.0.0.yaml
      │       └── quick-score-schema-v1.0.0.yaml
      └── profiles/                        # Assembled prompt profiles
          ├── full-alignment-v1.0.0.yaml
          ├── topic-breakdown-v1.0.0.yaml
          └── quick-score-v1.0.0.yaml
```

### Deployment Pipeline

```
repo: prompt-configs/users/  ── git push ──→  GitHub Actions  ──→  gs://repcheck-prompt-configs/users/
```

- Instruction blocks and profiles are version-controlled in the repo under `prompt-configs/users/`
- GitHub Actions deploys updated configs to GCS on merge to main
- The prompt engine reads from GCS at runtime
- Local file fallback for development (reads from `prompt-configs/users/` on filesystem)

---

## Implementation Areas

| Area | Status | Description |
|------|--------|-------------|
| 9.1 GCS Block Loader | New | Loads instruction blocks and profiles from GCS with version filtering and local fallback (same mechanism as Component 8 §8.1, different config path) |
| 9.2 User Scoring Profiles | New | Defines scoring profiles and the user-specific block catalog |
| 9.3 User Prompt Assembler | New | User-specific `ChainAssembler` implementation with context injection for user preferences, voting records, and bill analyses |

## Component Routing Table

| Task | Area File |
|------|-----------|
| Loading instruction blocks and profiles from GCS | [9.1 GCS Block Loader](09-prompt-engine-users/09.1-gcs-block-loader.md) |
| Scoring profile definitions, block catalog | [9.2 User Scoring Profiles](09-prompt-engine-users/09.2-user-scoring-profiles.md) |
| User-specific prompt assembly with context injection | [9.3 User Prompt Assembler](09-prompt-engine-users/09.3-user-prompt-assembler.md) |

---

## Cross-Cutting Concerns

### Package Structure

```
repcheck-prompt-engine-users/
└── repcheck.prompt.users
    ├── loader
    │   ├── BlockLoader                    (9.1)
    │   └── ProfileLoader                  (9.1)
    ├── profiles
    │   └── UserScoringProfiles            (9.2)
    ├── assembler
    │   ├── UserPromptAssembler            (9.3)
    │   └── UserContextInjector            (9.3)
    ├── config
    │   └── UserPromptEngineConfig         (9.1)
    └── errors
        ├── BlockLoadFailed                (9.1)
        ├── ProfileLoadFailed              (9.1)
        └── ContextInjectionFailed         (9.3)
```

### Dependencies

```
repcheck-prompt-engine-users
├── repcheck-shared-models               (published artifact — Component 1)
│   ├── PromptStage, InstructionBlock, StageConfig, PromptProfile   (§1.7)
│   ├── ChainAssembler, WeightTranslator                            (§1.7)
│   └── AlignmentScoreOutput, TopicAlignmentScore, etc.             (§1.6, referenced by name only)
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
