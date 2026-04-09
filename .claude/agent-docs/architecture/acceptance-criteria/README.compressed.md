<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/README.md -->

# Acceptance Criteria — Master Index

## What These Are

Acceptance criteria are the **authoritative implementation spec** for each component. They define every class, trait, and method signature; behavioral rules as numbered steps; constructor dependencies; key design decisions with rationale; acceptance criteria tables (each row = one testable expectation); and cross-component references linking to exact area files.

## How Agents Should Use These

**When implementing a component:**
1. Read this index to find the component
2. Read the component's **index file** (e.g., `04-BILLS-PIPELINE.md`) for system context, data flow diagram, package structure, dependency tree, and testing strategy
3. Use the index file's **Component Routing Table** to find the specific area file for the class you're implementing
4. Each **area file** is self-contained: class signatures, behavior spec, design decisions, acceptance criteria table
5. Follow cross-references (e.g., "uses `PlaceholderCreator` from Component 3 §3.6") to understand contracts

**When writing tests:** The acceptance criteria table in each area file defines every test case. Each row = one test. Column 1 = criterion (what must be true). Column 2 = test (how to verify).

**Relationship to other documentation:**
- **System design** (`docs/architecture/system-design/`) provides high-level context
- **Behavioral specs** (`docs/architecture/BEHAVIORAL_SPECS.md`) define cross-cutting rules (change detection, event emission, scoring). Acceptance criteria reference but do not duplicate.
- **Templates** (`docs/templates/`) provide code patterns; acceptance criteria define *what*, templates show *how*
- **Acceptance criteria are authoritative.** Where they conflict with system design, acceptance criteria win.

---

## Component Map

### Phase 1: Foundation

| # | Component | Repository / Module | Index File | Status |
|---|-----------|-------------------|------------|--------|
| 1 | Shared Models | `repcheck-shared-models` | [01-SHARED-MODELS.md](01-SHARED-MODELS.md) | Complete |
| 2 | Pipeline Models | `repcheck-pipeline-models` | [02-PIPELINE-MODELS.md](02-PIPELINE-MODELS.md) | Complete |

### Phase 2: Data Ingestion

| # | Component | Repository / Module | Index File | Status |
|---|-----------|-------------------|------------|--------|
| 3 | Ingestion Common | `repcheck-data-ingestion / ingestion-common` | [03-INGESTION-COMMON.md](03-INGESTION-COMMON.md) | Complete |
| 4 | Bills Pipeline | `repcheck-data-ingestion / bill-*` (3 projects) | [04-BILLS-PIPELINE.md](04-BILLS-PIPELINE.md) | Complete |
| 5 | Members Pipeline | `repcheck-data-ingestion / member-*` (2 projects) | [05-MEMBERS-PIPELINE.md](05-MEMBERS-PIPELINE.md) | Complete |
| 6 | Votes Pipeline | `repcheck-data-ingestion / votes-pipeline` | [06-VOTES-PIPELINE.md](06-VOTES-PIPELINE.md) | Complete |
| 7 | Amendments Pipeline | `repcheck-data-ingestion / amendments-pipeline` | [07-AMENDMENTS-PIPELINE.md](07-AMENDMENTS-PIPELINE.md) | Complete |

### Phase 3: Prompt Engines + LLM Analysis

| # | Component | Repository / Module | Index File | Status |
|---|-----------|-------------------|------------|--------|
| 8 | Prompt Engine — Bills | `repcheck-prompt-engine-bills` | [08-PROMPT-ENGINE-BILLS.md](08-PROMPT-ENGINE-BILLS.md) | Complete |
| 9 | Prompt Engine — Users | `repcheck-prompt-engine-users` | [09-PROMPT-ENGINE-USERS.md](09-PROMPT-ENGINE-USERS.md) | Complete |
| 10 | LLM Analysis | `repcheck-llm-analysis` (llm-adapter + bill-analysis-pipeline) | [10-LLM-ANALYSIS.md](10-LLM-ANALYSIS.md) | Complete |

### Phase 4: Scoring + API

| # | Component | Repository / Module | Index File | Status |
|---|-----------|-------------------|------------|--------|
| 11 | Scoring Engine | `repcheck-scoring-engine` (scoring-pipeline + score-cache + stance-materializer) | [11-SCORING-ENGINE.md](11-SCORING-ENGINE.md) | Complete |
| 12 | API Server | `repcheck-api-server` | — | Future Phase |

---

## Cross-Component Dependencies

```
Component 1 (shared-models) ─── no dependencies
Component 2 (pipeline-models) ─── no dependencies
Component 3 (ingestion-common) ─── depends on 1, 2
Component 4 (bills-pipeline) ─── depends on 1, 2, 3
Component 5 (members-pipeline) ─── depends on 1, 2, 3
Component 6 (votes-pipeline) ─── depends on 1, 2, 3
Component 7 (amendments-pipeline) ─── depends on 1, 2, 3
Component 8 (prompt-engine-bills) ─── depends on 1
Component 9 (prompt-engine-users) ─── depends on 1
Component 10 (llm-analysis) ─── depends on 1, 2, 8
Component 11 (scoring-engine) ─── depends on 1, 2, 9, 10
```

**Common cross-references:**
- "Component 1 §1.2" → `01-SHARED-MODELS/01.2-legislative-dos.md` (DO definitions)
- "Component 2 §2.1" → `02-PIPELINE-MODELS/02.1-inter-pipeline-communication.md` (event payloads)
- "Component 3 §3.3" → `03-INGESTION-COMMON/03.3-change-detection.md` (ChangeDetector)
- "Component 3 §3.6" → `03-INGESTION-COMMON/03.6-placeholder-entities.md` (PlaceholderCreator)
- "Component 3 §3.7" → `03-INGESTION-COMMON/03.7-execution-helpers.md` (PipelineBootstrap)

## Numbering Convention

Components use two-digit numbers (01–12). Area files use `{component}.{area}` numbering (e.g., `04.3` = Component 4, Area 3). Cross-references use "Component N §N.M" → file `{NN}-{folder}/{NN}.{M}-{name}.md`.