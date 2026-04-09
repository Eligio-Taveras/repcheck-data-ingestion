# Acceptance Criteria — Master Index

## What These Are

Acceptance criteria are the **authoritative implementation spec** for each component of the RepCheck system. They bridge the gap between system design (which describes intent and architecture) and implementation (which is code). Each component's acceptance criteria define:

- **Every class, trait, and method signature** the component must expose
- **Behavioral rules** as numbered steps (e.g., "1. Fetch detail, 2. Convert to DO, 3. Detect change...")
- **Constructor dependencies** — what gets injected and from which component
- **Key design decisions** with rationale (e.g., why `ON CONFLICT DO NOTHING` instead of check-then-insert)
- **Acceptance criteria tables** — each row is a testable expectation with a concrete test description
- **Cross-component references** (e.g., "per Component 3 §3.6") linking to the exact area file that defines the contract

## How Agents Should Use These

### When implementing a component:
1. Read this index to find the component
2. Read the component's **index file** (e.g., `04-BILLS-PIPELINE.md`) for system context, data flow diagram, package structure, dependency tree, and testing strategy
3. The index file contains a **Component Routing Table** — use it to find the specific area file for the class you're implementing
4. Each **area file** (e.g., `04.3-bill-metadata-processing.md`) is self-contained: class signatures, behavior spec, design decisions, and acceptance criteria table
5. When an area file references another component (e.g., "uses `PlaceholderCreator` from Component 3 §3.6"), follow the cross-reference to understand the contract you're coding against

### When writing tests:
- The acceptance criteria table in each area file defines every test case. Each row is one test.
- Column 1 is the criterion (what must be true). Column 2 is the test (how to verify it).

### Relationship to other documentation:
- **System design** (`docs/architecture/system-design/`) describes the *why* and *what* at a high level. Read it for context when onboarding.
- **Behavioral specs** (`docs/architecture/BEHAVIORAL_SPECS.md`) define cross-cutting rules (change detection strategy, event emission conditions, scoring logic). Acceptance criteria reference these but do not duplicate them.
- **Templates** (`docs/templates/`) provide Scala code patterns and skeletons. Acceptance criteria define *what* to build; templates show *how* to write it in this codebase's style.
- **Acceptance criteria** are the refined, cross-checked spec. Where they conflict with system design, acceptance criteria win — they were written later with more precision and cross-component consistency auditing.

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

Components build on each other. When implementing a component, you may need to reference area files from its dependencies:

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

Common cross-references you'll encounter:
- "Component 1 §1.2" → `01-shared-models/01.2-legislative-dos.md` (DO definitions)
- "Component 2 §2.1" → `02-pipeline-models/02.1-inter-pipeline-communication.md` (event payloads)
- "Component 3 §3.3" → `03-ingestion-common/03.3-change-detection.md` (ChangeDetector)
- "Component 3 §3.6" → `03-ingestion-common/03.6-placeholder-entities.md` (PlaceholderCreator)
- "Component 3 §3.7" → `03-ingestion-common/03.7-execution-helpers.md` (PipelineBootstrap)

## Numbering Convention

Each component has a two-digit number (01–12). Area files within a component use `{component}.{area}` numbering (e.g., `04.3` = Component 4, Area 3). Cross-references use the format "Component N §N.M" which maps to file `{NN}-{folder}/{NN}.{M}-{name}.md`.
