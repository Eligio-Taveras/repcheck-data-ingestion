<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/system-design/11-implementation-priority.md -->

# Implementation Priority (This Phase)

## Phase 1: Foundation
1. **Create `repcheck-shared-models` repo** — Legislative DTOs/DOs, user DTOs/DOs, LLM output schemas, prompt chain base traits (`InstructionBlock`, `PromptProfile`, `ChainAssembler`, weight translation), serializers. Publish as versioned artifact.
2. **Create `repcheck-pipeline-models` repo** — Pub/Sub event schemas, publisher/subscriber helpers, pipeline job metadata/status types, pipeline config types, AlloyDB table name constants. Publish as versioned artifact.
3. **CI/CD for artifact publishing** — GitHub Actions to publish both model repos to GitHub Packages on tagged releases.

## Phase 2: Data Ingestion
4. **Create `repcheck-data-ingestion` repo** — Multi-project SBT build with `ingestion-common` project. Migrate existing `PagingApiBase` and Congress.gov API code from current `repcheck` repo.
5. **`bills-pipeline`** — Refactor existing bill pipeline into its own SBT project, add `bill.text.available` event.
6. **`members-pipeline`** — New SBT project for Congress.gov member API.
7. **`votes-pipeline`** — New SBT project for roll call votes + per-member positions.
8. **`amendments-pipeline`** — New SBT project for amendments.

## Phase 3: Prompt Engines + LLM Analysis
9. **Create `repcheck-prompt-engine-bills` repo** — GCS block loader, chain assembler, initial blocks (system, persona, lenses, guardrails, output format), prompt profiles for full-analysis and pork-detection.
10. **Create `repcheck-prompt-engine-users` repo** — Same architecture, user-specific blocks and profiles.
11. **GitHub Actions for prompt config deployment** — CI pipeline per prompt-engine repo to push configs to GCS on merge.
12. **Create `repcheck-llm-analysis` repo** — `llm-adapter` project with `LlmProvider` trait + Claude implementation. `bill-analysis-pipeline` project using `repcheck-prompt-engine-bills` for prompt assembly.
13. **Additional LLM providers** — Gemini and OpenAI implementations in `llm-adapter`.

## Phase 4: Scoring
14. **AlloyDB setup** — User tables, preference tables, Q&A response tables, bill embeddings (pgvector).
15. **Create `repcheck-scoring-engine` repo** — `scoring-pipeline` project using `repcheck-prompt-engine-users` for prompt assembly. `score-cache` project for AlloyDB writes.
16. **Create `repcheck-api-server` repo** — Http4s REST API (future phase, scaffold only).