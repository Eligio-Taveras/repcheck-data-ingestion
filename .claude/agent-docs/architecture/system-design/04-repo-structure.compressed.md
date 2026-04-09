<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/system-design/04-repo-structure.md -->

# Repository & Project Structure

Each major component is its own Git repository. Sub-items within a repository are SBT projects (multi-project builds). Shared dependencies are published as versioned library artifacts.

```mermaid
graph TB
    subgraph "repcheck-shared-models (repo)"
        SM_MODELS["shared-models<br/>─────────────<br/>Legislative DTOs/DOs,<br/>User DTOs/DOs,<br/>LLM output schemas,<br/>Prompt chain base traits,<br/>Serializers, Constants"]
    end

    subgraph "repcheck-pipeline-models (repo)"
        PM_MODELS["pipeline-models<br/>─────────────<br/>Pub/Sub event schemas,<br/>Pipeline job metadata,<br/>Publisher/Subscriber helpers,<br/>Pipeline config types,<br/>AlloyDB table name constants"]
    end

    subgraph "repcheck-data-ingestion (repo)"
        DI_BILLS["bills-pipeline<br/>(SBT project)<br/>─────────────<br/>Bill ingestion,<br/>text link fetching"]
        DI_VOTES["votes-pipeline<br/>(SBT project)<br/>─────────────<br/>Roll call votes,<br/>member positions"]
        DI_MEMBERS["members-pipeline<br/>(SBT project)<br/>─────────────<br/>Congress member<br/>profile sync"]
        DI_AMENDMENTS["amendments-pipeline<br/>(SBT project)<br/>─────────────<br/>Amendment ingestion,<br/>linked to bills"]
        DI_SHARED["ingestion-common<br/>(SBT project)<br/>─────────────<br/>PagingApiBase,<br/>Pub/Sub publisher,<br/>Congress.gov API utils"]
    end

    subgraph "repcheck-prompt-engine-bills (repo)"
        PEB["prompt-engine-bills<br/>─────────────<br/>Bill analysis prompt<br/>composition pipeline<br/>GCS instruction loader<br/>Stage chain assembly"]
    end

    subgraph "repcheck-prompt-engine-users (repo)"
        PEU["prompt-engine-users<br/>─────────────<br/>User preference prompt<br/>composition pipeline<br/>GCS instruction loader<br/>Stage chain assembly"]
    end

    subgraph "repcheck-llm-analysis (repo)"
        LLM_ADAPTER["llm-adapter<br/>(SBT project)<br/>─────────────<br/>LlmProvider trait,<br/>Claude/Gemini/OpenAI<br/>implementations"]
        LLM_PIPELINE["bill-analysis-pipeline<br/>(SBT project)<br/>─────────────<br/>Bill text analysis,<br/>structured output parsing,<br/>Pub/Sub subscriber"]
    end

    subgraph "repcheck-scoring-engine (repo)"
        SC_PIPELINE["scoring-pipeline<br/>(SBT project)<br/>─────────────<br/>Alignment scoring,<br/>batch computation"]
        SC_CACHE["score-cache<br/>(SBT project)<br/>─────────────<br/>Pre-computed score<br/>writes to AlloyDB"]
    end

    subgraph "repcheck-api-server (repo — future)"
        API["api-server<br/>─────────────<br/>Http4s REST API"]
    end

    DI_SHARED --> PM_MODELS
    DI_SHARED --> SM_MODELS
    DI_BILLS --> DI_SHARED
    DI_VOTES --> DI_SHARED
    DI_MEMBERS --> DI_SHARED
    DI_AMENDMENTS --> DI_SHARED

    LLM_PIPELINE --> PM_MODELS
    SC_PIPELINE --> PM_MODELS

    PEB --> SM_MODELS
    PEU --> SM_MODELS

    LLM_ADAPTER --> SM_MODELS
    LLM_PIPELINE --> SM_MODELS
    LLM_PIPELINE --> LLM_ADAPTER
    LLM_PIPELINE --> PEB

    SC_PIPELINE --> SM_MODELS
    SC_PIPELINE --> LLM_ADAPTER
    SC_PIPELINE --> PEU
    SC_CACHE --> SM_MODELS
    SC_PIPELINE --> SC_CACHE

    API --> SM_MODELS
```

## Repository Summary

| Repository | SBT Projects | Purpose |
|---|---|---|
| **repcheck-shared-models** | `shared-models` | Published library: legislative DTOs/DOs, user DTOs/DOs, LLM output schemas, prompt chain base traits, serializers |
| **repcheck-pipeline-models** | `pipeline-models` | Published library: Pub/Sub event schemas, pipeline job metadata/status, publisher/subscriber helpers, pipeline config types, AlloyDB table name constants |
| **repcheck-data-ingestion** | `ingestion-common`, `bills-pipeline`, `votes-pipeline`, `members-pipeline`, `amendments-pipeline` | Data pipelines fetching from Congress.gov API |
| **repcheck-prompt-engine-bills** | `prompt-engine-bills` | Bill analysis prompt composition, GCS block loading |
| **repcheck-prompt-engine-users** | `prompt-engine-users` | User scoring prompt composition, GCS block loading |
| **repcheck-llm-analysis** | `llm-adapter`, `bill-analysis-pipeline` | Pluggable LLM providers + bill analysis pipeline |
| **repcheck-scoring-engine** | `scoring-pipeline`, `score-cache` | Alignment scoring + AlloyDB score caching |
| **repcheck-api-server** | `api-server` | REST API (future phase) |

## Cross-Repo Dependency Management

- **`repcheck-shared-models`** and **`repcheck-pipeline-models`** published as versioned artifacts (GitHub Packages or GCS Maven repo)
- `repcheck-shared-models` has no repcheck dependencies (root domain library)
- `repcheck-pipeline-models` has no repcheck dependencies (root operational library)
- Pipeline repos depend on both `repcheck-shared-models` (domain types) and `repcheck-pipeline-models` (operational types)
- Non-pipeline repos depend only on `repcheck-shared-models`
- `repcheck-llm-analysis` depends on `repcheck-prompt-engine-bills` as published artifact
- `repcheck-scoring-engine` depends on `repcheck-llm-analysis` (for adapter) and `repcheck-prompt-engine-users` as published artifacts
- Artifact publishing automated via GitHub Actions on tagged releases