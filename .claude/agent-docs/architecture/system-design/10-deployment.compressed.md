<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/system-design/10-deployment.md -->

# Deployment Architecture

```mermaid
graph TB
    subgraph "GCP Project: repcheck-421801"
        subgraph "Cloud Run Jobs (from repcheck-data-ingestion)"
            JOB_BILLS["bills-pipeline"]
            JOB_VOTES["votes-pipeline"]
            JOB_MEMBERS["members-pipeline"]
            JOB_AMEND["amendments-pipeline"]
        end

        subgraph "Cloud Run Jobs (from repcheck-llm-analysis)"
            JOB_ANALYSIS["bill-analysis-pipeline"]
        end

        subgraph "Cloud Run Jobs (from repcheck-scoring-engine)"
            JOB_SCORING["scoring-pipeline"]
        end

        SCHEDULER["Cloud Scheduler<br/>(Cron triggers for ingestion)"]

        subgraph "Pub/Sub Topics"
            T1["bill-events<br/>(bill.text.available)"]
            T2["vote-events<br/>(vote.recorded)"]
            T3["analysis-events<br/>(analysis.completed)"]
            T4["user-events<br/>(user.profile.updated)"]
        end

        subgraph "Storage"
            DB["AlloyDB<br/>(PostgreSQL-compatible)"]
            GCS["GCS: repcheck-prompt-configs<br/>(instruction blocks + profiles)"]
        end

        subgraph "External APIs"
            CONGRESS["Congress.gov API"]
            LLM_APIS["LLM APIs<br/>(Claude, Gemini, OpenAI)"]
        end
    end

    SCHEDULER --> JOB_BILLS
    SCHEDULER --> JOB_VOTES
    SCHEDULER --> JOB_MEMBERS
    SCHEDULER --> JOB_AMEND

    JOB_BILLS --> T1
    JOB_VOTES --> T2

    T1 --> JOB_ANALYSIS
    T2 --> JOB_SCORING
    T3 --> JOB_SCORING
    T4 --> JOB_SCORING

    JOB_ANALYSIS --> T3

    JOB_BILLS --> CONGRESS
    JOB_VOTES --> CONGRESS
    JOB_MEMBERS --> CONGRESS
    JOB_AMEND --> CONGRESS
    JOB_ANALYSIS --> LLM_APIS
    JOB_ANALYSIS --> GCS
    JOB_SCORING --> LLM_APIS
    JOB_SCORING --> GCS

    JOB_BILLS --> DB
    JOB_VOTES --> DB
    JOB_MEMBERS --> DB
    JOB_AMEND --> DB
    JOB_ANALYSIS --> DB
    JOB_SCORING --> DB
```

**Architecture Overview:** Serverless event-driven pipeline on GCP. Cloud Scheduler triggers ingestion jobs on schedule. Data pipelines emit Pub/Sub events triggering LLM analysis and scoring. All jobs read/write AlloyDB and external APIs. Prompt configs stored in GCS.