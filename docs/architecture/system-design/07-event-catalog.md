> Part of [System Design](../SYSTEM_DESIGN.md)

## Pub/Sub Event Catalog

> **Design principle**: Only emit Pub/Sub events when they trigger downstream actions. Pipelines that only persist data (members, amendments) do not publish events.

```mermaid
graph LR
    subgraph "Events (only those with downstream consumers)"
        direction TB
        E1["bill.text.available<br/>─────<br/>billId, textUrl,<br/>format (PDF/XML/TXT)"]
        E2["vote.recorded<br/>─────<br/>voteId, billId,<br/>chamber, date"]
        E3["analysis.completed<br/>─────<br/>billId, analysisId,<br/>topics[], model used"]
        E4["user.profile.updated<br/>─────<br/>userId, topics changed[]"]
    end

    subgraph "Producer → Consumer"
        E1 -.-> |"llm-analysis"| A1[Bill Analysis Pipeline]
        E2 -.-> |"scoring-engine"| A2[Alignment Scoring Pipeline]
        E3 -.-> |"scoring-engine"| A2
        E4 -.-> |"scoring-engine"| A2
    end
```
