> Part of [System Design](../SYSTEM_DESIGN.md)

## Vision

RepCheck is a citizen-facing platform that helps users understand how their legislators vote relative to their personal political interests. It achieves this through:

1. **Dynamic Q&A** — Users answer a dynamically generated questionnaire on hot-button topics
2. **User Political Profiles** — Responses are processed into a structured political preference profile
3. **Bill Intelligence** — Congressional bill texts are analyzed by LLMs to produce structured summaries, topic tags, stance classifications, pork/rider detection, impact analysis, and fiscal estimates
4. **Voting Record Tracking** — Every congress member's vote on every bill is tracked
5. **Alignment Scoring** — LLMs compare user political profiles against legislator voting records to produce alignment scores
6. **Pre-computed Results** — Scores are batch-computed and cached in AlloyDB for instant retrieval
