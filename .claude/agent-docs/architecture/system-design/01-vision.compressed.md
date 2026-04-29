<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/system-design/01-vision.md -->

# RepCheck

## Vision
RepCheck helps users understand legislative alignment through: dynamic Q&A questionnaire → user political profiles → bill intelligence (LLM analysis of bills) → voting record tracking → alignment scoring via LLM comparison → pre-computed cached results.

## Core Components

### Dynamic Q&A
- Generates contextual questionnaire on hot-button topics
- User responses → structured political preference profile

### Bill Intelligence
LLM analyzes congressional bill texts to produce:
- Structured summaries
- Topic tags
- Stance classifications
- Pork/rider detection
- Impact analysis
- Fiscal estimates

### Voting Record Tracking
Every congress member's vote on every bill is tracked and stored.

### Alignment Scoring
LLMs compare user political profiles against legislator voting records to produce alignment scores.

### Pre-computed Results
Scores batch-computed and cached in AlloyDB for instant retrieval.