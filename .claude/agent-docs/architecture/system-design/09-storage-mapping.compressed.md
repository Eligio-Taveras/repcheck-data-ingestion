<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/system-design/09-storage-mapping.md -->

# Storage Mapping

| Entity | Store | Table(s) | Rationale |
|--------|-------|----------|-----------|
| Bills | AlloyDB | `bills`, `bill_cosponsors`, `bill_subjects` | Congressional metadata, normalized cosponsors and subjects |
| Bill Text Versions | AlloyDB | `bill_text_versions` | Immutable text versions with content and embeddings (pgvector) |
| Members | AlloyDB | `members`, `member_terms`, `member_party_history` | Congressional metadata, terms, party affiliations |
| Member History | AlloyDB | `member_history`, `member_term_history` | Archive-before-overwrite snapshots |
| LIS Mapping | AlloyDB | `lis_member_mapping` | Senate LIS ID → bioguideId mapping from senator-lookup.xml |
| Committees | AlloyDB | `committees`, `committee_members`, `bill_committee_referrals` | Committee membership from chamber XML feeds |
| Votes | AlloyDB | `votes`, `vote_positions` | Congressional metadata, normalized positions |
| Vote History | AlloyDB | `vote_history`, `vote_history_positions` | Archive-before-overwrite snapshots |
| Amendments | AlloyDB | `amendments` | Congressional metadata, linked to bills |
| Bill Text & Sections | AlloyDB | `bill_text_sections` | Bill text section breakdowns (BillTextSectionDO) |
| Bill Concept Groups | AlloyDB | `bill_concept_groups`, `bill_concept_group_sections` | Concept groupings from decomposition, junction table linking sections to concept groups |
| Bill Concept Summaries | AlloyDB | `bill_concept_summaries` | Per-concept LLM summaries |
| Bill Analyses | AlloyDB | `bill_analyses`, `bill_findings`, `amendment_findings`, `finding_types` | LLM analysis results linked to bills via `bill_id` FK |
| Bill Analysis Topics | AlloyDB | `bill_analysis_topics` | Normalized topic confidence scores per bill analysis |
| Bill Fiscal Estimates | AlloyDB | `bill_fiscal_estimates` | Fiscal impact estimates per bill |
| Q&A | AlloyDB | `qa_questions`, `qa_question_topics`, `qa_answer_options`, `qa_user_responses` | Survey/questionnaire questions, topic mappings, answer choices, user responses |
| Users | AlloyDB | `users`, `user_preferences` | Relational user data, joins with preferences |
| User Topic Priorities | AlloyDB | `user_topic_priorities` | User's prioritized topics derived from Q&A responses |
| User Legislator Pairings | AlloyDB | `user_legislator_pairings` | User-to-legislator matching by district/state |
| Member Bill Stances | AlloyDB | `member_bill_stances` | Derived vote positions per bill per member |
| Member Bill Stance Topics | AlloyDB | `member_bill_stance_topics` | Per-topic stance breakdown for member votes |
| User Alignment Scores | AlloyDB | `user_bill_alignments`, `user_amendment_alignments` | Pre-computed user-bill and user-amendment alignment scores |
| Stance Materialization | AlloyDB | `stance_materialization_status` | Tracks readiness for stance materialization |
| Pre-computed Scores | AlloyDB | `scores`, `score_topics`, `score_congress`, `score_congress_topics` | Denormalized scoring tables, keyed by `(user_id, member_id)` |
| Score History | AlloyDB | `score_history`, `score_history_congress`, `score_history_congress_topics`, `score_history_highlights` | Historical score snapshots with LLM reasoning |
| Workflow State | AlloyDB | `workflow_runs`, `workflow_run_steps` | Launcher workflow execution tracking |
| Pipeline Runs | AlloyDB | `pipeline_runs`, `processing_results` | Per-pipeline execution metadata |
| US States | AlloyDB | `us_states` | US state reference data (lookup table) |
| Event Log | AlloyDB | `event_log` | Audit log for pipeline events |

**Authority**: See `Tables` object in Component 2 §2.10 for authoritative table name constants. All SQL queries must reference table names through `Tables`, never hardcoded strings.

**Dev Cost Optimization**: Dev uses Cloud SQL PostgreSQL (db-f1-micro, ~$10/mo) instead of AlloyDB (~$390/mo). Both PostgreSQL wire-compatible — same Doobie code works against either. pgvector available on both via `CREATE EXTENSION vector`. Staging/prod use AlloyDB for columnar engine and optimized vector search. `db_engine` variable in Terraform `data` module controls provisioned engine.