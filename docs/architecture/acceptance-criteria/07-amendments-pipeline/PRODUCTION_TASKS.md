# Component 7 — Production Tasks

> Tracker for action items that need real environments, external authorities, or upstream artifact bumps. Co-located with the area specs; updated as tasks progress.
>
> **Scope:** Amendment **ingestion only** (§7.1–§7.6). Amendment analysis (§10.11), bill-side analysis refactor (§10.6/§10.7), and amendment scoring (§11.12) are deferred to separate plans — their production tasks live with those plans, not here.

Format per task:
- **ID** — short stable identifier
- **Status** — `Not started` / `In progress` / `Blocked: <reason>` / `Done`
- **Trigger** — when this becomes actionable
- **Contact / Action** — what to do, with concrete URLs / commands
- **Done when** — observable success criterion

---

## Global constant: `MIN_AMENDMENT_CONGRESS = 102`

Per design decision: amendments older than the 102nd Congress (1991-1993) are **out of scope** for ingestion. Rationale: pre-102 senate.gov XML and Congress.gov data have inconsistent shape, SUAMDT (Senate Unprinted Amendments) only existed in the 97th-98th Congresses, and historical analysis of pre-1991 amendments is not part of the product. The `102` cutoff is enforced at:

| Surface | Enforcement |
|---|---|
| Amendments-pipeline ingestion (§7.3) | `congresses` config defaults to `(102 to currentCongress).toList`; per-congress iteration skips anything below |
| Amendment-text-availability-checker (§7.5) | Already filters `congress >= 117` for upstream text-coverage reasons (a tighter constraint that subsumes the 102 cutoff) |
| Votes-pipeline placeholder creation (§7.4) | Skip `upsertPlaceholder` when the amendment's congress < 102 — log info-level skip with `pre_102_amendment` reason |

The constant lives in `repcheck-pipeline-models` `Tables` companion or a dedicated `Constants` object as `Constants.MinAmendmentCongress = 102`. **Add to P7.5 pipeline-models bump.**

---

## P7.0 — Shared-models bump for `AmendmentDO` field additions (PRECONDITION for §7.2 and §7.5)

- **Status:** Not started
- **Trigger:** Before §7.2 / §7.5 implementation. Blocking — code can't compile without the bumped artifact.
- **Contact / Action:** Bump `repchecksharedmodels` from 0.1.39 → 0.1.40 with these field additions:

  **`AmendmentDO` — three new metadata + text-tracking fields** (per S2 — `textVersionsCount` dropped; count is derived from `amendment_text_versions` directly when needed):
  ```scala
  proposedDate:      Option[LocalDate],   // Senate-only — when the amendment was proposed for floor consideration
  latestActionTime:  Option[String],      // pairs with latestActionDate; Congress.gov returns date and time as separate fields
  lastTextCheckAt:   Option[Instant]      // RepCheck-internal audit — when §7.5 last SUCCESSFULLY completed text check (per L1)
  ```
  Update `AmendmentDetailDTO.toDO` to populate `proposedDate` and `latestActionTime` from the upstream response. `lastTextCheckAt` defaults to `None` on initial insert; §7.5 sets it via `updateLastTextCheckAt` only on the success path.

  **`AmendmentDO` — two additional fields for sub-amendment chain resolution (per §7.3):**
  ```scala
  parentAmendmentId: Option[Long]         // surrogate FK to amendments(id) — populated when DTO has `amendedAmendment`. The amendment is a sub-amendment of another amendment
  effectiveBillId:   Option[Long]         // surrogate FK to bills(id). Denormalized cache: equals bill_id directly when set, OR walks up parent_amendment_id chain to find ancestor bill. **Resolved inline at ingest time** via the recursive parent-resolution flow in §7.3 — no end-of-run sweep. NULL for treaty / procedural amendments (orphans — per L8, future scoring/analysis pipelines querying via `findByEffectiveBillId` will skip these as expected).
  ```

  **`chamber` is `chamber_enum` (NOT NULL)** — per L9, always populated. Both ingestion paths (Congress.gov amendment endpoint and Senate XML votes) deterministically produce the chamber from `amendmentType`, so no NULL handling is needed. Schema enforces the constraint.

  **NO `failureCount` / `lastFailureReason` fields.** There's no "stuck amendment" concept — transient errors retry through `RetryWrapper`; systemic errors log and skip on each cron tick. Per-row failure state adds operator complexity without buying anything (the next cron tick re-attempts and either succeeds or logs the same failure again).

  **NO analysis-pass / impact / complexity fields on `AmendmentFindingDO` or `BillFindingDO`** — those belong to the deferred analysis plan, not this one. Same for `AmendmentComplexityScoreDO` / `BillComplexityScoreDO`. Same for the `AnalysisComplexity` and `FindingImpact` ordered sealed traits. Those types are designed and rename-applied across §10.6, §10.7, §10.11, §11.12 area files for when those plans pick them up, but **shared-models 0.1.40 ships without them** — they go into a later bump (likely 0.1.41 or 0.1.42) as part of the analysis plan.

  **NEW: `LegislationRef` sealed type** in `repcheck.shared.models.congress.amendment` (used by §7.4 votes-pipeline integration):
  ```scala
  sealed trait LegislationRef
  object LegislationRef {
    final case class Bill(billType: BillType)              extends LegislationRef
    final case class Amendment(amendmentType: AmendmentType) extends LegislationRef
  }
  ```
  Replaces the current `Either[NonBillOrUnknown, BillType]` return shape of `SenateVoteConverter.normalizeDocumentType` per §7.4.

  **NEW: `AmendmentDetailDTO.toDO` overload** in `AmendmentConversions$AmendmentDetailDTOOps` (the existing parameterless `toDO()` already exists per shared-models 0.1.39 inspection — keep that for callers that don't need resolved ids; add an overload for §7.3):
  ```scala
  def toDO(
    billId:            Option[Long],   // resolved by amendments-pipeline via billRepo.upsertPlaceholder + findByNaturalKey
    sponsorMemberId:   Option[Long],   // resolved via memberRepo.upsertPlaceholder(bioguideId) + findByBioguideId
    parentAmendmentId: Option[Long]    // resolved via the inline recursion in §7.3 (no upsertPlaceholder shortcut — the recursive call drains the parent chain to the bill)
  ): Either[String, AmendmentDO]
  ```
  Returns the constructed `AmendmentDO` with the resolved surrogate ids substituted in. Same parse / validation logic as the parameterless variant.

  **NEW: `AmendmentTextItemDTO` and `AmendmentFormatDTO`** in `repcheck.shared.models.congress.dto.amendment` for `/amendment/.../text` decoding (used by §7.5):
  ```scala
  final case class AmendmentTextItemDTO(
    `type`:  Option[String],          // "Submitted" | "Modified"
    date:    Option[String],          // ISO datetime — parsed downstream
    formats: List[AmendmentFormatDTO]
  )

  final case class AmendmentFormatDTO(
    `type`: String,                   // "PDF" | "HTML"
    url:    String                    // www.congress.gov CREC URL — §7.6 rewrites to api.govinfo.gov
  )

  // Wrapper for the response: { textVersions: [...], pagination: {...} }
  final case class AmendmentTextResponseDTO(
    textVersions: List[AmendmentTextItemDTO],
    pagination:   Option[PaginationInfoDTO]
  )
  ```

  Bump `build.sbt` `repchecksharedmodels` pin from `0.1.39` to `0.1.40` once published.
- **Done when:** New JAR published; this repo's build resolves it; all existing tests still pass.

---

## P7.1 — Pre-flight verification of `/amendment/.../text` format URLs ✅ COMPLETED 2026-05-03

- **Status:** **Done.** Verified against live API on 2026-05-03 with `CONGRESS_GOV_API_KEY`.
- **Result:** Format URLs point to `www.congress.gov/{congress}/crec/{yyyy}/{mm}/{dd}/{vol}/{issue}/[modified/]CREC-{yyyy-mm-dd}-pt{N}-Pg{Section}{-N}.{ext}`. Two formats per version (HTML + PDF). Real example URLs:
  ```
  https://www.congress.gov/117/crec/2021/08/01/167/136/modified/CREC-2021-08-01-pt1-PgS5255.htm
  https://www.congress.gov/119/crec/2025/02/19/171/33/modified/CREC-2025-02-19-pt1-PgS1044-4.htm
  ```
- **Mirror confirmed:** Same content available on `api.govinfo.gov` (verified `HEAD` against `https://api.govinfo.gov/packages/CREC-2021-08-01/granules/CREC-2021-08-01-pt1-PgS5255/htm?api_key=$GOVINFO_API_KEY` returned `200 OK, Content-Type: text/html, Content-Length: 3049349`). Govinfo rate limit `X-Ratelimit-Limit: 36000` per hour. **Tier 1 path is available — no Cloudflare strategy needed.** §7.6 implementation uses a CREC URL rewriter mirroring the existing `GovInfoUrlRewriter` for bills.

---

## P7.2 — Govinfo API key provisioning + rate-limit budgeting

- **Status:** Not started (key already in dev env per CONGRESS_GOV_API_KEY/GOVINFO_API_KEY scheme; production provisioning needed)
- **Trigger:** Production deployment of §7.6 amendment-text-pipeline
- **Contact / Action:** No external contact for the key itself — sign up at [api.data.gov](https://api.data.gov/signup/) which serves as the unified govinfo / api.congress.gov key issuer. **Sign up for govinfo specifically** if a separate key is needed (per existing `.env.example` convention, `GOVINFO_API_KEY` is a separate env var from `CONGRESS_GOV_API_KEY`).
  Budget: govinfo rate limit is 36000/hour per the `X-Ratelimit-Limit` header. Per-amendment download cost is one HEAD + one GET for chosen format (HTML preferred). For 117th+ Senate amendments (~3000 per congress with text), full backfill ≈ 6000 calls comfortably under the hourly budget. Steady-state new amendments are <100/day — negligible.
  Wire the key into Cloud Run secret manager for production deploy. Mirror the existing `GOVINFO_API_KEY` resolution path used by `bill-text-pipeline`.
- **Done when:** Production amendment-text-pipeline successfully downloads text via api.govinfo.gov on first run.

---

## P7.3 — BEHAVIORAL_SPECS.md update for amendment vote weights

- **Status:** Not started
- **Trigger:** Coordinated with the deferred amendment-scoring plan (NOT this plan). Documentation update only — included here because the `vote_weight_type` enum extension is part of P7.6's schema migration even though scoring code that uses it ships later.
- **Contact / Action:** No external contact. Update [BEHAVIORAL_SPECS.md §2](../../../BEHAVIORAL_SPECS.md) to add:
  - Two new `vote_weight_type` enum values: `AMENDMENT_SUBSTANTIVE` (weight 0.7) and `AMENDMENT_PROCEDURAL` (weight 0.2)
  - The `legislation_type × vote_type → vote_weight_type` dispatch table from the deferred §11.12 plan
  - Cross-reference to the amendment-scoring plan once it lands
- **Done when:** BEHAVIORAL_SPECS.md and §11.12 (when implemented) agree on weights and dispatch logic, with no contradiction.

---

## P7.4 — `text-extraction-common` extraction PR (precondition for §7.6)

- **Status:** Not started
- **Trigger:** Before §7.6 implementation. Blocks §7.6 because §7.6 reuses bills-pipeline text infrastructure from this new shared module.
- **Contact / Action:** Standalone PR that:
  1. Creates new `text-extraction-common` SBT subproject in this repo
  2. Moves these from `bill-text-pipeline` to the new module:
     - `HtmlStreamExtractor`, `PdfStreamExtractor`, `XmlStreamExtractor`, `PlainTextStreamExtractor`
     - `BillTextChunker` (rename to `TextChunker` — already format-agnostic; just rename the object)
     - `OllamaEmbeddingService` (already trait-based, just relocate)
     - `CrossBillEmbedder` (rename to `CrossEntityEmbedder` or generic-typed variant — wraps the embedder with batched cross-entity persistence)
  3. Add `HtmlStreamExtractorBase` trait — bills pass through; CREC-aware amendment extractor overrides `shouldKeepNode` + `transformText` to drop CREC running headers/footers
  4. Updates `bill-text-pipeline` to depend on `text-extraction-common`
  5. Existing bill-text tests still pass — no behavior change
  Land this PR before any §7.6 work begins. Per Q9, separate PR keeps each change small and reviewable.
- **Done when:** PR merged. `bill-text-pipeline` depends on `text-extraction-common`. All existing tests green.

---

## P7.5 — Pipeline-models bump for amendment events

- **Status:** Not started
- **Trigger:** Before §7.5 / §7.6 implementation. Blocks code compilation.
- **Contact / Action:** Bump `repcheck-pipeline-models` from 0.1.21 → 0.1.22 with:
  - **One new event:**
    ```scala
    AmendmentTextAvailableEvent(
      amendmentId: Long,           // surrogate
      naturalKey: String,
      congress: Int,
      amendmentType: AmendmentType,
      number: String,
      versionTypeCode: String,     // "SUB" | "MOD"
      formatType: String,          // "HTML" | "PDF"
      url: String,
      publishedDate: Option[Instant],
      correlationId: UUID
    )
    ```
    **No `AmendmentTextIngestedEvent`.** The amendment-text-pipeline does NOT emit a completion event. Downstream pipelines (analysis, scoring) read `amendment_text_versions WHERE fetched_at IS NOT NULL` — same pattern as the bill-side post-event-removal model.
  - **No `AnalysisCompletedEvent` extension.** That belonged to the deferred analysis plan, not this one.
  - Add `EventTypes.AmendmentTextAvailable` constant.
  - Add `Tables.Amendments`, `Tables.AmendmentTextVersions`, `Tables.AmendmentTextChunks` constants (ingestion-only — analysis/scoring tables go into the deferred plans' bumps).
  - Add `Constants.MinAmendmentCongress: Int = 102` (per Q9 of planning).
  Bump `build.sbt` `repcheck-pipeline-models` pin once published.
- **Done when:** New JAR published; build resolves it; events serialize/deserialize round-trip cleanly through Pub/Sub emulator.

---

## P7.6 — Schema migration deployment (ingestion only)

- **Status:** Not started
- **Trigger:** Implementation of §7.2 / §7.4 / §7.6
- **Contact / Action:** Required `repcheck-db-migrations` artifact bumps, sequenced with each area:
  1. **Base amendments table** (§7.2): `amendments` table with surrogate `id BIGSERIAL PRIMARY KEY`, `natural_key TEXT UNIQUE NOT NULL`, `chamber chamber_enum NOT NULL` (per L9 — always populated, derived from amendment_type), plus all DO fields including the new fields from P7.0 (`proposed_date`, `latest_action_time`, `last_text_check_at`, `parent_amendment_id`, `effective_bill_id`). **No `text_versions_count` column** (per S2).
  2. **Legislation type enum extension** (§7.4): `ALTER TYPE legislation_type_enum ADD VALUE IF NOT EXISTS 'HAMDT' / 'SAMDT' / 'SUAMDT';`
  3. **Amendment text tables** (§7.6): `amendment_text_versions` + `amendment_text_chunks` (embedding column `vector(1024)` to match the bill-side qwen3-embedding:0.6b dimension; **HNSW index** per P4) + new `amendment_format_type` enum + new `amendment_text_version_code_type` enum with values `'SUB' / 'MOD'` (per L3 — dedicated enum, NOT an extension of the bill-side `text_version_code_type`). FK to `amendments.id` (surrogate, per Q3). Two partial indexes on `amendment_text_versions` for `WHERE fetched_at IS NULL` and `WHERE fetched_at IS NOT NULL` (so downstream consumers polling readiness don't scan the full table).
  4. **Vote-weight enum extension** (P7.3 / deferred §11.12): `ALTER TYPE vote_weight_type ADD VALUE IF NOT EXISTS 'AMENDMENT_SUBSTANTIVE' / 'AMENDMENT_PROCEDURAL';` — the enum values are added now (cheap, harmless when unused) so the deferred scoring plan doesn't have to chain another migration.

  **Deferred to analysis/scoring plans (NOT in this P7.6):**
  - `amendment_findings`, `amendment_complexity_scores`, `amendment_themes`, `amendment_theme_chunk_members`
  - `bill_complexity_scores` + drop of routing-score columns from `bill_analyses`
  - `member_amendment_stances`, `member_amendment_stance_topics`, `member_amendment_stance_topic_findings`, `user_amendment_alignments`, `member_bill_stance_topic_findings`
  - `finding_impact_weights` table
  - `analysis_complexity` and `finding_impact` Postgres enums
  - `bill_analyses.id UUID→BIGSERIAL`, `bill_concept_groups.id UUID→BIGSERIAL`, `bill_findings.concept_group_id` type change

  Bump `db-migrations-runner` pin in `build.sbt` for each artifact version.
- **Done when:** All listed migrations are published in versioned `repcheck-db-migrations` artifacts and pinned in `build.sbt`. Schema applied to local AlloyDB Omni and dev environment cleanly.

---

## P7.7 — Govinfo rate-limit observability tuning

- **Status:** Blocked: requires production traffic baseline
- **Trigger:** §7.6 has been deployed to production and run for at least 1 week against real `api.govinfo.gov` traffic
- **Contact / Action:** No external contact. Internal observability work:
  - Track `amendment_text_download_attempts_total{outcome="success|govinfo_429|govinfo_5xx|other"}` counter
  - Track `X-Ratelimit-Remaining` header from each response — alert if approaching 0 within an hour window
  - Establish empirical baseline rate; tune alerting thresholds based on baseline + 2× standard deviation
  - If 429 rate sustained > 5%: contact GPO via [askGPO](https://ask.gpo.gov/s/contactsupport) under "govinfo.gov question" category requesting rate-limit increase, citing public-domain civic-tech use case
- **Done when:** Production alerting threshold matches observed baseline; rate-limit headroom tracked.

---

## P7.12 — Terraform infrastructure (`tf-repcheck-infra`) — stable resources only

- **Status:** Not started
- **Trigger:** Before §7.5 / §7.6 production deploy. Local development is unblocked (uses `pubsub-init.sh` + Pub/Sub emulator).
- **Repo:** `C:\Users\elita\source\repos2024\tf-repcheck-infra`
- **Scope (this task):** add only resources that are stable across code changes — the deferred Cloud Run / Cloud Scheduler / monitoring resources land in a follow-up Terraform PR after image refs exist (per IMPLEMENTATION_PLAN Part 7).

### Files to change

1. **`modules/messaging/main.tf`** — append:
   - `google_pubsub_topic.amendment_text_available_dead_letter`
   - `google_pubsub_topic.amendment_text_available`
   - `google_pubsub_subscription.amendment_text_available_pipeline_sub` (60s ack deadline; `dead_letter_policy.max_delivery_attempts = 5`; `retry_policy.minimum_backoff = "10s"` / `maximum_backoff = "600s"`)
   - `google_pubsub_topic_iam_member.pubsub_sa_amendment_dlq_publisher` (Pub/Sub service agent → publisher on DLQ)
   - `google_pubsub_subscription_iam_member.pubsub_sa_amendment_subscriber` (Pub/Sub service agent → subscriber on main sub)

2. **`modules/secrets/main.tf`** — append:
   - `google_secret_manager_secret.govinfo_api_key` (`secret_id = "govinfo-api-key"`; `replication { auto {} }`)
   - `google_secret_manager_secret_iam_member.pipeline_govinfo_accessor` (binds `roles/secretmanager.secretAccessor` on the `repcheck-pipeline-{env}` SA)

3. **`environments/{dev,staging,prod}/main.tf`** — no changes needed (modules already wire `repcheck-pipeline-{env}` SA into messaging + secrets via outputs).

4. **`environments/{dev,staging,prod}/{env}.auto.tfvars`** — no new variables required for this task.

### Out-of-band steps (not Terraform)

- `gcloud secrets versions add govinfo-api-key --project=repcheck-{env} --data-file=- <<< "$GOVINFO_API_KEY"` for each env after `terraform apply`.

### Done when

- `terraform plan -chdir=environments/dev` shows the new resources only (zero drift on existing pipelines).
- `terraform apply` clean in dev → staging → prod via Atlantis.
- `gcloud pubsub topics list` shows `amendment-text-available` and `amendment-text-available-dead-letter` in each project.
- `gcloud pubsub subscriptions describe amendment-text-available-pipeline-sub` shows `ackDeadlineSeconds: 60`, `deadLetterPolicy.maxDeliveryAttempts: 5`.
- `gcloud secrets describe govinfo-api-key` succeeds in each project after value is added.
- The `repcheck-pipeline-{env}` SA can read `govinfo-api-key` (test via `gcloud secrets versions access latest --secret=govinfo-api-key --impersonate-service-account=...`).

### Deferred to follow-up Terraform PR

Cloud Run Jobs (`amendments-pipeline`, `amendment-text-availability-checker`), Cloud Run Service (`amendment-text-pipeline`), Cloud Scheduler triggers, monitoring alert policies. These land after CI builds + pushes images to Artifact Registry — Terraform can't reference an image that doesn't exist yet without a circular dependency.

### Service-account decision (no Terraform change required)

All three new pipelines reuse the existing `repcheck-pipeline-{env}` SA — same shared-SA pattern as bills + members. No per-pipeline SA isolation (the precedent for that is `votes-pipeline`, but its rationale doesn't apply: amendments and bills share the same trust boundary, same secrets, same Pub/Sub topics they care about).

---

## P7.11 — Consolidate transient-network-error helper into `ingestion-common` (per S7)

- **Status:** Not started
- **Trigger:** Before §7.1 / §7.5 / §7.6 implementation. Code duplication today: `BillSummariesApiErrorClassifier` + the planned `AmendmentsApiErrorClassifier` (§7.1) + `AmendmentTextCheckErrorClassifier` (§7.5) + `AmendmentTextDownloadErrorClassifier` (§7.6) all carry an identical `isTransientNetworkError` cause-chain walk. Earlier draft accepted the duplication; at 4× call sites it's worth extracting.
- **Contact / Action:** Standalone `ingestion-common` PR that:
  1. Adds a method `transientNetworkAware[E <: HttpStatusError](base: HttpStatusErrorClassifier[E]): ErrorClassifier` to the existing `ingestion-common` error-classifier base. The wrapper checks `isTransientNetworkError(t)` (cause-chain walk depth-bounded at 16) before deferring to the underlying status-code classifier.
  2. `isTransientNetworkError` matches: `org.http4s.ember.core.EmberException`, `java.io.IOException` (incl. `SocketTimeoutException`, `ConnectException`), and `java.util.concurrent.TimeoutException`. Other types fall through to the base classifier.
  3. Existing `BillSummariesApiErrorClassifier` is migrated to use the new wrapper. Tests stay green (regression-only).
  4. New `AmendmentsApiErrorClassifier` (§7.1), `AmendmentTextCheckErrorClassifier` (§7.5), `AmendmentTextDownloadErrorClassifier` (§7.6) all use the wrapper from day one — the duplicated cause-chain code never lands in the new pipelines.
- **Done when:** `grep -r "isTransientNetworkError" --include='*.scala'` finds the implementation only in `ingestion-common`; classifier files in `bill-summary-pipeline`, `amendments-pipeline`, `amendment-text-availability-checker`, `amendment-text-pipeline` reference it via the wrapper, not via copy-pasted bodies.

---

## P7.8 — REMOVED

Originally proposed adding `MemberRepository.upsertPlaceholder(bioguideId)`. **Dropped after PR review** — the codebase already has a member-placeholder mechanism that bills-metadata-pipeline uses today: shared `PlaceholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)` ([ingestion-common.placeholders](../../../../bill-metadata-pipeline/src/main/scala/repcheck/ingestion/bills/metadata/pipeline/MemberResolver.scala)) backed by `MemberInsertSql` (`ON CONFLICT (natural_key) DO NOTHING`). §7.3 reuses this pattern. No new method on `MemberRepository`. No bump to `members-common`.

The same correction applies to `AmendmentRepository.upsertPlaceholder` and `BillRepository.upsertPlaceholder` (existing) — both should be `F[Unit]` (caller does separate `findByNaturalKey` / `findByBillId` to get surrogate id), mirroring the established `BillLookup` pattern in votes-pipeline ([BillLookup.scala:43-44](../../../../votes-pipeline/src/main/scala/repcheck/ingestion/votes/pipeline/BillLookup.scala)). The earlier "S5 single-roundtrip" simplification is rescinded — the 2-step pattern is the codebase precedent and the perf delta is negligible at expected volumes.

---

## Volume + cost estimates (per Q6)

Order-of-magnitude estimates for capacity planning. **Ingestion only** — analysis/scoring costs belong to the deferred plans. Refine from real production observability after first month.

| Concern | Estimate | Notes |
|---|---|---|
| Amendments per Congress, modern era (102+) | ~3K-10K | Varies by activity level; reconciliation / appropriations congresses skew higher |
| Total historical backfill rows (102-119, 18 congresses) | ~50K-150K | Single one-shot run with wide `lookbackDays` |
| Amendments with text granules (117+ only) | ~10K-30K | LoC publishes text only for 117+; House subset is partial |
| Steady-state new amendments per week | ~50-200 | Drives §7.3 cron load |
| Amendments getting text-checked per §7.5 cron run (4h cadence) | ~10-100 | Bounded by `staleAfter` filter + 117+ subset |
| §7.6 download volume (initial backfill) | ~10K-30K | One per text-version; HTML preferred |
| §7.6 download volume (steady-state) | ~10-50/day | Steady-state govinfo fetches |
| §7.6 ollama embedding calls (initial backfill) | ~150K-500K chunk embeddings | Self-hosted; no per-call cost. CPU/GPU capacity sized to bills-pipeline pattern |
| §7.6 ollama embedding calls (steady-state) | ~50-500/day | Negligible |

These are conservative — refine when production traffic provides real numbers. **No LLM cost lines** — that's the deferred analysis plan.

---

## Notes

| Task | Production-only? | Blocking work? |
|---|---|---|
| P7.0 (shared-models bump, ingestion fields only) | No (publish from dev) | **Blocking** — §7.2/§7.5 can't compile without |
| P7.1 (pre-flight) | No | Done |
| P7.2 (govinfo key) | Yes (production secret) | Blocking for production-deploy of §7.6 |
| P7.3 (BEHAVIORAL_SPECS) | No | Non-blocking documentation sync; coordinated with deferred §11.12 plan |
| P7.4 (text-extraction-common extraction) | No | **Blocking** §7.6 |
| P7.5 (pipeline-models bump, ingestion events only) | No (publish from dev) | **Blocking** — §7.5/§7.6 can't compile without |
| P7.6 (schema migrations, ingestion tables only) | No (publish from dev) | **Blocking** — sequenced per area |
| P7.7 (govinfo rate-limit observability) | Yes (real-traffic baseline) | Non-blocking, post-deploy tuning |
| ~~P7.8 (members-common upsertPlaceholder)~~ | — | **REMOVED** — reuse existing `PlaceholderCreator` pattern from bills-metadata-pipeline |
| P7.11 (shared `transientNetworkAware` classifier helper, per S7) | No (publish from dev) | **Blocking** — §7.1, §7.5, §7.6 classifiers depend on it from day one |
| P7.12 (Terraform — Pub/Sub topic + DLQ + sub + `govinfo-api-key` secret) | Yes (each env via Atlantis) | Blocking for §7.5 / §7.6 production deploy. Local dev unblocked via emulator + pubsub-init.sh. |

## Artifact versioning model (per Q53)

Each publishable artifact (`repcheck-shared-models`, `repcheck-pipeline-models`, `repcheck-db-migrations`, `repcheck-ingestion-common`) is bumped **independently**, not lockstep — matches the existing repo convention. Each consumer (this repo's SBT subprojects) pins minimum versions and tolerates additive changes from upstream. Bump order during the amendment-ingestion work:

1. `repcheck-shared-models` (P7.0 — `AmendmentDO` field additions, `LegislationRef`, `AmendmentTextItemDTO`, `AmendmentDetailDTO.toDO` overload — **no analysis types**)
2. `repcheck-pipeline-models` (P7.5 — `AmendmentTextAvailableEvent`, Tables constants, `MinAmendmentCongress` — **no `AmendmentTextIngestedEvent`**)
3. `repcheck-db-migrations` (P7.6 — ingestion tables + ahead-of-time enum extensions for the deferred scoring plan)
4. `repcheck-ingestion-common` (P7.11 — `transientNetworkAware` helper)
5. This repo's amendments-pipeline / §7.5 / §7.6 code that depends on the above. **No `members-common` bump** — §7.3 reuses the existing `PlaceholderCreator` machinery.

> **Versions are sbt-dynver-driven** — each artifact repo auto-tags the next semver patch on merge. The plan's literal version numbers (e.g. "0.1.40") are aspirational; the actual tag is whatever CI produces. Consumers bump the pin to the produced tag, not the planned number.

Consumers can bump out-of-order as long as required minimum versions are met. No "release train" — each artifact ships when ready.

---

## Out of scope (deferrals to other plans)

These had their own task IDs in earlier drafts. They've been removed from this tracker because the work belongs to plans that haven't started yet:

| Removed task | Belongs to |
|---|---|
| **P7.9** (Component 8 amendment-analysis prompt blocks) | Amendment-analysis plan |
| **P7.10** (Component 9 score-explainer prompt update for amendments) | Amendment-scoring plan |
| Bill-side analysis schema migrations (`bill_complexity_scores`, UUID→BIGSERIAL, `bill_concept_groups` type change) | Component 10 plan |
| Amendment scoring tables (`member_amendment_stances*`, `user_amendment_alignments`, `finding_impact_weights`) | Amendment-scoring plan |

When those plans pick up, they reference the contracts already in the area files (§10.6, §10.7, §10.11, §11.12) — those files already have the correct enum names (`AnalysisComplexity` / `FindingImpact`) and field names (`impact` not `significance`) from the rename pass done during this planning effort.

> **What used to be here and got deleted:** earlier drafts had Cloudflare-mitigation tasks (LoC allowlist outreach, headless browser sidecar, Cloudflare 403 alerting). All removed after P7.1 verified that the api.govinfo.gov mirror exists for amendment text — the Cloudflare path is unused. If a future Congress.gov change ever invalidates the CREC mirror, those tasks should be re-introduced.
