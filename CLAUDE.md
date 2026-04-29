# RepCheck — Agent Routing Guide

## What This Project Is
RepCheck is a citizen-facing platform that helps users understand how their legislators vote relative to their personal political interests. It combines Congressional data ingestion, LLM-powered bill analysis, and alignment scoring.

## Documentation Map — Read Only What You Need

**Agents: Use the compressed versions in `.claude/agent-docs/` for lower token cost.**
**Humans: Use the full versions in `docs/` for readability.**

### If you need to understand the overall system:
- Agent: `.claude/agent-docs/architecture/SYSTEM_DESIGN.compressed.md`
- Human: `docs/architecture/SYSTEM_DESIGN.md`

### If you need behavioral rules (diffing, scoring, events, workflows):
- Agent: `.claude/agent-docs/architecture/BEHAVIORAL_SPECS.compressed.md`
- Human: `docs/architecture/BEHAVIORAL_SPECS.md`

### If you need coding conventions:
- Agent: `.claude/agent-docs/architecture/SCALA_CODE_PATTERNS.compressed.md`
- Human: `docs/architecture/SCALA_CODE_PATTERNS.md`

### If you need to find a template for a specific component:
- Agent: `.claude/agent-docs/templates/PATTERNS_GUIDE.compressed.md`
- Human: `docs/templates/PATTERNS_GUIDE.md`

### If you need the Congress.gov API field/endpoint reference:
- `docs/reference/congress-gov-api.yaml` (local OpenAPI spec — no internet access needed)

---

## Task Routing Table

**MANDATORY**: You must use the routing table below whenever you are planning a task. Before writing any code, match your task to the closest entry and read every file listed under it. Additionally, every 5th task step during implementation, pause and check whether you need information from the routing table documentation that you have not yet loaded. This ensures you stay aligned with project patterns as work evolves.

Read ONLY the files listed for your task. Do not load all documentation.

---

### Getting started or onboarding to the project
```
docs/QUICKSTART.md
docs/architecture/system-design/01-vision.md
docs/architecture/system-design/02-architecture.md
```

### Determining which repository or SBT project to work in
```
docs/architecture/system-design/04-repo-structure.md
docs/architecture/system-design/05-component-details.md
```

### Understanding the event-driven data flow between pipelines
```
docs/architecture/system-design/03-event-flow.md
docs/architecture/system-design/07-event-catalog.md
docs/architecture/BEHAVIORAL_SPECS.md
```

### Understanding the data model or storage layout
```
docs/architecture/system-design/08-data-model.md
docs/architecture/system-design/09-storage-mapping.md
```

### Understanding implementation priority or phasing
```
docs/architecture/system-design/11-implementation-priority.md
docs/architecture/system-design/05-component-details.md
```

---

### Implementing any component (Components 1–11)

Acceptance criteria are the **authoritative implementation spec** for each component. They define every class, trait, method signature, behavioral rule, and test expectation. When implementing a component, the acceptance criteria are your primary source of truth — they supersede system design docs where they conflict (system design is intent; acceptance criteria are the refined, cross-checked spec).

**How to use acceptance criteria:**
1. Start at the master index: `docs/architecture/acceptance-criteria/README.md` — find your component
2. Read the component's index file (e.g., `04-BILLS-PIPELINE.md`) — this gives system context, data flow, package structure, dependencies, and a routing table to area files
3. Read only the area files relevant to your task (e.g., `04.3-bill-metadata-processing.md` for the processor class)
4. Each area file specifies: class signatures, constructor dependencies, method behavior (numbered steps), key design decisions, and an acceptance criteria table with test expectations
5. Cross-references between components (e.g., "per Component 3 §3.6") point to other area files — follow them when you need the referenced contract

**What acceptance criteria are NOT:**
- They are not tutorials or explanations — they assume you've read the system design for context
- They do not contain implementation code — only signatures, behavior specs, and test expectations
- They do not duplicate the templates — use the templates (below) for Scala patterns and code skeletons

```
docs/architecture/acceptance-criteria/README.md (master index — routes to all components)
```

Also read the relevant templates for your component type:
- DTOs/DOs/enums: `docs/templates/annotated/dto-do-layering.md`, `circe-codecs.md`, `enum-with-parsing.md`
- API clients: `docs/templates/annotated/paginated-api-client.md`, `congress-entity-api.md`
- Repositories: `docs/templates/annotated/alloydb-persistence.md`
- Pipeline entry points: `docs/templates/annotated/ioapp-entry-point.md`, `config-loading.md`
- Error handling: `docs/templates/skeletons/error-pattern.scala`, `retry-wrapper.scala`
- Congress.gov API reference: `docs/reference/congress-gov-api.yaml`

### Implementing Component 5: members-pipeline (member profiles, LIS mapping)
```
docs/architecture/acceptance-criteria/05-MEMBERS-PIPELINE.md (index — follow routing table to area files)
docs/architecture/acceptance-criteria/03-INGESTION-COMMON.md (for shared patterns referenced by members pipeline)
docs/reference/congress-gov-api.yaml
```

---

### Building a Congress.gov API client (bills, votes, members, amendments)
```
docs/templates/annotated/paginated-api-client.md
docs/templates/annotated/dto-do-layering.md
docs/templates/annotated/circe-codecs.md
docs/templates/skeletons/config-pattern.scala
docs/templates/skeletons/error-pattern.scala
docs/templates/skeletons/retry-wrapper.scala
```

### Building any new Congress.gov entity client (amendments, members, votes, etc.)
```
docs/templates/annotated/congress-entity-api.md
docs/templates/skeletons/congress-entity-api.scala
docs/reference/congress-gov-api.yaml
docs/templates/skeletons/error-pattern.scala
```

### Building an AlloyDB persistence layer
```
docs/templates/annotated/alloydb-persistence.md
docs/templates/skeletons/alloydb-repository.scala
docs/templates/skeletons/doobie-repository.scala
docs/templates/skeletons/error-pattern.scala
docs/architecture/system-design/08-data-model.md
docs/architecture/system-design/09-storage-mapping.md
```

### Building a pipeline entry point (Cloud Run Job)
```
docs/templates/annotated/ioapp-entry-point.md
docs/templates/annotated/config-loading.md
docs/templates/skeletons/pipeline-app.scala
docs/templates/skeletons/streaming-pipeline.scala
docs/templates/skeletons/config-pattern.scala
```

### Building the Pub/Sub publisher or subscriber
```
docs/templates/skeletons/pubsub-publisher.scala
docs/templates/skeletons/pubsub-subscriber.scala
docs/templates/skeletons/error-pattern.scala
docs/templates/skeletons/retry-wrapper.scala
docs/architecture/system-design/07-event-catalog.md
```

### Building the orchestrator
```
docs/architecture/BEHAVIORAL_SPECS.md
docs/templates/skeletons/orchestrator.scala
docs/templates/skeletons/pubsub-publisher.scala
docs/templates/skeletons/pubsub-subscriber.scala
docs/templates/skeletons/config-pattern.scala
docs/architecture/system-design/03-event-flow.md
```

### Building the LLM client adapter
```
docs/templates/skeletons/llm-client-adapter.scala
docs/templates/skeletons/error-pattern.scala
docs/templates/skeletons/retry-wrapper.scala
docs/templates/skeletons/config-pattern.scala
docs/architecture/system-design/06-llm-cost-strategy.md
```

### Building the prompt engine
```
docs/templates/skeletons/prompt-engine.scala
docs/templates/skeletons/gcs-reader.scala
docs/templates/skeletons/config-pattern.scala
```

### Building the snapshot service
```
docs/architecture/BEHAVIORAL_SPECS.md
docs/templates/skeletons/snapshot-service.scala
docs/templates/skeletons/gcs-reader.scala
docs/templates/skeletons/alloydb-repository.scala
docs/templates/skeletons/doobie-repository.scala
```

### Building the scoring engine
```
docs/architecture/BEHAVIORAL_SPECS.md
docs/templates/skeletons/streaming-pipeline.scala
docs/templates/skeletons/llm-client-adapter.scala
docs/templates/skeletons/prompt-engine.scala
docs/templates/skeletons/alloydb-repository.scala
docs/templates/skeletons/doobie-repository.scala
docs/templates/skeletons/config-pattern.scala
docs/architecture/system-design/06-llm-cost-strategy.md
```

### Building workflow definitions
```
docs/architecture/BEHAVIORAL_SPECS.md
docs/templates/skeletons/workflow-definition.scala
docs/architecture/system-design/03-event-flow.md
docs/architecture/system-design/07-event-catalog.md
```

### Writing tests for any module
```
docs/templates/annotated/test-patterns.md
docs/templates/annotated/testing-infrastructure.md
docs/templates/skeletons/test-templates.scala
docs/templates/skeletons/github-actions-bug-on-failure.yml
```

### Deploying or containerizing a repository
```
docs/templates/annotated/deployment-architecture.md
docs/templates/skeletons/dockerfile-pipeline.txt
docs/templates/skeletons/docker-compose-local-dev.yml
docs/templates/skeletons/cloud-run-job.yaml
docs/templates/skeletons/github-actions-deploy.yml
docs/architecture/system-design/10-deployment.md
```

### Creating a new enum or domain type
```
docs/templates/annotated/enum-with-parsing.md
docs/templates/annotated/circe-codecs.md
```

### Looking up Scala implementation patterns (effect system, streaming, error handling, etc.)
```
docs/architecture/SCALA_CODE_PATTERNS.md (index — follow TOC to specific subsection files in docs/architecture/scala-code-patterns/)
```

---

## Universal Rules (always apply)

- **Scala style**: Always use traditional curly braces syntax. No Scala 3 braceless/indentation syntax. Use `if (cond) { ... } else { ... }`, not `if cond then ... else ...`. Use `object Foo { }`, not `object Foo:`.
- **Effect system**: Tagless final `F[_]` everywhere. `Sync[F].delay` for synchronous side effects, `Async[F].blocking` for blocking I/O.
- **Never bind effectful values to `_`**: `val _ = effect` is a silent no-op in cats-effect (and ZIO/Monix). The effect is never run — only its description is discarded. Treat any `val _ = io.Something(...)`, `val _ = ref.complete(...)`, `val _ = stream.compile.drain`, or `val _ = deferred.complete(...)` as a bug. Use `_ <- effect` inside a for-comprehension, or `effect.void` followed by sequencing. The bill-text-pipeline hung for 25 minutes on `val _ = progress.deferred.complete(...)` because the IO description was discarded; the Deferred never resolved.
- **Read before rollback. Report before resolve.**: When a commit, diff, or `git status` shows artifacts you did not author — extra files in a commit, unexpected modifications, untracked content swept into a stage — do NOT autonomously run corrective commands (`git rm --cached`, `git checkout --`, `git reset`, `rm`, file deletes). Read each unexpected artifact, summarize it for the user (path + nature + size), then ask before resolving. The "unintended" file is often the user's in-flight work that was swept up by mistake. A `PreToolUse` hook in `.claude/settings.json` prints a reminder when these commands are about to run, but the rule is the principle.
- **Windows / Git Bash environment** (this developer's machine, not Linux CI): One-time machine configs to verify — `git config --global core.autocrlf input` (LF normalization, prevents scalafmt false-positives on Windows checkouts), `git config --global core.longpaths true` (avoids the 260-char path limit when worktrees nest), `git config --global url."https://github.com/".insteadOf git@github.com:` (rewrites all SSH GitHub URLs to HTTPS, sidesteps the recurring `ssh-agent` socket exhaustion on this install). Ongoing practice — invoke sbt as `sbt.bat` not `sbt` from Git Bash (the bare shim is unreliable); read Windows user-scope env vars via `powershell.exe -Command "[System.Environment]::GetEnvironmentVariable('NAME', 'User')" | tr -d '\r'` (Bash does not inherit HKCU vars); PreToolUse hooks parsing JSON tool input require `jq` on PATH (drop binary at `~/bin/jq.exe` if missing) or hook checks silently no-op.
- **Errors**: Flat, unique exception per failure case. No sealed hierarchies. Context implied by the executing application. `ErrorClassifier` per subsystem: `Transient` (continue) vs `Systemic` (halt).
- **Streaming**: Always use `parEvalMap(config.parallelism)`. Sequential = parallelism 1.
- **Retry**: Centralized `RetryWrapper[F]` with per-subsystem `RetryConfig`. Exponential backoff (10ms initial, 2x multiplier, 60s cap, 3 retries default).
- **DTO/DO layering**: DTOs for API shapes, DOs for storage. `toDO` returns `Either`. AlloyDB DOs use Doobie auto-derived `Read`/`Write` — no `toPojo` needed.
- **Config**: PureConfig with auto-derivation. Per-subsystem retry, parallelism, and timeout config.
- **IDs**: Natural keys for legislative data (Congress.gov IDs). Generated UUIDs for RepCheck-specific entities.
- **Correlation IDs**: Every pipeline item gets a UUID. Visible in all logs and ProcessingResults.
- **Tests**: Equivalence class negative testing. Line-by-line failure analysis. Every exception unique. AlloyDB Omni (Docker) for local integration. WireMock for failure simulation. Dev GCP for contract validation. All run on every PR commit. **No `null` in test assertions** — WartRemover's `Wart.Null` applies to test code too. Use `.toString should not be empty` or `shouldBe a[SomeType]` instead of `should not be null`.
- **Coverage**: All newly created or changed code must have test coverage above 90% (enforced by Codecov patch coverage on PRs). Run `sbt coverage test coverageReport` locally to verify before pushing. Never add files to the `ignore` list in `codecov.yml` to work around missing coverage — instead, use the testability refactoring pattern below.
- **Testability refactoring**: When an entry point (IOApp, main object, or any class that hard-wires its own dependencies) has logic that can't be unit tested, apply this pattern: (1) extract the logic into a new class whose constructor receives the dependencies (AlloyDB transactor, HTTP client, Logger, etc.) so tests can pass mocks; (2) scope helpers as `private[package]` instead of `private` — tests in the same package can call them directly; (3) add a `package` declaration to any file that lacks one; (4) extract each multi-line for-comprehension RHS into its own named method so each sub-operation is independently testable; (5) for IOApp `run` methods, move ALL pipeline logic into a `private[app]` companion object method that accepts factory functions (`configLoader`, `dbInit`, `apiFactory`, etc.) as parameters — tests inject stubs, production code passes real implementations, and the IOApp `run` becomes pure wiring. `coverageExcludedFiles` in `build.sbt` is **only acceptable** for an App/IOApp class that has been fully reduced to wiring with no domain logic remaining — this is the only valid use of any coverage exclusion mechanism. This pattern applies broadly — not just to IOApp.
- **Table names**: Use constants from `pipeline-models` `Tables` object, never hardcode strings.
- **GCS versioning**: Semver in filenames. Version configurable per app, CI deploys default.
- **No hardcoded prompts**: All prompt fragments live in GCS. Prompt engines are loaders + assemblers only.
- **Mocking**: MockitoScala for trait mocking, WireMock for HTTP simulation, AlloyDB Omni (Docker) for integration with real infrastructure.
- **Test isolation**: Ephemeral namespace prefix per test run (e.g., `test-{uuid}-`). All cloud resources use the prefix. Cleanup in `afterAll()`.
- **E2E tests**: Same modules, tagged with `taggedAs E2ETest`. Excluded from `sbt test`. Run via `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`.
- **Container base image**: Google Distroless Java 21 (`gcr.io/distroless/java21-debian12`) for all Cloud Run deployments.
- **GCP auth in CI**: Workload Identity Federation (keyless OIDC). No service account JSON keys.
- **Promotion**: Dev → Staging → Prod. Auto-deploy to dev on merge. Manual gate before prod.
- **No `@nowarn`**: Never suppress compiler warnings with `@nowarn` or similar annotations. Always find and apply the real fix — use the correct non-deprecated API, proper types, or refactor the code. If a library deprecates a method, migrate to its replacement.
- **No `SELECT *`**: Never use `SELECT *` in Doobie queries. Always list columns explicitly in the order matching the target case class constructor. `SELECT *` returns columns in physical table order, which may not match the case class field order (e.g., when columns were added via `ALTER TABLE ADD COLUMN`). Doobie maps columns positionally, so a mismatch causes silent type errors at runtime.
- **Schema design — denormalize, no JSONB**: Always denormalize tables. Avoid JSONB in structured schemas. The only acceptable use of JSONB is when you genuinely cannot predict the object shape being stored and expect a wide variety of structures. When tempted to use JSONB, normalize into separate tables with proper columns and foreign keys instead.
- **Pre-push CI checks — MANDATORY**: Never push or create a PR without running CI checks first. Use the provided shell functions (source `scripts/ci-functions.sh`):
  - `CreatePR "title" "body"` — runs all CI checks, pushes, creates the PR. Use this for new PRs.
  - `pushToPR` — runs all CI checks, pushes. Use this for updates to an existing PR.
  - Both functions run: `sbt compile`, `sbt test`, `sbt scalafmtCheckAll`, `sbt scalafixAll --check`. They abort on the first failure. **Do not bypass these functions by pushing directly.**
  - If tests require unavailable infrastructure (e.g., Docker not running), note it in the PR.
- **Branch hygiene**: Before pushing commits, check if the current branch has already been merged into main (via `gh pr list --state merged --head <branch>`). If it has been merged, create a new branch named after the work being done. If it has not been merged, push to the existing branch.

## Tech Stack Quick Reference

| Concern | Technology |
|---------|-----------|
| Language | Scala 3.7.3 |
| Effect system | Cats Effect (tagless final) |
| HTTP | http4s Ember |
| JSON | Circe (semi-auto derivation) |
| Streaming | FS2 |
| Database | Doobie (auto-derived Read/Write, pgvector for embeddings). AlloyDB in staging/prod, Cloud SQL PostgreSQL in dev (~$10/mo vs ~$390/mo). Both wire-compatible. |
| GCS | Google Cloud Storage Java SDK wrapped in Sync[F] |
| Pub/Sub | Google Cloud Pub/Sub Java SDK wrapped in F[_] |
| Config | PureConfig (auto-derivation, pureconfig-generic-scala3) |
| Testing | ScalaTest (AnyFlatSpec + Matchers), MockitoScala, AlloyDB Omni (Docker), WireMock |
| Containers | Google Distroless Java 21, Docker Compose (local dev) |
| Deployment | Cloud Run Jobs (pipelines), Cloud Run Services (API, future) |
| Build | SBT multi-project |
| Linting | WartRemover (11 error rules), Scalafix (import ordering), tpolecat |
| CI | GitHub Actions |
| ML (in-process) | DJL (Deep Java Library) + ONNX Runtime for section embedding (all-MiniLM-L6-v2), Smile for clustering (k-means/DBSCAN) |
| Text parsing sidecar | Ollama (Cloud Run sidecar) for format-agnostic bill text section identification (handles XML, plain text, PDF-extracted text) |
| LLM | Anthropic Java SDK (Claude), OpenAI SDK (GPT) via vendor-neutral adapter |
| Publishing | GitHub Packages (Maven) |

## Build Commands
```bash
sbt compile              # Compile with WartRemover + tpolecat
sbt test                 # Run all tests
sbt scalafmtCheckAll     # Check formatting (fails if unformatted)
sbt scalafmtAll          # Auto-format all source files
sbt scalafixAll --check  # Check import ordering and lint rules
sbt scalafixAll          # Auto-fix import ordering
```
