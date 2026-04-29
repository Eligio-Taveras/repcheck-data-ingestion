---
name: ci-diagnose
description: Pull failing CI logs for a PR, classify the failure, and propose a specific fix. Stops at proposal — never auto-applies. Pairs with the build-fix loop and CI freshness rules.
---

# CI Diagnose

Given a PR number, perform structured diagnosis.

## Steps

1. **Get the latest commit SHA on the PR**:
   ```bash
   SHA=$(gh pr view <pr> --json headRefOid -q .headRefOid)
   REPO=$(gh pr view <pr> --json baseRepository -q '.baseRepository.nameWithOwner')
   ```

2. **Apply the CI freshness rule** before reading any check status. Poll until at least one check run for `$SHA` exists, then until all are terminal:
   ```bash
   gh api repos/$REPO/commits/$SHA/check-runs --jq '.check_runs[] | {name, status, conclusion}'
   ```

3. **Identify failed runs** (`conclusion` in `failure`, `cancelled`, `timed_out`, `action_required`):
   ```bash
   gh api repos/$REPO/commits/$SHA/check-runs --jq '.check_runs[] | select(.conclusion != "success") | {id, name, conclusion, html_url}'
   ```
   If all green, report and stop.

4. **For each failed run, pull logs**:
   ```bash
   gh run view <run_id> --log-failed | tail -200
   ```
   Capture the last meaningful error context.

5. **Classify the failure**. Pick the closest match:
   - **Compile error** (Scala/TS/etc.) — file + line + error type
   - **Test failure** — spec name + assertion that failed + relevant input
   - **scalafmt** — list files needing reformatting; fix is `sbt scalafmtAll`
   - **scalafix** — rule violations + locations; fix is `sbt scalafixAll`
   - **Coverage gate** — files below threshold + percentages; per the Coverage rule, FIRST move is testability refactoring, not adding tests
   - **Codecov patch coverage** — same as above but enforced by Codecov bot
   - **Lint / type-check** — tool name + first failure
   - **Docker / infrastructure** — usually environmental, not the code; check the runner image, network, or service availability
   - **Timeout** — long-running test or hung process; investigate the specific test
   - **Multiple classes** — list the dominant one + a brief note about the others

6. **Propose a specific fix** for the dominant class. Be concrete:
   - "Add `import x.y.Z` at line N of file F to satisfy the type at line M"
   - "Run `sbt scalafmtAll`, then commit"
   - "The test at FooSpec:42 expects `bar=true` but `bar` is now `Option[Boolean]`; update either the test or the production code"

7. **Stop**. Surface the classification + proposed fix to the user. Wait for user direction before applying the fix. The user may have additional context, may want to read logs themselves, or may prefer a different fix.

## Antipatterns

- Reporting "CI failed" without classification.
- Auto-applying a fix without surfacing the proposal.
- Skipping the freshness check and reporting on stale data.
- Pulling whole log files instead of `--log-failed` (drowns in noise).
