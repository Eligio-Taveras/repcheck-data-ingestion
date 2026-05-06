---
name: commit-and-pr
description: Bundle the standard commit + push + open PR + verify CI workflow into one invocation. Wraps pushToPR/CreatePR with the location, branch-state, and CI freshness rules.
---

# Commit and PR

Given a title and body, perform the bundled workflow.

## Steps

1. **Verify location** (per the location rule):
   ```bash
   pwd
   BRANCH=$(git branch --show-current)
   git status --short
   ```
   - Abort if `$BRANCH` is `main` or `master`.
   - Abort if working tree is clean (nothing to commit).

2. **Verify branch is not merged** (overlap with the PreToolUse blocking hook):
   ```bash
   gh pr list --state merged --head "$BRANCH" --json number -q '.[0].number'
   ```
   If a PR number comes back, abort and instruct: create a new branch from latest main first.

3. **Capture pre-commit SHA** for later CI verification:
   ```bash
   SHA_BEFORE_COMMIT=$(git rev-parse HEAD)
   ```

4. **Stage and commit**. Default: `git add -A`. Confirm with the user if the diff includes unrelated files.
   ```bash
   git commit -m "<title>" -m "<body>"
   SHA_AFTER_COMMIT=$(git rev-parse HEAD)
   ```

5. **Push via the repo's CI script if available**:
   - If `scripts/ci-functions.sh` exists in the repo, source it and call:
     - `CreatePR "<title>" "<body>"` for a NEW PR (this is the canonical pattern in RepCheck repos)
     - `pushToPR` to update an existing PR
   - Otherwise: `git push -u origin "$BRANCH"` then `gh pr create --title "<title>" --body "<body>"`.

6. **Verify CI freshness** (per the verify-CI rule). Poll for the new SHA's check runs:
   ```bash
   REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
   # Wait for at least one check-run for $SHA_AFTER_COMMIT to appear
   # Then wait for all to reach terminal conclusion
   gh api "repos/$REPO/commits/$SHA_AFTER_COMMIT/check-runs"
   ```

7. **Report**:
   - PR URL (from `gh pr view --json url`)
   - Each check name + conclusion (success / failure / cancelled / etc.)
   - If any non-success: invoke the `/ci-diagnose` skill for the failed checks.

## When to invoke

- The user says "commit and push", "open a PR", or "ship this"
- Wrapping up a task with uncommitted changes ready to ship
- Any time the standard sequence applies

## Antipatterns

- Skipping the location check and committing in the wrong worktree.
- Pushing to a merged branch (the blocking hook stops this, but the skill should not even try).
- Reporting CI status from the first response without freshness verification.
- `git add -A` blindly when the working tree contains unrelated edits — confirm scope first.
