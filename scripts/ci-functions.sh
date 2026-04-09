#!/usr/bin/env bash
# =============================================================================
# CI-gated Git functions for the votr/RepCheck project.
#
# Source this file in your shell profile or at the start of a session:
#   source scripts/ci-functions.sh
#
# Provides:
#   CreatePR  <title> <body>   — run CI checks, push, create PR
#   pushToPR                   — run CI checks, push to current PR branch
#   runDocCompressor [thresh]  — regenerate compressed agent docs
#   syncDocsToG8 [g8-path]    — sync docs to repcheck-g8 template repo
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# runDocCompressor [threshold]
#
# Regenerates compressed agent docs from docs/ into .claude/agent-docs/.
# Reads ANTHROPIC_API_KEY from PowerShell user environment variables
# (Windows) so it works even when the key isn't in the bash session.
#
# Usage:
#   runDocCompressor          # default 20% threshold
#   runDocCompressor 0.30     # custom 30% threshold
# ---------------------------------------------------------------------------
runDocCompressor() {
  local threshold="${1:-0.20}"
  local sbt_cmd
  sbt_cmd="$(command -v sbt || echo "sbt.bat")"

  # Resolve ANTHROPIC_API_KEY from PowerShell user env if not already set
  if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    local key
    key="$(powershell.exe -Command "[System.Environment]::GetEnvironmentVariable('ANTHROPIC_API_KEY', 'User')" | tr -d '\r\n')"
    if [ -z "$key" ]; then
      echo "✗ ANTHROPIC_API_KEY not found in bash or Windows user environment."
      echo "  Set it with: [System.Environment]::SetEnvironmentVariable('ANTHROPIC_API_KEY', 'sk-ant-...', 'User')"
      return 1
    fi
    export ANTHROPIC_API_KEY="$key"
    echo "▸ Loaded ANTHROPIC_API_KEY from Windows user environment"
  fi

  # Resolve project root (git repo root)
  local project_root
  project_root="$(git rev-parse --show-toplevel)"
  export PROJECT_ROOT="$project_root"

  echo "═══════════════════════════════════════════════"
  echo "  Running doc compressor (threshold: $threshold)"
  echo "═══════════════════════════════════════════════"

  if ! "$sbt_cmd" "docGenerator/run $threshold"; then
    echo "✗ DOC COMPRESSION FAILED."
    return 1
  fi

  echo ""
  echo "✓ Compressed docs written to .claude/agent-docs/"
}

# ---------------------------------------------------------------------------
# Internal: run all CI checks that GitHub Actions will run.
# Returns 0 if all pass, non-zero on first failure.
# ---------------------------------------------------------------------------
_run_ci_checks() {
  local sbt_cmd
  sbt_cmd="$(command -v sbt || echo "sbt.bat")"

  echo "═══════════════════════════════════════════════"
  echo "  Running CI checks locally before pushing"
  echo "═══════════════════════════════════════════════"

  echo ""
  echo "▸ sbt compile"
  if ! "$sbt_cmd" compile; then
    echo "✗ COMPILE FAILED — fix errors before pushing."
    return 1
  fi

  echo ""
  echo "▸ sbt test"
  if ! "$sbt_cmd" test; then
    echo "✗ TESTS FAILED — fix failures before pushing."
    return 1
  fi

  echo ""
  echo "▸ sbt scalafmtCheckAll"
  if ! "$sbt_cmd" scalafmtCheckAll; then
    echo "✗ FORMATTING FAILED — run 'sbt scalafmtAll' then retry."
    return 1
  fi

  echo ""
  echo "▸ sbt scalafixAll --check"
  if ! "$sbt_cmd" "scalafixAll --check"; then
    echo "✗ SCALAFIX FAILED — run 'sbt scalafixAll' then retry."
    return 1
  fi

  echo ""
  echo "═══════════════════════════════════════════════"
  echo "  All CI checks passed"
  echo "═══════════════════════════════════════════════"
  return 0
}

# ---------------------------------------------------------------------------
# CreatePR <title> <body>
#
# 1. Runs all CI checks (compile, test, scalafmt, scalafix)
# 2. Checks branch hygiene (not already merged)
# 3. Pushes current branch to origin
# 4. Creates a PR via gh cli
#
# Usage:
#   CreatePR "Fix the widget" "## Summary
#   - Fixed the widget
#
#   ## Test plan
#   - [x] Unit tests pass"
# ---------------------------------------------------------------------------
CreatePR() {
  if [ $# -lt 2 ]; then
    echo "Usage: CreatePR <title> <body>"
    echo ""
    echo "Example:"
    echo '  CreatePR "Fix the widget" "## Summary'
    echo '  - Fixed the widget'
    echo ''
    echo '  ## Test plan'
    echo '  - [x] Unit tests pass"'
    return 1
  fi

  local title="$1"
  local body="$2"
  local branch
  branch="$(git rev-parse --abbrev-ref HEAD)"

  if [ "$branch" = "main" ] || [ "$branch" = "master" ]; then
    echo "✗ Cannot create a PR from $branch. Checkout a feature branch first."
    return 1
  fi

  # Check if this branch was already merged
  local merged
  merged="$(gh pr list --state merged --head "$branch" --json number --jq 'length')"
  if [ "$merged" != "0" ]; then
    echo "✗ Branch '$branch' has already been merged. Create a new branch."
    return 1
  fi

  # Run CI checks
  if ! _run_ci_checks; then
    return 1
  fi

  # Push
  echo ""
  echo "▸ git push -u origin $branch"
  if ! git push -u origin "$branch"; then
    echo "✗ PUSH FAILED."
    return 1
  fi

  # Create PR
  echo ""
  echo "▸ Creating PR..."
  gh pr create --title "$title" --body "$body"
}

# ---------------------------------------------------------------------------
# pushToPR
#
# 1. Runs all CI checks (compile, test, scalafmt, scalafix)
# 2. Pushes current branch to origin
#
# Intended for pushing updates to an existing PR branch.
#
# Usage:
#   pushToPR
# ---------------------------------------------------------------------------
pushToPR() {
  local branch
  branch="$(git rev-parse --abbrev-ref HEAD)"

  if [ "$branch" = "main" ] || [ "$branch" = "master" ]; then
    echo "✗ Cannot push directly to $branch."
    return 1
  fi

  # Run CI checks
  if ! _run_ci_checks; then
    return 1
  fi

  # Push
  echo ""
  echo "▸ git push"
  if ! git push; then
    echo "✗ PUSH FAILED."
    return 1
  fi

  echo ""
  echo "Pushed to $(git remote get-url origin) ($branch)"
}

# ---------------------------------------------------------------------------
# syncDocsToG8 [g8-repo-path]
#
# Syncs documentation, compressed agent docs, CLAUDE.md, and ci-functions.sh
# from the votr repo to the repcheck-g8 Giter8 template repo.
#
# The votr repo is the source of truth. This function copies the relevant
# files into the G8 template layout (src/main/g8/), creates a branch,
# commits, pushes, and opens a PR.
#
# Usage:
#   syncDocsToG8                                          # default sibling path
#   syncDocsToG8 /c/Users/me/repos/repcheck-g8           # custom path
# ---------------------------------------------------------------------------
syncDocsToG8() {
  local votr_root
  votr_root="$(git rev-parse --show-toplevel)"
  local g8_root="${1:-$(dirname "$votr_root")/repcheck-g8}"
  local g8_template="$g8_root/src/main/g8"

  # Validate paths
  if [ ! -d "$votr_root/docs" ]; then
    echo "✗ Cannot find docs/ in votr repo at $votr_root"
    return 1
  fi
  if [ ! -d "$g8_template" ]; then
    echo "✗ Cannot find G8 template at $g8_template"
    echo "  Expected repcheck-g8 repo at: $g8_root"
    echo "  Pass the path explicitly: syncDocsToG8 /path/to/repcheck-g8"
    return 1
  fi

  echo "═══════════════════════════════════════════════"
  echo "  Syncing docs from votr → repcheck-g8"
  echo "═══════════════════════════════════════════════"
  echo ""
  echo "  Source: $votr_root"
  echo "  Target: $g8_template"
  echo ""

  # Sync docs/
  echo "▸ Syncing docs/"
  rm -rf "$g8_template/docs"
  cp -r "$votr_root/docs" "$g8_template/docs"

  # Sync .claude/agent-docs/
  echo "▸ Syncing .claude/agent-docs/"
  rm -rf "$g8_template/.claude/agent-docs"
  mkdir -p "$g8_template/.claude"
  cp -r "$votr_root/.claude/agent-docs" "$g8_template/.claude/agent-docs"

  # Sync CLAUDE.md
  echo "▸ Syncing CLAUDE.md"
  cp "$votr_root/CLAUDE.md" "$g8_template/CLAUDE.md"

  # Sync scripts/ci-functions.sh
  echo "▸ Syncing scripts/ci-functions.sh"
  mkdir -p "$g8_template/scripts"
  cp "$votr_root/scripts/ci-functions.sh" "$g8_template/scripts/ci-functions.sh"

  echo ""

  # Create branch, commit, push, and create PR in the G8 repo
  local branch="sync-docs-from-votr-$(date +%Y%m%d-%H%M%S)"
  local prev_dir="$PWD"
  cd "$g8_root"

  echo "▸ Creating branch: $branch"
  git checkout main
  git pull origin main
  git checkout -b "$branch"

  git add -A src/main/g8/docs src/main/g8/.claude/agent-docs src/main/g8/CLAUDE.md src/main/g8/scripts/ci-functions.sh
  local changed
  changed="$(git diff --cached --stat | tail -1)"

  if [ -z "$changed" ]; then
    echo "✓ No changes to sync — G8 repo is already up to date."
    git checkout main
    git branch -d "$branch"
    cd "$prev_dir"
    return 0
  fi

  echo "▸ Changes: $changed"
  git commit -m "Sync docs, compressed agent docs, CLAUDE.md, and ci-functions from votr"

  echo ""
  echo "▸ Pushing and creating PR..."
  git push -u origin "$branch"
  gh pr create \
    --title "Sync docs from votr ($(date +%Y-%m-%d))" \
    --body "Automated sync of documentation files from the votr source-of-truth repo.

## Files synced
- \`docs/\` — full documentation tree
- \`.claude/agent-docs/\` — compressed agent docs
- \`CLAUDE.md\` — agent routing guide
- \`scripts/ci-functions.sh\` — CI shell functions

## Source
votr repo at commit \`$(cd "$votr_root" && git rev-parse --short HEAD)\`"

  cd "$prev_dir"
  echo ""
  echo "✓ Docs synced and PR created in repcheck-g8."
}
