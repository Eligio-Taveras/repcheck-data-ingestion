#!/usr/bin/env bash
# Generates compressed agent documentation from human-readable docs.
# Run manually after updating docs: ./scripts/generate-agent-docs.sh
# Requires: ANTHROPIC_API_KEY environment variable.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Compression threshold (minimum average % reduction required). Override via first arg.
THRESHOLD="${1:-0.20}"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "ERROR: ANTHROPIC_API_KEY is not set."
  echo "Set it in your shell profile or run: export ANTHROPIC_API_KEY='your-key'"
  exit 1
fi

echo "Generating compressed agent docs (threshold: ${THRESHOLD})..."

cd "$PROJECT_ROOT"
export PROJECT_ROOT

sbt "docGenerator/run $THRESHOLD"

echo "Agent docs generated. Review changes with: git diff .claude/agent-docs/"
