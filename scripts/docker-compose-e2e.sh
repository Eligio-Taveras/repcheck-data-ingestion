#!/usr/bin/env bash
# =============================================================================
# Full-stack E2E orchestrator
# =============================================================================
# Brings up docker-compose.e2e.yml, runs each pipeline to completion, then
# executes the assertion script. Proves that cross-container communication
# (DB, Pub/Sub, WireMock) is correctly wired end-to-end.
#
# Not a correctness test of the pipelines themselves — that's unit + class-
# level + the per-pipeline E2E specs. This script is a _wiring_ test.
#
# Usage:
#   ./scripts/docker-compose-e2e.sh
#
# Environment:
#   COMPOSE_PROJECT  compose project name (default: e2e-docker)
#                    Namespaces all containers/networks/volumes so concurrent
#                    runs from different worktrees don't collide.
#   KEEP_STACK=1     skip teardown (for post-run debugging)
# =============================================================================
set -euo pipefail

COMPOSE_PROJECT="${COMPOSE_PROJECT:-e2e-docker}"
COMPOSE_FILE="docker-compose.e2e.yml"
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
cd "$PROJECT_ROOT"

dc() {
  docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" "$@"
}

teardown() {
  if [ "${KEEP_STACK:-0}" = "1" ]; then
    echo "▸ KEEP_STACK=1 — leaving stack running"
    return 0
  fi
  echo ""
  echo "▸ Tearing down stack (use KEEP_STACK=1 to skip)"
  dc down -v --remove-orphans 2>&1 | sed 's/^/    /' || true
}
trap teardown EXIT

section() { echo ""; echo "═══ $1 ═══"; }

# -----------------------------------------------------------------------------
# 1. Ensure fat JARs exist. Dockerfiles COPY them in — if they're missing the
#    `docker compose up --build` step later will fail. We check first and only
#    run sbt assembly if any JAR is absent.
#
#    SKIP_BUILD=1: bypass the build entirely even if a JAR is missing (useful
#    when caller has already built them via a different sbt invocation, e.g.
#    on Windows hosts where sbt.bat has PATH issues under Git-Bash).
# -----------------------------------------------------------------------------
section "Checking fat JARs"

REQUIRED_JARS=(
  "bill-metadata-pipeline/target/scala-3.7.3/bill-metadata-pipeline.jar"
  "bill-text-availability-checker/target/scala-3.7.3/bill-text-availability-checker.jar"
  "bill-text-pipeline/target/scala-3.7.3/bill-text-pipeline.jar"
  "votes-pipeline/target/scala-3.7.3/votes-pipeline.jar"
)

MISSING_JARS=()
for jar in "${REQUIRED_JARS[@]}"; do
  if [ ! -f "$jar" ]; then
    MISSING_JARS+=("$jar")
  fi
done

if [ "${#MISSING_JARS[@]}" -gt "0" ] && [ "${SKIP_BUILD:-0}" != "1" ]; then
  section "Building missing fat JARs: ${MISSING_JARS[*]}"
  if command -v sbt >/dev/null 2>&1; then
    sbt \
      "billMetadataPipeline/assembly" \
      "billTextAvailabilityChecker/assembly" \
      "billTextPipeline/assembly" \
      "votesPipeline/assembly"
  else
    echo "ERROR: sbt not found on PATH. Build manually via:"
    echo "  sbt 'billMetadataPipeline/assembly' 'billTextAvailabilityChecker/assembly' \\"
    echo "      'billTextPipeline/assembly' 'votesPipeline/assembly'"
    echo "Then re-run this script (optionally with SKIP_BUILD=1)."
    exit 1
  fi
elif [ "${#MISSING_JARS[@]}" -gt "0" ]; then
  echo "ERROR: SKIP_BUILD=1 but JARs missing: ${MISSING_JARS[*]}"
  exit 1
else
  echo "  ✓ all 4 fat JARs present"
fi

# -----------------------------------------------------------------------------
# 2. Bring up infrastructure.
# -----------------------------------------------------------------------------
section "Starting infrastructure (alloydb, pubsub-emulator, wiremock)"
dc up -d --build alloydb pubsub-emulator wiremock

section "Waiting for health checks"
for service in alloydb pubsub-emulator wiremock; do
  container="${COMPOSE_PROJECT}-${service}-1"
  echo -n "  waiting for $container ... "
  until docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null | grep -q "^healthy$"; do
    sleep 2
  done
  echo "healthy"
done

# -----------------------------------------------------------------------------
# 3. Run init containers (migrations, pubsub topic creation).
# -----------------------------------------------------------------------------
section "Running init containers"
dc run --rm --no-deps db-migrations
dc run --rm --no-deps pubsub-init

# -----------------------------------------------------------------------------
# 4. Run each pipeline sequentially. Each is one-shot (exits after one run).
# -----------------------------------------------------------------------------
section "Running bill-metadata-pipeline"
dc run --rm --no-deps bill-metadata-pipeline

section "Running bill-text-availability-checker"
dc run --rm --no-deps bill-text-availability-checker

section "Running bill-text-pipeline"
dc run --rm --no-deps bill-text-pipeline

section "Running votes-pipeline"
dc run --rm --no-deps votes-pipeline

# -----------------------------------------------------------------------------
# 5. Run assertions against the resulting stack state.
# -----------------------------------------------------------------------------
section "Running assertions"
COMPOSE_PROJECT="$COMPOSE_PROJECT" COMPOSE_FILE="$COMPOSE_FILE" \
  ./scripts/docker-compose-e2e-assert.sh

section "E2E stack test passed"
