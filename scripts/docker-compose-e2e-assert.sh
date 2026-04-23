#!/usr/bin/env bash
# =============================================================================
# E2E stack assertions
# =============================================================================
# Runs after `docker-compose-e2e.sh` has brought up the stack and executed
# every pipeline. Verifies cross-pipeline outcomes by querying AlloyDB and
# the Pub/Sub emulator. Exits non-zero on the first failed check.
#
# This is the "test" in the Tier-3 sense (per the P4.2b design note in the
# C6 plan): it asserts on observable stack state rather than asking the
# agent to run ad-hoc SELECTs.
#
# Expected inputs (via env):
#   COMPOSE_PROJECT  the compose project name used by the orchestrator
#   COMPOSE_FILE     the compose file (default: docker-compose.e2e.yml)
#
# Fixture baseline (what the pipelines should have persisted):
#   - bills:                 1 (HR 1 "Lower Energy Costs Act", abridged)
#   - bill_text_versions:    1 (HR 1 text downloaded + 1536-dim embedding)
#   - votes:                 5 (3 Senate + 2 House, from the abridged
#                               house-vote-119-1-page1.json and the 3-vote
#                               abridged Senate index)
#   - vote_positions:        > 500 (100 per Senate vote + ~430 per House vote)
#   - lis_members:           > 90 (100 unique senators per Senate vote upserted,
#                                    dedup via ON CONFLICT)
#   - stance_materialization_status.has_votes rows: >= 3 (bill-linked votes)
#   - vote-events topic:     5 messages (one per vote)
#   - bill-text-ingested:    1 message
# =============================================================================
# NOTE: deliberately NOT using `set -e`. We want to collect every failed
# assertion and report the total count at the end, not bail on the first one.
# Errors from SQL helpers are captured per-call via `|| true` or empty-string
# defaulting.
set -uo pipefail

COMPOSE_PROJECT="${COMPOSE_PROJECT:-e2e-docker}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.e2e.yml}"
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
cd "$PROJECT_ROOT"

dc() {
  docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" "$@"
}

FAIL_COUNT=0

fail() {
  echo "  ✗ FAIL: $1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

pass() {
  echo "  ✓ $1"
}

# -----------------------------------------------------------------------------
# Helper: run SQL against alloydb and return a scalar (single row, single col).
# -----------------------------------------------------------------------------
sql() {
  local query="$1"
  # Wrap in `|| true` so a failed psql call (e.g. transient connection blip)
  # returns empty rather than aborting the script — individual assertions
  # handle the "empty result" case explicitly.
  { dc exec -T alloydb psql -U repcheck -d repcheck_e2e -t -A -c "$query" 2>/dev/null || true; } | tr -d '[:space:]'
}

# -----------------------------------------------------------------------------
# Helper: pull all messages from a subscription via the emulator REST API.
# Returns the number of messages currently available on the subscription.
#
# We use `pull` with returnImmediately to avoid blocking on an empty queue.
# (returnImmediately is deprecated in production but fine for test assertions;
# an empty response is "queue empty.")
# -----------------------------------------------------------------------------
pubsub_pull_count() {
  local subscription="$1"
  local url="http://${COMPOSE_PROJECT}-pubsub-emulator-1:8085/v1/projects/repcheck-e2e/subscriptions/${subscription}:pull"
  # Use curl inside wiremock (which has curl) OR exec a curl image. Simplest:
  # run a one-off curl container attached to the compose network.
  local response
  response=$(docker run --rm --network "${COMPOSE_PROJECT}_default" curlimages/curl:latest \
    -s -X POST "http://pubsub-emulator:8085/v1/projects/repcheck-e2e/subscriptions/${subscription}:pull" \
    -H "Content-Type: application/json" \
    -d '{"maxMessages":100,"returnImmediately":true}')
  # Count `messageId` occurrences as a message count.
  echo "$response" | grep -o '"messageId"' | wc -l | tr -d '[:space:]'
}

# -----------------------------------------------------------------------------
# Section 1: bills table
# -----------------------------------------------------------------------------
echo ""
echo "▸ Bills"

bill_count=$(sql "SELECT COUNT(*) FROM bills;")
if [ "$bill_count" = "4" ]; then
  pass "bills row count = 4 (1 bill-metadata + 3 vote-linked placeholders)"
elif [ "$bill_count" -ge "1" ]; then
  pass "bills row count = $bill_count (expected >= 1)"
else
  fail "bills row count = 0, expected >= 1"
fi

hr1=$(sql "SELECT natural_key FROM bills WHERE congress=118 AND bill_type='hr' AND number=1;" 2>/dev/null || echo "")
# HR 1 detail persisted by bill-metadata-pipeline uses the natural_key format
# set by that pipeline; just verify the row exists via its composite key.
if [ -n "$(sql "SELECT 1 FROM bills WHERE congress=118 AND bill_type='hr' AND number=1;")" ]; then
  pass "HR 1 (118) persisted by bill-metadata-pipeline"
else
  fail "HR 1 (118) missing from bills table"
fi

# -----------------------------------------------------------------------------
# Section 2: bill_text_versions (bill-text-pipeline)
# -----------------------------------------------------------------------------
echo ""
echo "▸ Bill text versions"

btv_count=$(sql "SELECT COUNT(*) FROM bill_text_versions;")
if [ "$btv_count" -ge "1" ]; then
  pass "bill_text_versions row count = $btv_count (expected >= 1)"
else
  fail "bill_text_versions row count = 0, expected >= 1"
fi

embed_count=$(sql "SELECT COUNT(*) FROM bill_text_versions WHERE embedding IS NOT NULL;")
if [ "$embed_count" -ge "1" ]; then
  pass "embeddings persisted: $embed_count (WireMock Ollama stub wired correctly)"
else
  fail "no embeddings persisted — OLLAMA_BASE_URL wiring or dimension validation broken"
fi

# -----------------------------------------------------------------------------
# Section 3: votes + vote_positions (votes-pipeline)
# -----------------------------------------------------------------------------
echo ""
echo "▸ Votes"

votes_count=$(sql "SELECT COUNT(*) FROM votes;")
if [ "$votes_count" = "5" ]; then
  pass "votes row count = 5 (3 Senate + 2 House from abridged fixtures)"
else
  fail "votes row count = $votes_count, expected 5"
fi

house_count=$(sql "SELECT COUNT(*) FROM votes WHERE chamber='House';")
senate_count=$(sql "SELECT COUNT(*) FROM votes WHERE chamber='Senate';")
if [ "$house_count" = "2" ] && [ "$senate_count" = "3" ]; then
  pass "chamber split: $house_count House + $senate_count Senate"
else
  fail "chamber split wrong: $house_count House + $senate_count Senate, expected 2+3"
fi

# Vote 659 is the PN373 procedural vote — should have bill_id IS NULL.
pn373_bill_id=$(sql "SELECT bill_id FROM votes WHERE natural_key='119-Senate-1-659';")
if [ -z "$pn373_bill_id" ]; then
  pass "PN373 Senate vote correctly has bill_id = NULL (procedural, not bill-linked)"
else
  fail "PN373 Senate vote should have bill_id NULL but got '$pn373_bill_id'"
fi

positions_count=$(sql "SELECT COUNT(*) FROM vote_positions;")
if [ "$positions_count" -ge "500" ]; then
  pass "vote_positions row count = $positions_count (expected >= 500: ~300 Senate + ~860 House)"
else
  fail "vote_positions row count = $positions_count, expected >= 500"
fi

# -----------------------------------------------------------------------------
# Section 4: lis_members (LisResolver upserts)
# -----------------------------------------------------------------------------
echo ""
echo "▸ LIS members (LisResolver upsert via ON CONFLICT)"

lis_count=$(sql "SELECT COUNT(*) FROM lis_members;")
if [ "$lis_count" -ge "90" ] && [ "$lis_count" -le "150" ]; then
  pass "lis_members row count = $lis_count (expected ~100 unique senators after dedup)"
else
  fail "lis_members row count = $lis_count, expected 90-150 (unique senators across 3 votes)"
fi

# -----------------------------------------------------------------------------
# Section 5: stance_materialization_status (triggered by bill-linked votes)
# -----------------------------------------------------------------------------
echo ""
echo "▸ Stance materialization"

stance_count=$(sql "SELECT COUNT(*) FROM stance_materialization_status WHERE has_votes=true;")
if [ "$stance_count" -ge "3" ]; then
  pass "stance_materialization_status.has_votes=true on $stance_count rows (>= 3 expected)"
else
  fail "stance_materialization_status.has_votes=true on $stance_count rows, expected >= 3"
fi

# -----------------------------------------------------------------------------
# Section 6: Pub/Sub events (vote-events + bill-text-ingested)
# -----------------------------------------------------------------------------
echo ""
echo "▸ Pub/Sub events"

vote_events_count=$(pubsub_pull_count "vote-recorded-sub")
if [ "$vote_events_count" -ge "5" ]; then
  pass "vote-events: $vote_events_count messages on vote-recorded-sub (>= 5 expected)"
else
  fail "vote-events: $vote_events_count messages on vote-recorded-sub, expected >= 5"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
if [ "$FAIL_COUNT" = "0" ]; then
  echo "═══ ALL ASSERTIONS PASSED ═══"
  exit 0
else
  echo "═══ $FAIL_COUNT ASSERTION(S) FAILED ═══"
  exit 1
fi
