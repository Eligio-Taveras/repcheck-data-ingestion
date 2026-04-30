#!/bin/bash
# =============================================================================
# Pub/Sub Emulator — Topic & Subscription Initialization
# =============================================================================
# Creates the topics and subscriptions needed by the bill pipelines.
# Waits for the emulator to be ready before creating resources.
# Called by docker-compose.local.yml as an init container.
# =============================================================================

set -e

EMULATOR_HOST="${PUBSUB_EMULATOR_HOST:-pubsub-emulator:8085}"
PROJECT_ID="${PUBSUB_PROJECT_ID:-repcheck-local}"

echo "Waiting for Pub/Sub emulator at ${EMULATOR_HOST}..."
until curl -sf "http://${EMULATOR_HOST}/" > /dev/null 2>&1; do
  sleep 1
done
echo "Pub/Sub emulator is ready."

# Create a topic or subscription, ignoring 409 (already exists)
create_resource() {
  local url="$1"
  shift
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$url" "$@")
  if [ "$http_code" = "200" ] || [ "$http_code" = "409" ]; then
    return 0
  else
    echo "ERROR: PUT $url returned HTTP $http_code"
    return 1
  fi
}

# Topic: bill-text-available (checker → text pipeline)
echo "Creating topic: bill-text-available"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/bill-text-available"

# Topic: bill-text-available-dead-letter (for messages that exceed max delivery attempts)
echo "Creating topic: bill-text-available-dead-letter"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/bill-text-available-dead-letter"

# Subscription: bill-text-available-sub (text pipeline reads from this)
# Dead-letter after 5 nacks (the cloud Pub/Sub minimum): TextDownloadFailed for old congresses
# (e.g., 102-HR-3412 returning HTTP 400 because the bill text was never digitized) was previously
# redelivered indefinitely, blocking the queue. Sticky failures now exit to bill-text-available-dead-letter
# instead of looping forever.
#
# retryPolicy 2s/30s rather than the 10s/600s used for bill-events/vote-events: bill-text failures
# are mostly deterministic (govinfo 400 for missing texts), so long backoffs don't help — they just
# delay the inevitable DLQ exit. 5 attempts at 2-30s ≈ ~60s wall-clock to dead-letter, vs ~5+ minutes
# under the default 10s/600s.
echo "Creating subscription: bill-text-available-sub"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/bill-text-available-sub" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"projects/${PROJECT_ID}/topics/bill-text-available\",\"ackDeadlineSeconds\":60,\"deadLetterPolicy\":{\"deadLetterTopic\":\"projects/${PROJECT_ID}/topics/bill-text-available-dead-letter\",\"maxDeliveryAttempts\":5},\"retryPolicy\":{\"minimumBackoff\":\"2s\",\"maximumBackoff\":\"30s\"}}"

# Topic: bill-text-ingested (text pipeline publishes downstream events)
echo "Creating topic: bill-text-ingested"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/bill-text-ingested"

# Topic: member-updated (member profile pipeline + LIS refresher publish when member data changes)
echo "Creating topic: member-updated"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/member-updated"

# Subscription: member-updated-sub (downstream consumers of member update events)
echo "Creating subscription: member-updated-sub"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/member-updated-sub" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"projects/${PROJECT_ID}/topics/member-updated\",\"ackDeadlineSeconds\":60}"

# Topic: vote-events (votes-pipeline publishes VoteRecordedEvent on new/updated votes)
echo "Creating topic: vote-events"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/vote-events"

# Topic: vote-events-dead-letter (for failed VoteRecordedEvent deliveries)
echo "Creating topic: vote-events-dead-letter"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/vote-events-dead-letter"

# Subscription: vote-recorded-sub (downstream consumers of VoteRecordedEvent — e.g. scoring engine)
echo "Creating subscription: vote-recorded-sub"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/vote-recorded-sub" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"projects/${PROJECT_ID}/topics/vote-events\",\"ackDeadlineSeconds\":60,\"deadLetterPolicy\":{\"deadLetterTopic\":\"projects/${PROJECT_ID}/topics/vote-events-dead-letter\",\"maxDeliveryAttempts\":5}}"

echo "Pub/Sub topics and subscriptions created successfully."
