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

# Subscription: bill-text-available-sub (text pipeline reads from this)
echo "Creating subscription: bill-text-available-sub"
create_resource "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/bill-text-available-sub" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"projects/${PROJECT_ID}/topics/bill-text-available\",\"ackDeadlineSeconds\":60}"

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

echo "Pub/Sub topics and subscriptions created successfully."
