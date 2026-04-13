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

# Topic: bill-text-available (checker → text pipeline)
echo "Creating topic: bill-text-available"
curl -sf -X PUT "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/bill-text-available" > /dev/null

# Subscription: bill-text-available-sub (text pipeline reads from this)
echo "Creating subscription: bill-text-available-sub"
curl -sf -X PUT "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/bill-text-available-sub" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"projects/${PROJECT_ID}/topics/bill-text-available\",\"ackDeadlineSeconds\":60}" > /dev/null

# Topic: bill-text-ingested (text pipeline publishes downstream events)
echo "Creating topic: bill-text-ingested"
curl -sf -X PUT "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/bill-text-ingested" > /dev/null

echo "Pub/Sub topics and subscriptions created successfully."
