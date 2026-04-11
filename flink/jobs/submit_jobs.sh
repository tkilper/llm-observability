#!/bin/bash
# Submits all three PyFlink jobs to the Flink cluster.
# Runs inside the flink-job-submitter container after the cluster is healthy.
set -euo pipefail

FLINK=/opt/flink/bin/flink
JOBS=/opt/flink/jobs
JM=flink-jobmanager:8081

echo "Waiting for Flink REST API at ${JM}..."
until curl -sf "http://${JM}/overview" > /dev/null; do
  sleep 3
done
echo "Flink cluster is ready."

submit() {
  local name=$1
  local script=$2
  echo ""
  echo ">>> Submitting: ${name}"
  $FLINK run \
    -m "${JM}" \
    -py "${JOBS}/${script}" \
    -D python.executable="${PYFLINK_PYTHON:-python3.9}"
  echo ">>> ${name} submitted."
}

submit "event_sink"        "event_sink.py"
submit "cost_rollups"      "cost_rollups.py"
submit "anomaly_detection" "anomaly_detection.py"

echo ""
echo "All jobs submitted. View at http://${JM}"
