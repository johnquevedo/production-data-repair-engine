#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root_dir"

mkdir -p build/fault-injection

if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  java_command="${JAVA_HOME}/bin/java"
elif [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
  java_command=/opt/homebrew/opt/openjdk/bin/java
else
  java_command=java
fi

jar=target/production-data-repair-engine.jar
killed_pid=""
survivor_pid=""

cleanup_processes() {
  if [[ -n "$killed_pid" ]]; then kill "$killed_pid" 2>/dev/null || true; fi
  if [[ -n "$survivor_pid" ]]; then kill "$survivor_pid" 2>/dev/null || true; fi
}
trap cleanup_processes EXIT

docker compose down --volumes --remove-orphans
docker compose up -d --wait postgres kafka
mvn -q -DskipTests package

"$java_command" -jar "$jar" seed --corrupted 5000 --controls 500

"$java_command" -jar "$jar" publish --replay 500 --batch-size 100 \
  --batch-delay-ms 100 >build/fault-injection/publisher.log 2>&1 &
publisher_pid=$!
sleep 1
docker compose restart kafka
docker compose up -d --wait kafka
wait "$publisher_pid"

"$java_command" -jar "$jar" consume --group fault-lineage \
  >build/fault-injection/consumer.log 2>&1 &
consumer_pid=$!
sleep 1
docker compose restart kafka
docker compose up -d --wait kafka
wait "$consumer_pid"

"$java_command" -jar "$jar" publish --replay 1000
"$java_command" -jar "$jar" consume --group fault-lineage

plan_output="$("$java_command" -jar "$jar" plan --bad-config bad-fees-v2 \
  --target-fee-bps 290 --partitions 32)"
job_id="$(printf '%s\n' "$plan_output" | sed -n 's/^JOB_ID=//p')"
if [[ -z "$job_id" ]]; then
  echo "Failed to resolve repair job id" >&2
  exit 1
fi

"$java_command" -jar "$jar" worker --job "$job_id" --threads 1 \
  --delay-after-claim-ms 10000 --stale-after-ms 1500 \
  --ready-file build/fault-injection/killed.ready \
  >build/fault-injection/killed-worker.log 2>&1 &
killed_pid=$!

"$java_command" -jar "$jar" worker --job "$job_id" --threads 3 \
  --delay-after-claim-ms 2 --stale-after-ms 1500 \
  --ready-file build/fault-injection/survivor.ready \
  >build/fault-injection/survivor-worker.log 2>&1 &
survivor_pid=$!

for _ in $(seq 1 120); do
  checkpoint_count="$(docker compose exec -T postgres psql -U repairs -d repairs -Atc \
    "SELECT COALESCE(sum(completed_count),0) FROM repair_checkpoints WHERE job_id='$job_id'")"
  applying_count="$(docker compose exec -T postgres psql -U repairs -d repairs -Atc \
    "SELECT count(*) FROM repair_items WHERE job_id='$job_id' AND state='APPLYING'")"
  if [[ "$checkpoint_count" -ge 20 && "$applying_count" -ge 1 ]]; then break; fi
  sleep 0.25
done

kill -9 "$killed_pid"
wait "$killed_pid" 2>/dev/null || true
killed_pid=""

docker compose restart postgres
docker compose up -d --wait postgres

for _ in $(seq 1 1200); do
  if ! kill -0 "$survivor_pid" 2>/dev/null; then break; fi
  sleep 0.5
done
if kill -0 "$survivor_pid" 2>/dev/null; then
  echo "Surviving worker did not complete within 10 minutes" >&2
  exit 1
fi
wait "$survivor_pid"
survivor_pid=""

"$java_command" -jar "$jar" validate

read -r pending applied duplicate_applies retries checkpoints wrong_fees <<<"$(
  docker compose exec -T postgres psql -U repairs -d repairs -AtF' ' -c "
    SELECT
      count(*) FILTER (WHERE state IN ('PENDING','APPLYING')),
      count(*) FILTER (WHERE state='APPLIED'),
      (SELECT count(*) FROM (
        SELECT payment_id FROM audit_log WHERE action='ITEM_APPLIED'
        GROUP BY payment_id HAVING count(*) > 1
      ) duplicates),
      COALESCE(sum(GREATEST(attempt_count-1,0)),0),
      (SELECT COALESCE(sum(completed_count),0) FROM repair_checkpoints WHERE job_id='$job_id'),
      (SELECT count(*) FROM payments WHERE fee_bps<>290)
    FROM repair_items WHERE job_id='$job_id';"
)"

if [[ "$pending" != "0" || "$applied" != "5000" || "$duplicate_applies" != "0" \
   || "$retries" -lt "1" || "$checkpoints" != "5000" || "$wrong_fees" != "0" ]]; then
  echo "Fault assertions failed: pending=$pending applied=$applied duplicate_applies=$duplicate_applies retries=$retries checkpoints=$checkpoints wrong_fees=$wrong_fees" >&2
  exit 1
fi

printf '{"jobId":"%s","killedWorkers":1,"kafkaRestarts":2,"postgresRestarts":1,"applied":%s,"duplicateApplies":%s,"retries":%s,"checkpoints":%s,"remainingWrongFees":%s,"passed":true}\n' \
  "$job_id" "$applied" "$duplicate_applies" "$retries" "$checkpoints" "$wrong_fees" \
  > build/reports/repair-fault-injection.json

echo "Hard fault-injection experiment passed"
