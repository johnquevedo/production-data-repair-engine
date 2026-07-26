#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root_dir"

kind_bin="$root_dir/.tools/kind"
cluster_name=repair-engine
namespace=data-repair
app_image=production-data-repair-engine:kind
spark_image=production-data-repair-spark:kind
postgres_image=production-data-repair-postgres:kind
kafka_image=production-data-repair-kafka:kind
kind_report=build/reports/repair-kind-experiment.json
previous_context="$(kubectl config current-context 2>/dev/null || true)"

cleanup() {
  "$kind_bin" delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  if [[ -n "$previous_context" ]]; then
    kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ! -x "$kind_bin" ]]; then
  echo "Kind binary is missing at $kind_bin" >&2
  exit 1
fi

mkdir -p build/kind build/reports
mvn -q -DskipTests package
docker build --platform linux/arm64 --provenance=false -t "$app_image" .
docker build --platform linux/arm64 --provenance=false -f Dockerfile.spark -t "$spark_image" .
docker build --platform linux/arm64 --provenance=false -f Dockerfile.postgres -t "$postgres_image" .
docker build --platform linux/arm64 --provenance=false -f Dockerfile.kafka -t "$kafka_image" .

"$kind_bin" delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
"$kind_bin" create cluster --name "$cluster_name" --wait 120s

"$kind_bin" load docker-image --name "$cluster_name" "$app_image"
"$kind_bin" load docker-image --name "$cluster_name" "$spark_image"
"$kind_bin" load docker-image --name "$cluster_name" "$postgres_image"
"$kind_bin" load docker-image --name "$cluster_name" "$kafka_image"

kubectl apply -f k8s/namespace.yaml
kubectl -n "$namespace" create secret generic repair-engine-database \
  --from-literal=DB_USER=repairs \
  --from-literal=DB_PASSWORD=repairs-local-only
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/kind-services.yaml
kubectl -n "$namespace" rollout status deployment/postgres --timeout=180s
kubectl -n "$namespace" rollout status deployment/kafka --timeout=180s

run_job() {
  local name="$1"
  shift
  local subcommand="$1"
  local args=("$@" --jdbc jdbc:postgresql://postgres:5432/repairs \
    --user repairs --password repairs-local-only)
  if [[ "$subcommand" == "publish" || "$subcommand" == "consume" ]]; then
    args+=(--kafka kafka:9092)
  fi
  kubectl -n "$namespace" delete job "$name" --ignore-not-found >/dev/null
  kubectl -n "$namespace" create job "$name" --image="$app_image" -- \
    java -jar /app/repair-engine.jar "${args[@]}"
  kubectl -n "$namespace" wait --for=condition=complete "job/$name" --timeout=600s
  kubectl -n "$namespace" logs "job/$name"
}

run_job seed seed --corrupted 5000 --controls 500
run_job publish publish --replay 500
run_job consume consume --group kind-lineage

kubectl -n "$namespace" delete job spark-discovery --ignore-not-found >/dev/null
kubectl -n "$namespace" create job spark-discovery --image="$spark_image" -- \
  /opt/spark/bin/spark-submit --class dev.datarepair.spark.AffectedRecordDiscoveryJob \
  --master 'local[*]' /opt/repair/repair-engine.jar \
  jdbc:postgresql://postgres:5432/repairs repairs repairs-local-only bad-fees-v2 /tmp/affected
kubectl -n "$namespace" wait --for=condition=complete job/spark-discovery --timeout=600s
spark_output="$(kubectl -n "$namespace" logs job/spark-discovery)"
spark_affected="$(printf '%s\n' "$spark_output" | sed -n 's/^AFFECTED_RECORDS=//p' | tail -1)"
if [[ "$spark_affected" != "5000" ]]; then
  echo "Spark discovery count mismatch: $spark_affected" >&2
  exit 1
fi

plan_output="$(run_job plan plan --bad-config bad-fees-v2 --target-fee-bps 290 --partitions 32)"
job_id="$(printf '%s\n' "$plan_output" | sed -n 's/^JOB_ID=//p' | tail -1)"
if [[ -z "$job_id" ]]; then
  echo "Could not parse Kind repair job id" >&2
  exit 1
fi

sed "s/replace-with-reviewed-job-id/$job_id/" k8s/job.yaml > build/kind/worker-job.yaml
kubectl apply -f build/kind/worker-job.yaml

for _ in $(seq 1 180); do
  ready_pods="$(kubectl -n "$namespace" get pods -l job-name=repair-engine-controlled-run \
    -o jsonpath='{range .items[*]}{range .status.conditions[?(@.type=="Ready")]}{.status}{"\n"}{end}{end}' \
    | grep -c '^True$' || true)"
  if [[ "$ready_pods" -ge 4 ]]; then break; fi
  sleep 1
done
if [[ "$ready_pods" -lt 4 ]]; then
  kubectl -n "$namespace" get pods
  echo "Four repair workers never became ready" >&2
  exit 1
fi

for worker_pod in $(kubectl -n "$namespace" get pods \
  -l job-name=repair-engine-controlled-run -o name); do
  run_as_non_root="$(kubectl -n "$namespace" get "$worker_pod" \
    -o jsonpath='{.spec.securityContext.runAsNonRoot}')"
  read_only_root="$(kubectl -n "$namespace" get "$worker_pod" \
    -o jsonpath='{.spec.containers[0].securityContext.readOnlyRootFilesystem}')"
  privilege_escalation="$(kubectl -n "$namespace" get "$worker_pod" \
    -o jsonpath='{.spec.containers[0].securityContext.allowPrivilegeEscalation}')"
  token_mount="$(kubectl -n "$namespace" get "$worker_pod" \
    -o jsonpath='{.spec.automountServiceAccountToken}')"
  readiness_command="$(kubectl -n "$namespace" get "$worker_pod" \
    -o jsonpath='{.spec.containers[0].readinessProbe.exec.command}')"
  if [[ "$run_as_non_root" != "true" || "$read_only_root" != "true" \
     || "$privilege_escalation" != "false" || "$token_mount" != "false" \
     || "$readiness_command" != *repair-worker-ready* ]]; then
    echo "Worker security/readiness assertion failed for $worker_pod" >&2
    exit 1
  fi
done

for _ in $(seq 1 180); do
  checkpoints="$(kubectl -n "$namespace" exec deployment/postgres -- \
    psql -U repairs -d repairs -Atc \
    "SELECT COALESCE(sum(completed_count),0) FROM repair_checkpoints WHERE job_id='$job_id'")"
  if [[ "$checkpoints" -ge 20 ]]; then break; fi
  sleep 0.5
done

victim_pod="$(kubectl -n "$namespace" get pods -l job-name=repair-engine-controlled-run \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
termination_started_ms="$(($(date +%s) * 1000))"
kubectl -n "$namespace" delete pod "$victim_pod" --grace-period=0 --force

recovery_millis=""
for _ in $(seq 1 600); do
  recovery_events="$(kubectl -n "$namespace" exec deployment/postgres -- \
    psql -U repairs -d repairs -Atc \
    "SELECT count(*) FROM audit_log WHERE job_id='$job_id' AND action='CRASH_RECOVERY'")"
  if [[ "$recovery_events" -ge 1 ]]; then
    recovery_millis="$(($(date +%s) * 1000 - termination_started_ms))"
    break
  fi
  sleep 0.5
done
if [[ -z "$recovery_millis" ]]; then
  echo "No crash-recovery audit event after pod termination" >&2
  exit 1
fi

kubectl -n "$namespace" wait --for=condition=complete \
  job/repair-engine-controlled-run --timeout=900s
run_job validate validate

read -r pending applied duplicate_applies retries checkpoints wrong_fees worker_ids <<<"$(
  kubectl -n "$namespace" exec deployment/postgres -- \
  psql -U repairs -d repairs -AtF' ' -c "
    SELECT
      count(*) FILTER (WHERE state IN ('PENDING','APPLYING')),
      count(*) FILTER (WHERE state='APPLIED'),
      (SELECT count(*) FROM (
        SELECT payment_id FROM audit_log WHERE action='ITEM_APPLIED'
        GROUP BY payment_id HAVING count(*) > 1
      ) duplicates),
      COALESCE(sum(GREATEST(attempt_count-1,0)),0),
      (SELECT COALESCE(sum(completed_count),0) FROM repair_checkpoints WHERE job_id='$job_id'),
      (SELECT count(*) FROM payments WHERE fee_bps<>290),
      count(DISTINCT worker_id)
    FROM repair_items WHERE job_id='$job_id';"
)"

if [[ "$pending" != "0" || "$applied" != "5000" || "$duplicate_applies" != "0" \
   || "$retries" -lt "1" || "$checkpoints" != "5000" || "$wrong_fees" != "0" \
   || "$worker_ids" -lt "4" ]]; then
  echo "Kind assertions failed: pending=$pending applied=$applied duplicate=$duplicate_applies retries=$retries checkpoints=$checkpoints wrong=$wrong_fees workers=$worker_ids" >&2
  exit 1
fi

printf '{"cluster":"kind-%s","sparkAffected":%s,"workerPodsReady":%s,"distinctWorkers":%s,"podsForceDeleted":1,"podRecoveryMillis":%s,"applied":%s,"duplicateApplies":%s,"retries":%s,"checkpoints":%s,"remainingWrongFees":%s,"securityContextVerified":true,"readinessProbeVerified":true,"passed":true}\n' \
  "$cluster_name" "$spark_affected" "$ready_pods" "$worker_ids" "$recovery_millis" \
  "$applied" "$duplicate_applies" "$retries" "$checkpoints" "$wrong_fees" \
  > "$kind_report"

echo "Kind end-to-end experiment passed"
