#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root_dir"

if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  java_command="${JAVA_HOME}/bin/java"
elif [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
  java_command=/opt/homebrew/opt/openjdk/bin/java
else
  java_command=java
fi

docker compose down --volumes --remove-orphans
docker compose up -d --wait postgres kafka
mvn -q -DskipTests package
"$java_command" -jar target/production-data-repair-engine.jar \
  scale-experiment --records 100000 --workers 8 \
  --report build/reports/repair-scale-experiment.json "$@"
