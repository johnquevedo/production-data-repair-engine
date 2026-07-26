#!/usr/bin/env bash
set -euo pipefail

docker compose down --volumes --remove-orphans
docker compose up -d --wait postgres kafka
mvn -q -DskipTests package

if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
  java_command="${JAVA_HOME}/bin/java"
elif [[ -x /opt/homebrew/opt/openjdk/bin/java ]]; then
  java_command=/opt/homebrew/opt/openjdk/bin/java
else
  java_command=java
fi

"${java_command}" -jar target/production-data-repair-engine.jar experiment "$@"
