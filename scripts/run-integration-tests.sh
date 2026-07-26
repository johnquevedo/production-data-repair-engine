#!/usr/bin/env bash
set -euo pipefail

docker compose up -d --wait postgres kafka
mvn -Drepair.integration=true -Dgroups=integration test
