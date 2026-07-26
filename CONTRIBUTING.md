# Contributing

Issues and focused pull requests are welcome. Use Java 17+, do not commit
credentials or generated experiment data, and run:

```bash
mvn -Dgroups='!integration' test
./scripts/run-integration-tests.sh
./scripts/test-kubernetes.sh
```

Performance claims must include a machine-readable report, environment details,
the exact command, final invariant checks, and a controlled-local qualifier.
