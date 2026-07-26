# Versioned evidence

These JSON files are immutable copies of the final controlled local experiment
outputs used by the recruiting specification and README. The executable scripts
write fresh results under ignored `build/` and `target/` directories; compare a
rerun with these files rather than overwriting them.

| File | Scope |
|---|---|
| `baseline.json` | Preserved 1,000-record baseline |
| `scale-100k.json` | Final 100,000-record online repair experiment |
| `spark-discovery-100k.json` | Independent Spark JDBC discovery measurement |
| `fault-injection.json` | Worker SIGKILL plus Kafka/PostgreSQL restart run |
| `kind-e2e.json` | Disposable local Kind execution and pod recovery |

The environment fields describe the machine on which the measurements were
recorded. They contain no credentials or personal filesystem paths. Results are
local validation evidence, not production benchmarks.
