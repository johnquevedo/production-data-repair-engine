# Publication audit

Audit performed before the v0.1.0 public release.

| Area | Finding | Disposition |
|---|---|---|
| Credentials and secrets | No tokens, private keys, cloud credentials, or real passwords found | Kubernetes test credentials are created at runtime; the example Secret contains only `replace-me`; Compose uses the documented local-only `repairs` credential |
| Personal information | No email addresses, usernames, home-directory paths, or personal metadata found in source/evidence | None required |
| Generated databases/data | No database files are tracked | `*.db`, `*.sqlite`, and `*.parquet` are ignored |
| Build outputs | Maven/Spark `target/` was 52 MB | Ignored; not published |
| Tool downloads | `.tools/kind` was an 11 MB downloaded binary | `.tools/` ignored; not published |
| Benchmark artifacts | `build/` contained scratch and smoke outputs | Ignored; only five final JSON reports were curated under `evidence/` |
| Large files | No intended tracked source file exceeds 1 MB | Verified before initialization |

The versioned evidence retains the final recruiting-scale, Spark, hard-fault,
Kind, and preserved baseline results. It contains environment descriptions but
no secrets or personal filesystem paths.
