# Changelog Fragment: Operational Safety E2E

Date: 2026-04-22
Scope: Wave 4 operational safety validation

## What Changed

- Added `scripts/ci/operational-safety-e2e.sh`, a disposable sandbox smoke test
  for the combined maintenance workflow.
- The script seeds synthetic Codex and Claude fixture files into sandbox
  sources, waits for raw, canonical, and search rows, then validates:
  - `moraine db doctor --deep`;
  - `moraine sources drift`;
  - `moraine import sync --execute` manifest v2 output through a local fake
    `rsync`;
  - `moraine import status`;
  - `moraine backup create` and `moraine backup verify`;
  - `moraine restore --execute` into a staging database;
  - `moraine reindex --search-only` dry-run, execute, and resume.
- Documented when to run the operational safety smoke in the sandbox guide.

## Validation

```bash
cargo fmt --all -- --check
bash -n scripts/ci/operational-safety-e2e.sh
scripts/ci/operational-safety-e2e.sh
```
