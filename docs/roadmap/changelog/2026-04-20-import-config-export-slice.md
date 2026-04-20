# Changelog Fragment: Import/Config/Export Usability Foundation

Date: 2026-04-20
Scope: C02 remote import profiles, C03 config wizard/source discovery, C11 portable archives, P06 CLI polish (foundation only)

## What Changed

### `crates/moraine-config`
- Added `ImportProfile` struct with `host`, `remote_paths`, `local_mirror`, `include_patterns`, `exclude_patterns`, `cadence`.
- Added `imports: HashMap<String, ImportProfile>` to `AppConfig`.
- Added `discover_sources()` — scans well-known agent directories (codex, claude-code, kimi-cli, hermes, opencode) and returns `DiscoveredSource` candidates with existence checks.
- Added `validate_sources()` — checks for missing directories, unknown formats, and overlapping watch roots. Returns `SourceValidationIssue` enums.
- Fixed pre-existing clippy `derivable_impls` warning on `RedactionMode`.

### `apps/moraine` CLI
- New commands:
  - `moraine import sync <name>` — previews profile config (live sync disabled unless a future slice wires `--execute`).
  - `moraine import status` — shows all configured profiles and last sync manifest.
  - `moraine archive export --out-dir <dir>` — previews export manifest (live export disabled unless a future slice wires `--execute`).
  - `moraine archive import --input <dir>` — previews archive tables (live import disabled unless a future slice wires `--execute`).
  - `moraine archive verify <dir>` — validates local `manifest.json` against JSONL files and row counts.
  - `moraine config detect --json` — outputs discovered sources.
  - `moraine config validate` — reports config issues.
  - `moraine config wizard` — interactive stdin wizard that appends discovered sources with `.toml.bak` backup.
- Non-interactive new commands support `--output json`; the interactive wizard remains terminal-oriented.
- Added focused unit tests covering clap parsing, archive verify, config validate, source-overlap validation, import profile normalization, sync manifest roundtrip, and existing regressions.

### Config
- `config/moraine.toml` now includes a commented `[imports.vm503]` example.

### Docs
- `docs/operations/build-and-operations.md` updated with usage examples for import profiles, portable archives, and config wizard/validation.

## What Is Intentionally Stubbed

Live execution paths are disabled in this foundation slice:
- `import sync --execute` returns a clear not-implemented error.
- `archive export --execute` returns a clear not-implemented error.
- `archive import --execute` returns a clear not-implemented error.

These stubs preserve the CLI contract, rendering layer, and JSON output schemas while leaving actual subprocess/ClickHouse I/O for a follow-up slice.

## Validation

```bash
cargo test --workspace --locked        # pass
cargo fmt --all -- --check             # pass
cargo clippy -p moraine -p moraine-config --all-targets -- -D warnings  # pass
```

## Backward Compatibility

- Existing config fields unchanged.
- New `imports` section is optional and defaults to empty.
- All existing CLI commands unaffected.
