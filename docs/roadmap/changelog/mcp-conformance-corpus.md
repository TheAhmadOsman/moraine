# Changelog: MCP Conformance Corpus

Date: 2026-04-20
Scope: R11 MCP conformance regression corpus for the existing server surface

## What Changed

### `crates/moraine-mcp-core`
- Added a focused MCP conformance corpus around the real request dispatcher instead of only helper-level assertions.
- Covered the existing server methods hosts rely on:
  - `initialize`
  - `tools/list`
  - `tools/call`
  - `resources/list`
  - `resources/templates/list`
  - `resources/read`
  - `prompts/list`
  - `prompts/get`
- Locked in host-facing invariants for:
  - required JSON-RPC top-level fields and request id echoing
  - published tool/resource/prompt catalogs
  - strict schema metadata and stable retrieval tool names
  - `tools/call` validation failures staying visible instead of being silently coerced
  - the split between JSON-RPC `-32602` request errors and MCP tool results with `isError=true`

### Docs
- Updated `docs/mcp/agent-interface.md` to describe the new conformance corpus and the contract it protects for hosts and contributors.

## Validation

```bash
cargo fmt --all
cargo test -p moraine-mcp-core --locked
```

## Notes

- The corpus stays additive and scoped to the existing MCP surface. It does not broaden protocol features or add a second test-only server contract.
- Live retrieval correctness still belongs in sandbox or end-to-end checks; this suite is for stable wire-contract regression coverage.
