# MCP Conformance & Safety — R11 + R12

## Scope
Focused MCP-layer conformance and untrusted-memory retrieval controls.
Excludes C07 search-feedback telemetry and C10 resource structs (removed as follow-up).

## Changes

### R11: MCP Conformance Regression Suite
- Added conformance tests validating:
  - `initialize` response shape (`protocolVersion`, `capabilities`, `serverInfo`)
  - `tools/list` strict input/output schemas with `additionalProperties: false`
  - Safety metadata output schema preservation
  - Existing tool args deserialization and strict mode field rejection

### R12: Untrusted-Memory Retrieval Controls
- `DEFAULT_OUTPUT_BUDGET_CHARS = 16_384` — char budget for all prose-formatting tools
- `truncate_prose_to_budget(text, budget, counters)` — word-boundary truncation with `...` suffix, updates `SafetyCounters`
- Extended `SafetyCounters` with `truncation_applied` and `output_chars`
- Updated `safety_metadata_output_schema()` to expose truncation counters
- Applied truncation to all 6 prose tools: `search`, `open`, `search_conversations`, `list_sessions`, `get_session`, `get_session_events`

### C10 Resources (retained)
- `resources/list` and `resources/read` handlers with session/event URI templates remain in place

## Rejected / Follow-up
- **C07 Search Feedback**: `record_feedback` tool, `search_feedback_log` SQL migration, `eval.rs` metrics — reverted due to cross-cutting conversations/SQL migration invasiveness. Recommend dedicated branch.

## Validation
- `cargo test -p moraine-mcp-core --lib --locked` → 41 passed
- `cargo fmt --all -- --check` → clean
- `cargo clippy -p moraine-mcp-core --all-targets -- -D warnings -Aclippy::too_many_arguments -Aclippy::collapsible_if -Aclippy::derivable_impls` → clean
