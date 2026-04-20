use std::collections::HashSet;
use std::path::PathBuf;

use moraine_ingest_core::normalize::normalize_record;
use serde_json::Value;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("kimi-cli")
        .join(name)
}

fn normalize_lines(name: &str) -> Vec<moraine_ingest_core::model::NormalizedRecord> {
    let path = fixture_path(name);
    let body = std::fs::read_to_string(&path).expect("read fixture");
    body.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            if line.trim().is_empty() {
                return None;
            }
            let record: Value = serde_json::from_str(line).expect("valid fixture line");
            Some(
                normalize_record(
                    &record,
                    "ci-kimi",
                    "kimi-cli",
                    path.to_str().unwrap(),
                    1,
                    1,
                    idx as u64 + 1,
                    idx as u64,
                    "",
                    "",
                )
                .expect("kimi fixture normalizes"),
            )
        })
        .collect()
}

#[test]
fn kimi_wire_fixture_maps_messages_tools_and_tokens() {
    let rows = normalize_lines("wire.jsonl");
    assert!(rows.iter().all(|row| row.error_rows.is_empty()));
    assert_eq!(
        rows[0].raw_row.get("record_ts").and_then(Value::as_str),
        Some("1970-01-01T00:00:00.000001Z")
    );
    let metadata = &rows[0].event_rows[0];
    assert_eq!(
        metadata.get("event_kind").and_then(Value::as_str),
        Some("session_meta")
    );
    assert_eq!(
        metadata.get("record_ts").and_then(Value::as_str),
        Some("1970-01-01T00:00:00.000001Z")
    );
    assert_eq!(
        rows[1].raw_row.get("harness").and_then(Value::as_str),
        Some("kimi-cli")
    );
    assert_eq!(
        rows[1]
            .raw_row
            .get("inference_provider")
            .and_then(Value::as_str),
        Some("moonshot")
    );
    assert!(rows[1].session_hint.starts_with("kimi-cli:kimi-cli"));

    let user = &rows[1].event_rows[0];
    assert_eq!(
        user.get("event_kind").and_then(Value::as_str),
        Some("message")
    );
    assert_eq!(user.get("actor_kind").and_then(Value::as_str), Some("user"));
    assert_eq!(
        user.get("record_ts").and_then(Value::as_str),
        Some("2026-04-12T00:32:24.549974Z")
    );

    let reasoning = &rows[3].event_rows[0];
    assert_eq!(
        reasoning.get("event_kind").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(
        reasoning.get("has_reasoning").and_then(Value::as_u64),
        Some(1)
    );

    let tool_call = &rows[5].event_rows[0];
    assert_eq!(
        tool_call.get("event_kind").and_then(Value::as_str),
        Some("tool_call")
    );
    assert_eq!(
        tool_call.get("tool_name").and_then(Value::as_str),
        Some("ReadFile")
    );
    assert_eq!(rows[5].tool_rows.len(), 1);

    let tool_result = &rows[6].event_rows[0];
    assert_eq!(
        tool_result.get("event_kind").and_then(Value::as_str),
        Some("tool_result")
    );
    assert_eq!(rows[6].tool_rows.len(), 1);

    let usage = &rows[7].event_rows[0];
    assert_eq!(
        usage.get("payload_type").and_then(Value::as_str),
        Some("token_count")
    );
    assert_eq!(usage.get("input_tokens").and_then(Value::as_u64), Some(10));
    assert_eq!(usage.get("output_tokens").and_then(Value::as_u64), Some(5));
}

#[test]
fn kimi_context_fixture_uses_synthetic_timestamps_without_errors() {
    let rows = normalize_lines("context.jsonl");
    assert!(rows.iter().all(|row| row.error_rows.is_empty()));

    let user = &rows[1].event_rows[0];
    assert_eq!(user.get("actor_kind").and_then(Value::as_str), Some("user"));
    assert_eq!(
        user.get("record_ts").and_then(Value::as_str),
        Some("1970-01-01T00:00:00.000002Z")
    );

    let usage = &rows[4].event_rows[0];
    assert_eq!(
        usage.get("payload_type").and_then(Value::as_str),
        Some("token_count")
    );
    assert_eq!(usage.get("input_tokens").and_then(Value::as_u64), Some(42));
}

#[test]
fn kimi_events_do_not_reuse_raw_record_uid() {
    let rows = normalize_lines("wire.jsonl");
    let mut event_uids = HashSet::new();

    for row in &rows {
        let raw_uid = row
            .raw_row
            .get("event_uid")
            .and_then(Value::as_str)
            .expect("raw uid");
        for event in &row.event_rows {
            let event_uid = event
                .get("event_uid")
                .and_then(Value::as_str)
                .expect("event uid");
            assert_ne!(raw_uid, event_uid);
            assert!(event_uids.insert(event_uid.to_string()));
        }
    }
}
