use std::path::PathBuf;

use moraine_ingest_core::normalize::normalize_record;
use serde_json::{json, Value};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("opencode")
        .join("events.jsonl")
}

fn normalize_fixture() -> Vec<moraine_ingest_core::model::NormalizedRecord> {
    let path = fixture_path();
    let body = std::fs::read_to_string(&path).expect("read fixture");
    body.lines()
        .enumerate()
        .map(|(idx, line)| {
            let record: Value = serde_json::from_str(line).expect("valid fixture json");
            normalize_record(
                &record,
                "ci-opencode",
                "opencode",
                path.to_str().unwrap(),
                1,
                1,
                idx as u64 + 1,
                idx as u64,
                "",
                "",
            )
            .expect("opencode fixture normalizes")
        })
        .collect()
}

#[test]
fn opencode_fixture_maps_session_messages_tools_and_tokens() {
    let rows = normalize_fixture();
    assert!(rows.iter().all(|row| row.error_rows.is_empty()));

    let session = &rows[0].event_rows[0];
    assert_eq!(
        session.get("event_kind").and_then(Value::as_str),
        Some("session_meta")
    );
    assert_eq!(
        session.get("session_id").and_then(Value::as_str),
        Some("opencode:ses_1")
    );

    let user = &rows[1].event_rows[0];
    assert_eq!(user.get("actor_kind").and_then(Value::as_str), Some("user"));
    assert_eq!(
        user.get("payload_type").and_then(Value::as_str),
        Some("user_message")
    );
    assert_eq!(
        user.get("inference_provider").and_then(Value::as_str),
        Some("openai")
    );

    let reasoning = &rows[2].event_rows[0];
    assert_eq!(
        reasoning.get("event_kind").and_then(Value::as_str),
        Some("reasoning")
    );
    assert_eq!(
        reasoning.get("inference_provider").and_then(Value::as_str),
        Some("anthropic")
    );
    assert_eq!(
        reasoning.get("model").and_then(Value::as_str),
        Some("claude-sonnet-4.6")
    );

    let tool = &rows[3].event_rows[0];
    assert_eq!(
        tool.get("event_kind").and_then(Value::as_str),
        Some("tool_result")
    );
    assert_eq!(
        tool.get("tool_call_id").and_then(Value::as_str),
        Some("call_read")
    );
    assert_eq!(rows[3].tool_rows.len(), 1);

    let finish = &rows[4].event_rows[0];
    assert_eq!(finish.get("input_tokens").and_then(Value::as_u64), Some(10));
    assert_eq!(finish.get("output_tokens").and_then(Value::as_u64), Some(5));
    assert_eq!(
        finish.get("op_status").and_then(Value::as_str),
        Some("stop")
    );
}

#[test]
fn opencode_mutable_rows_keep_stable_event_identity() {
    let source_file = fixture_path();
    let base = json!({
        "type": "opencode_part",
        "timestamp": "2026-04-18T12:00:00.000Z",
        "row_id": "part_mutable",
        "session_id": "ses_1",
        "message_id": "msg_1",
        "part_id": "part_mutable",
        "message": {
            "id": "msg_1",
            "role": "assistant",
            "providerID": "anthropic",
            "modelID": "claude-sonnet-4.6"
        },
        "part": {
            "id": "part_mutable",
            "type": "tool",
            "tool": "read",
            "callID": "call_mutable",
            "state": {
                "status": "running",
                "input": { "path": "Cargo.toml" }
            }
        }
    });
    let mut updated = base.clone();
    updated["timestamp"] = json!("2026-04-18T12:00:01.000Z");
    updated["part"]["state"] = json!({
        "status": "completed",
        "input": { "path": "Cargo.toml" },
        "output": "ok"
    });

    let first = normalize_record(
        &base,
        "ci-opencode",
        "opencode",
        source_file.to_str().unwrap(),
        1,
        1,
        42,
        1_715_000_001_000,
        "",
        "",
    )
    .expect("initial opencode row normalizes");
    let second = normalize_record(
        &updated,
        "ci-opencode",
        "opencode",
        source_file.to_str().unwrap(),
        1,
        1,
        42,
        1_715_000_002_000,
        "",
        "",
    )
    .expect("updated opencode row normalizes");

    assert_eq!(
        first.event_rows[0].get("event_uid"),
        second.event_rows[0].get("event_uid")
    );
    assert_eq!(
        first.event_rows[0]
            .get("source_offset")
            .and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        second.event_rows[0]
            .get("event_kind")
            .and_then(Value::as_str),
        Some("tool_result")
    );
}
