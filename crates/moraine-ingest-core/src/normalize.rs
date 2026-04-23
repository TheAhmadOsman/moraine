use crate::model::NormalizedRecord;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, NaiveDateTime, SecondsFormat, Utc};
use regex::Regex;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

const TEXT_LIMIT: usize = 200_000;
const RAW_JSON_LIMIT: usize = TEXT_LIMIT;
const PREVIEW_LIMIT: usize = 320;
const UNPARSEABLE_EVENT_TS: &str = "1970-01-01 00:00:00.000";

fn session_id_re() -> &'static Regex {
    static SESSION_ID_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_ID_RE.get_or_init(|| {
        Regex::new(
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$",
        )
        .expect("valid session id regex")
    })
}

fn session_date_re() -> &'static Regex {
    static SESSION_DATE_RE: OnceLock<Regex> = OnceLock::new();
    SESSION_DATE_RE.get_or_init(|| {
        Regex::new(r"/(?:sessions|projects)/(\d{4})/(\d{2})/(\d{2})/")
            .expect("valid session date regex")
    })
}

fn to_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn to_u32(value: Option<&Value>) -> u32 {
    match value {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        Some(Value::String(s)) => s.parse::<u64>().unwrap_or(0).min(u32::MAX as u64) as u32,
        _ => 0,
    }
}

fn to_u16(value: Option<&Value>) -> u16 {
    to_u32(value).min(u16::MAX as u32) as u16
}

fn to_u8_bool(value: Option<&Value>) -> u8 {
    match value {
        Some(Value::Bool(v)) => u8::from(*v),
        Some(Value::Number(v)) => u8::from(v.as_i64().unwrap_or(0) != 0),
        Some(Value::String(s)) => {
            let lower = s.to_ascii_lowercase();
            u8::from(lower == "true" || lower == "1")
        }
        _ => 0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Harness {
    Codex,
    ClaudeCode,
    FactoryDroid,
    Hermes,
    KimiCli,
    OpenCode,
}

impl Harness {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "codex" => Ok(Self::Codex),
            "claude-code" => Ok(Self::ClaudeCode),
            "factory-droid" => Ok(Self::FactoryDroid),
            "hermes" => Ok(Self::Hermes),
            "kimi-cli" => Ok(Self::KimiCli),
            "opencode" => Ok(Self::OpenCode),
            _ => Err(anyhow!(
                "unsupported harness `{}`; expected one of: codex, claude-code, factory-droid, hermes, kimi-cli, opencode",
                raw.trim()
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::ClaudeCode => "claude-code",
            Self::FactoryDroid => "factory-droid",
            Self::Hermes => "hermes",
            Self::KimiCli => "kimi-cli",
            Self::OpenCode => "opencode",
        }
    }

    /// Default LLM vendor for the harness. Hermes trajectories encode the
    /// vendor inside `model` as `vendor/model`, so the per-record vendor is
    /// resolved at normalization time rather than being fixed to the harness.
    fn inference_provider(self) -> &'static str {
        match self {
            Self::Codex => "openai",
            Self::ClaudeCode => "anthropic",
            Self::FactoryDroid => "",
            Self::Hermes => "",
            Self::KimiCli => "moonshot",
            Self::OpenCode => "",
        }
    }
}

fn canonicalize_model(harness: &str, raw_model: &str) -> String {
    let mut model = raw_model.trim().to_ascii_lowercase();
    if model.is_empty() {
        return String::new();
    }

    model = model.replace(' ', "-");

    if harness == "codex" && model == "codex" {
        return "gpt-5.3-codex-xhigh".to_string();
    }

    model
}

fn resolve_model_hint(event_rows: &[Value], harness: &str, fallback: &str) -> String {
    for row in event_rows.iter().rev() {
        if let Some(model) = row.get("model").and_then(Value::as_str) {
            let normalized = canonicalize_model(harness, model);
            if !normalized.is_empty() {
                return normalized;
            }
        }
    }

    canonicalize_model(harness, fallback)
}

/// Split a Hermes `vendor/model` string into `(inference_provider, model)`.
///
/// Hermes trajectories encode the LLM vendor in the `model` field, e.g.
/// `anthropic/claude-sonnet-4.6`. We split on the first slash only: everything
/// before becomes `inference_provider`, everything after is kept verbatim as
/// `model`. If there is no slash, the whole value is treated as a bare model
/// and `inference_provider` is empty.
///
/// Both pieces are lower-cased and trimmed but otherwise left alone — Hermes
/// model strings are already the canonical name in upstream catalogues, so we
/// do not apply dot-to-dash or snapshot-stripping mangling here. Cloud-prefixed
/// forms such as `bedrock/anthropic/claude-opus-4-5` split on the first slash
/// too; that leaves `bedrock` as the vendor and `anthropic/claude-opus-4-5` as
/// the model. Future work can re-nest those, but for now the grammar allows
/// slashes in the model string.
fn split_hermes_vendor_model(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new());
    }

    match trimmed.split_once('/') {
        Some((vendor, model)) => (
            vendor.trim().to_ascii_lowercase(),
            model.trim().to_ascii_lowercase(),
        ),
        None => (String::new(), trimmed.to_ascii_lowercase()),
    }
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        input.to_string()
    } else {
        input.chars().take(max_chars).collect()
    }
}

fn extract_message_text(content: &Value) -> String {
    fn walk(node: &Value, out: &mut Vec<String>) {
        match node {
            Value::String(s) if !s.trim().is_empty() => {
                out.push(s.clone());
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            Value::Object(map) => {
                for key in ["text", "message", "output", "thinking", "think", "summary"] {
                    if let Some(Value::String(s)) = map.get(key) {
                        if !s.trim().is_empty() {
                            out.push(s.clone());
                        }
                    }
                }

                for key in ["content", "text_elements", "input"] {
                    if let Some(value) = map.get(key) {
                        walk(value, out);
                    }
                }
            }
            _ => {}
        }
    }

    let mut chunks = Vec::<String>::new();
    walk(content, &mut chunks);
    truncate_chars(&chunks.join("\n"), TEXT_LIMIT)
}

fn extract_content_types(content: &Value) -> Vec<String> {
    if let Value::Array(items) = content {
        let mut out = Vec::<String>::new();
        for item in items {
            if let Some(t) = item.get("type").and_then(|v| v.as_str()) {
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            }
        }
        out.sort();
        out.dedup();
        return out;
    }
    Vec::new()
}

fn parse_json_string(value: &str) -> Option<Value> {
    serde_json::from_str::<Value>(value.trim()).ok()
}

trait NonEmptyStringExt {
    fn or_else_nonempty<F: FnOnce() -> String>(self, fallback: F) -> String;
}

impl NonEmptyStringExt for String {
    fn or_else_nonempty<F: FnOnce() -> String>(self, fallback: F) -> String {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}

fn opencode_provider_model(record: &Value) -> (String, String) {
    let message = record.get("message").unwrap_or(record);
    let part = record.get("part").unwrap_or(&Value::Null);

    let provider = to_str(message.get("providerID"))
        .or_else_nonempty(|| to_str(message.get("provider_id")))
        .or_else_nonempty(|| to_str(message.get("model").and_then(|m| m.get("providerID"))))
        .or_else_nonempty(|| to_str(part.get("model").and_then(|m| m.get("providerID"))));
    let model = to_str(message.get("modelID"))
        .or_else_nonempty(|| to_str(message.get("model_id")))
        .or_else_nonempty(|| to_str(message.get("model").and_then(|m| m.get("modelID"))))
        .or_else_nonempty(|| to_str(part.get("model").and_then(|m| m.get("modelID"))));

    (provider, canonicalize_model("opencode", &model))
}

fn split_provider_model_hint(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new());
    }

    match trimmed.split_once('/') {
        Some((provider, model)) => (
            provider.trim().to_ascii_lowercase(),
            canonicalize_model("factory-droid", model),
        ),
        None => (String::new(), canonicalize_model("factory-droid", trimmed)),
    }
}

fn infer_factory_droid_provider(explicit_provider: &str, model: &str) -> String {
    let provider = explicit_provider.trim().to_ascii_lowercase();
    if !provider.is_empty() {
        return provider;
    }

    let model = model.trim().to_ascii_lowercase();
    if model.starts_with("claude-") || model.contains("/claude-") {
        "anthropic".to_string()
    } else if model.starts_with("gpt-")
        || model.starts_with("o1")
        || model.starts_with("o3")
        || model.starts_with("o4")
        || model.contains("openai")
    {
        "openai".to_string()
    } else if model.starts_with("gemini-") {
        "google".to_string()
    } else if model.starts_with("glm-") {
        "zai".to_string()
    } else if model.starts_with("kimi-") {
        "moonshot".to_string()
    } else if model.starts_with("minimax-") {
        "minimax".to_string()
    } else if model == "droid-core" {
        "factory".to_string()
    } else {
        String::new()
    }
}

fn factory_droid_provider_model(record: &Value, model_hint: &str) -> (String, String) {
    let settings = record.get("settings").unwrap_or(record);
    let explicit_provider = to_str(record.get("providerLock"))
        .or_else_nonempty(|| to_str(record.get("provider")))
        .or_else_nonempty(|| to_str(record.get("inference_provider")))
        .or_else_nonempty(|| to_str(settings.get("providerLock")))
        .or_else_nonempty(|| to_str(settings.get("provider")))
        .or_else_nonempty(|| to_str(settings.get("inference_provider")));
    let explicit_model = to_str(record.get("model"))
        .or_else_nonempty(|| to_str(settings.get("model")))
        .or_else_nonempty(|| to_str(record.get("modelId")))
        .or_else_nonempty(|| to_str(settings.get("modelId")));
    let (hint_provider, hint_model) = split_provider_model_hint(model_hint);
    let model = if explicit_model.trim().is_empty() {
        hint_model
    } else {
        canonicalize_model("factory-droid", &explicit_model)
    };
    let provider = infer_factory_droid_provider(
        &explicit_provider.or_else_nonempty(|| hint_provider),
        &model,
    );
    (provider, model)
}

fn factory_droid_session_id(
    source_file: &str,
    session_hint: &str,
    top_type: &str,
    record: &Value,
) -> String {
    let explicit =
        to_str(record.get("session_id")).or_else_nonempty(|| to_str(record.get("sessionId")));
    if !explicit.is_empty() {
        return explicit;
    }
    if top_type == "session_start" {
        let session_start_id = to_str(record.get("id"));
        if !session_start_id.is_empty() {
            return session_start_id;
        }
    }
    if !session_hint.is_empty() {
        return session_hint.to_string();
    }
    infer_session_id_from_file(source_file)
}

fn pruned_factory_droid_payload(value: &Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(pruned_factory_droid_payload).collect())
        }
        Value::Object(map) => {
            let mut out = Map::<String, Value>::new();
            for (key, val) in map {
                match key.as_str() {
                    "openaiEncryptedContent" => {
                        out.insert(
                            key.clone(),
                            Value::String("[omitted:encrypted_content]".to_string()),
                        );
                    }
                    "systemInfo" => {
                        out.insert(
                            "systemInfo_omitted".to_string(),
                            Value::String("[omitted:large_environment_snapshot]".to_string()),
                        );
                    }
                    _ => {
                        out.insert(key.clone(), pruned_factory_droid_payload(val));
                    }
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

fn factory_droid_payload_json(value: &Value) -> String {
    compact_json(&pruned_factory_droid_payload(value))
}

fn update_string_field(row: &mut Value, key: &str, value: &str) {
    if let Some(obj) = row.as_object_mut() {
        obj.insert(key.to_string(), json!(value));
    }
}

fn update_u8_field(row: &mut Value, key: &str, value: u8) {
    if let Some(obj) = row.as_object_mut() {
        obj.insert(key.to_string(), json!(value));
    }
}

pub fn infer_session_id_from_file(source_file: &str) -> String {
    let stem = std::path::Path::new(source_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    session_id_re()
        .captures(stem)
        .and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_default()
}

pub fn infer_session_date_from_file(source_file: &str, record_ts: &str) -> String {
    if let Some(cap) = session_date_re().captures(source_file) {
        return format!("{}-{}-{}", &cap[1], &cap[2], &cap[3]);
    }

    parse_record_ts(record_ts)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

fn parse_record_ts(record_ts: &str) -> Option<DateTime<Utc>> {
    let trimmed = record_ts.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

fn format_event_ts(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn format_record_ts(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn format_unix_seconds_decimal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.contains(['e', 'E']) {
        return None;
    }

    let (secs_part, frac_part) = trimmed.split_once('.').unwrap_or((trimmed, ""));
    let secs = secs_part.parse::<i64>().ok()?;
    let mut nanos = frac_part
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .take(9)
        .collect::<String>();
    while nanos.len() < 9 {
        nanos.push('0');
    }
    let nanos = nanos.parse::<u32>().ok()?.min(999_999_999);
    DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| format_record_ts(&dt))
}

fn format_unix_seconds_ts(seconds: f64) -> Option<String> {
    if !seconds.is_finite() {
        return None;
    }
    let secs = seconds.trunc() as i64;
    let nanos = (seconds.fract().abs() * 1_000_000_000.0).round() as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos.min(999_999_999)).map(|dt| format_record_ts(&dt))
}

fn parse_event_ts(record_ts: &str) -> (String, bool) {
    if let Some(dt) = parse_record_ts(record_ts) {
        return (format_event_ts(&dt), false);
    }

    (UNPARSEABLE_EVENT_TS.to_string(), true)
}

fn event_kind_in_domain(value: &str) -> bool {
    matches!(
        value,
        "session_meta"
            | "turn_context"
            | "message"
            | "tool_call"
            | "tool_result"
            | "reasoning"
            | "event_msg"
            | "compacted_raw"
            | "progress"
            | "system"
            | "summary"
            | "queue_operation"
            | "file_history_snapshot"
            | "unknown"
    )
}

fn payload_type_in_domain(value: &str) -> bool {
    matches!(
        value,
        "session_meta"
            | "turn_context"
            | "message"
            | "function_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "web_search_call"
            | "reasoning"
            | "response_item"
            | "event_msg"
            | "user_message"
            | "agent_message"
            | "agent_reasoning"
            | "token_count"
            | "task_started"
            | "task_complete"
            | "turn_aborted"
            | "item_completed"
            | "search_results_received"
            | "compacted"
            | "thinking"
            | "tool_use"
            | "tool_result"
            | "text"
            | "progress"
            | "system"
            | "summary"
            | "queue-operation"
            | "file-history-snapshot"
            | "unknown"
    )
}

fn link_type_in_domain(value: &str) -> bool {
    matches!(
        value,
        "parent_event"
            | "compacted_parent"
            | "parent_uuid"
            | "tool_use_id"
            | "source_tool_assistant"
            | "unknown"
    )
}

fn canonicalize_event_kind(value: &str) -> &str {
    if event_kind_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

fn canonicalize_payload_type(value: &str) -> &str {
    if payload_type_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

fn canonicalize_link_type(value: &str) -> &str {
    if link_type_in_domain(value) {
        value
    } else {
        "unknown"
    }
}

fn event_uid(
    source_file: &str,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_fingerprint: &str,
    suffix: &str,
) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}",
        source_file, source_generation, source_line_no, source_offset, record_fingerprint, suffix
    );

    let mut hasher = Sha256::new();
    hasher.update(material.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn event_version() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    now as u64
}

pub(crate) fn raw_hash(raw_json: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(raw_json.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0)
}

fn io_hash(input_json: &str, output_json: &str) -> u64 {
    raw_hash(&format!("{}\n{}", input_json, output_json))
}

struct RecordContext<'a> {
    source_name: &'a str,
    harness: &'a str,
    inference_provider: &'a str,
    session_id: &'a str,
    session_date: &'a str,
    source_file: &'a str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    record_ts: &'a str,
    event_ts: &'a str,
}

fn base_event_obj(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    event_kind: &str,
    payload_type: &str,
    actor_kind: &str,
    text_content: &str,
    payload_json: &str,
) -> Map<String, Value> {
    let text_content = truncate_chars(text_content, TEXT_LIMIT);
    let payload_json = truncate_chars(payload_json, TEXT_LIMIT);
    let event_kind = canonicalize_event_kind(event_kind);
    let payload_type = canonicalize_payload_type(payload_type);
    let mut obj = Map::<String, Value>::new();
    obj.insert(
        "event_uid".to_string(),
        Value::String(event_uid.to_string()),
    );
    obj.insert(
        "session_id".to_string(),
        Value::String(ctx.session_id.to_string()),
    );
    obj.insert(
        "session_date".to_string(),
        Value::String(ctx.session_date.to_string()),
    );
    obj.insert(
        "source_name".to_string(),
        Value::String(ctx.source_name.to_string()),
    );
    obj.insert(
        "harness".to_string(),
        Value::String(ctx.harness.to_string()),
    );
    obj.insert(
        "inference_provider".to_string(),
        Value::String(ctx.inference_provider.to_string()),
    );
    obj.insert(
        "source_file".to_string(),
        Value::String(ctx.source_file.to_string()),
    );
    obj.insert("source_inode".to_string(), json!(ctx.source_inode));
    obj.insert(
        "source_generation".to_string(),
        json!(ctx.source_generation),
    );
    obj.insert("source_line_no".to_string(), json!(ctx.source_line_no));
    obj.insert("source_offset".to_string(), json!(ctx.source_offset));
    obj.insert(
        "source_ref".to_string(),
        Value::String(format!(
            "{}:{}:{}",
            ctx.source_file, ctx.source_generation, ctx.source_line_no
        )),
    );
    obj.insert(
        "record_ts".to_string(),
        Value::String(ctx.record_ts.to_string()),
    );
    obj.insert(
        "event_ts".to_string(),
        Value::String(ctx.event_ts.to_string()),
    );
    obj.insert(
        "event_kind".to_string(),
        Value::String(event_kind.to_string()),
    );
    obj.insert(
        "actor_kind".to_string(),
        Value::String(actor_kind.to_string()),
    );
    obj.insert(
        "payload_type".to_string(),
        Value::String(payload_type.to_string()),
    );
    obj.insert("op_kind".to_string(), Value::String(String::new()));
    obj.insert("op_status".to_string(), Value::String(String::new()));
    obj.insert("request_id".to_string(), Value::String(String::new()));
    obj.insert("trace_id".to_string(), Value::String(String::new()));
    obj.insert("turn_index".to_string(), json!(0u32));
    obj.insert("item_id".to_string(), Value::String(String::new()));
    obj.insert("tool_call_id".to_string(), Value::String(String::new()));
    obj.insert(
        "parent_tool_call_id".to_string(),
        Value::String(String::new()),
    );
    obj.insert("origin_event_id".to_string(), Value::String(String::new()));
    obj.insert(
        "origin_tool_call_id".to_string(),
        Value::String(String::new()),
    );
    obj.insert("tool_name".to_string(), Value::String(String::new()));
    obj.insert("tool_phase".to_string(), Value::String(String::new()));
    obj.insert("tool_error".to_string(), json!(0u8));
    obj.insert("agent_run_id".to_string(), Value::String(String::new()));
    obj.insert("agent_label".to_string(), Value::String(String::new()));
    obj.insert("coord_group_id".to_string(), Value::String(String::new()));
    obj.insert(
        "coord_group_label".to_string(),
        Value::String(String::new()),
    );
    obj.insert("is_substream".to_string(), json!(0u8));
    obj.insert("model".to_string(), Value::String(String::new()));
    obj.insert("input_tokens".to_string(), json!(0u32));
    obj.insert("output_tokens".to_string(), json!(0u32));
    obj.insert("cache_read_tokens".to_string(), json!(0u32));
    obj.insert("cache_write_tokens".to_string(), json!(0u32));
    obj.insert("latency_ms".to_string(), json!(0u32));
    obj.insert("retry_count".to_string(), json!(0u16));
    obj.insert("service_tier".to_string(), Value::String(String::new()));
    obj.insert("content_types".to_string(), json!([]));
    obj.insert("has_reasoning".to_string(), json!(0u8));
    obj.insert(
        "text_content".to_string(),
        Value::String(text_content.clone()),
    );
    obj.insert(
        "text_preview".to_string(),
        Value::String(truncate_chars(&text_content, PREVIEW_LIMIT)),
    );
    obj.insert(
        "payload_json".to_string(),
        Value::String(payload_json.to_string()),
    );
    obj.insert("token_usage_json".to_string(), Value::String(String::new()));
    obj.insert("event_version".to_string(), json!(event_version()));
    obj
}

fn build_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_event_uid: &str,
    linked_external_id: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    let link_type = canonicalize_link_type(link_type);
    json!({
        "event_uid": event_uid,
        "linked_event_uid": linked_event_uid,
        "linked_external_id": linked_external_id,
        "link_type": link_type,
        "session_id": ctx.session_id,
        "harness": ctx.harness,
        "inference_provider": ctx.inference_provider,
        "source_name": ctx.source_name,
        "metadata_json": metadata_json,
        "event_version": event_version(),
    })
}

fn build_event_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_event_uid: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    build_link_row(
        ctx,
        event_uid,
        linked_event_uid,
        "",
        link_type,
        metadata_json,
    )
}

fn build_external_link_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    linked_external_id: &str,
    link_type: &str,
    metadata_json: &str,
) -> Value {
    build_link_row(
        ctx,
        event_uid,
        "",
        linked_external_id,
        link_type,
        metadata_json,
    )
}

fn build_tool_row(
    ctx: &RecordContext<'_>,
    event_uid: &str,
    tool_call_id: &str,
    parent_tool_call_id: &str,
    tool_name: &str,
    tool_phase: &str,
    tool_error: u8,
    input_json: &str,
    output_json: &str,
    output_text: &str,
) -> Value {
    let input_json = truncate_chars(input_json, TEXT_LIMIT);
    let output_json = truncate_chars(output_json, TEXT_LIMIT);
    let output_text = truncate_chars(output_text, TEXT_LIMIT);

    json!({
        "event_uid": event_uid,
        "session_id": ctx.session_id,
        "harness": ctx.harness,
        "inference_provider": ctx.inference_provider,
        "source_name": ctx.source_name,
        "tool_call_id": tool_call_id,
        "parent_tool_call_id": parent_tool_call_id,
        "tool_name": tool_name,
        "tool_phase": tool_phase,
        "tool_error": tool_error,
        "input_json": input_json,
        "output_json": output_json,
        "output_text": output_text,
        "input_bytes": input_json.len() as u32,
        "output_bytes": output_json.len() as u32,
        "input_preview": truncate_chars(&input_json, PREVIEW_LIMIT),
        "output_preview": truncate_chars(&output_text, PREVIEW_LIMIT),
        "io_hash": io_hash(&input_json, &output_json),
        "source_ref": format!("{}:{}:{}", ctx.source_file, ctx.source_generation, ctx.source_line_no),
        "event_version": event_version(),
    })
}

#[derive(Debug)]
enum HermesSegment {
    Text(String),
    Think(String),
    ToolCall(String),
    ToolResponse(String),
}

#[derive(Debug)]
struct HermesPendingToolCall {
    event_idx: usize,
    tool_idx: usize,
    tool_call_id: String,
    tool_name: String,
}

fn parse_hermes_segments(input: &str) -> Vec<HermesSegment> {
    const TAGS: [(&str, &str); 3] = [
        ("<think>", "</think>"),
        ("<tool_call>", "</tool_call>"),
        ("<tool_response>", "</tool_response>"),
    ];

    let mut out = Vec::new();
    let mut cursor = 0usize;

    while cursor < input.len() {
        let next = TAGS
            .iter()
            .filter_map(|(start_tag, end_tag)| {
                input[cursor..]
                    .find(start_tag)
                    .map(|relative| (cursor + relative, *start_tag, *end_tag))
            })
            .min_by_key(|(idx, _, _)| *idx);

        let Some((start_idx, start_tag, end_tag)) = next else {
            let tail = input[cursor..].trim();
            if !tail.is_empty() {
                out.push(HermesSegment::Text(tail.to_string()));
            }
            break;
        };

        let prefix = input[cursor..start_idx].trim();
        if !prefix.is_empty() {
            out.push(HermesSegment::Text(prefix.to_string()));
        }

        let body_start = start_idx + start_tag.len();
        let Some(end_relative) = input[body_start..].find(end_tag) else {
            let tail = input[start_idx..].trim();
            if !tail.is_empty() {
                out.push(HermesSegment::Text(tail.to_string()));
            }
            break;
        };

        let body_end = body_start + end_relative;
        let body = input[body_start..body_end].trim().to_string();
        match start_tag {
            "<think>" => out.push(HermesSegment::Think(body)),
            "<tool_call>" => out.push(HermesSegment::ToolCall(body)),
            "<tool_response>" => out.push(HermesSegment::ToolResponse(body)),
            _ => {}
        }
        cursor = body_end + end_tag.len();
    }

    out
}

fn hermes_session_id(base_uid: &str) -> String {
    format!("hermes:{base_uid}")
}

fn kimi_session_id(source_file: &str, session_hint: &str) -> String {
    if !session_hint.is_empty() {
        return session_hint.to_string();
    }
    let path = std::path::Path::new(source_file);
    if let Some(parent) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
    {
        if !parent.is_empty() {
            return format!("kimi-cli:{parent}");
        }
    }
    infer_session_id_from_file(source_file)
}

fn synthetic_kimi_timestamp(source_line_no: u64) -> (String, String, bool) {
    let base = DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch");
    let dt = base + Duration::microseconds(source_line_no as i64);
    (format_record_ts(&dt), format_event_ts(&dt), false)
}

fn parse_kimi_timestamp(record: &Value, source_line_no: u64) -> (String, String, bool) {
    if let Some(ts_val) = record.get("timestamp") {
        let formatted = match ts_val {
            Value::Number(n) => format_unix_seconds_decimal(&n.to_string())
                .or_else(|| n.as_f64().and_then(format_unix_seconds_ts)),
            Value::String(s) => format_unix_seconds_decimal(s).or_else(|| {
                s.parse::<f64>()
                    .ok()
                    .and_then(format_unix_seconds_ts)
                    .or_else(|| parse_record_ts(s).map(|dt| format_record_ts(&dt)))
            }),
            _ => None,
        };

        if let Some(record_ts) = formatted {
            if let Some(dt) = parse_record_ts(&record_ts) {
                return (record_ts, format_event_ts(&dt), false);
            }
        }

        let raw_ts = to_str(Some(ts_val));
        if !raw_ts.trim().is_empty() {
            let (event_ts, event_ts_parse_failed) = parse_event_ts(&raw_ts);
            return (raw_ts, event_ts, event_ts_parse_failed);
        }
    }

    synthetic_kimi_timestamp(source_line_no)
}

fn hermes_status(record: &Value) -> String {
    if to_u8_bool(record.get("partial")) != 0 {
        "partial".to_string()
    } else if record.get("completed").is_some() {
        if to_u8_bool(record.get("completed")) != 0 {
            "completed".to_string()
        } else {
            "failed".to_string()
        }
    } else {
        String::new()
    }
}

fn hermes_metadata_payload(record: &Value) -> Value {
    let mut meta = record.as_object().cloned().unwrap_or_else(Map::new);
    meta.remove("conversations");
    Value::Object(meta)
}

fn hermes_event_dt(base_dt: Option<DateTime<Utc>>, index: usize) -> DateTime<Utc> {
    let base =
        base_dt.unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"));
    base + Duration::microseconds(index as i64)
}

fn hermes_stamp_time(row: &mut Value, dt: &DateTime<Utc>) {
    update_string_field(row, "record_ts", &format_record_ts(dt));
    update_string_field(row, "event_ts", &format_event_ts(dt));
}

fn normalize_hermes_trajectory(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
    model: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();
    let mut pending_tool_calls = VecDeque::<HermesPendingToolCall>::new();
    let mut current_turn = 0u32;
    let base_dt = parse_record_ts(ctx.record_ts);
    // The caller (`normalize_record`) has already split the record's
    // `vendor/model` string into `inference_provider` (stored on ctx) and the
    // verbatim `model` name. We intentionally do NOT call `canonicalize_model`
    // here: Hermes models flow through verbatim (modulo lowercase+trim) so
    // catalog strings like `claude-sonnet-4.6` are preserved end-to-end.
    let model = model.to_string();
    let status = hermes_status(record);
    let metadata_payload = hermes_metadata_payload(record);

    let mut session_meta = Value::Object(base_event_obj(
        ctx,
        base_uid,
        "session_meta",
        "session_meta",
        "system",
        "",
        &compact_json(&metadata_payload),
    ));
    if !model.is_empty() {
        update_string_field(&mut session_meta, "model", &model);
    }
    if !status.is_empty() {
        update_string_field(&mut session_meta, "op_status", &status);
    }
    let prompt_index = to_str(record.get("prompt_index"));
    if !prompt_index.is_empty() {
        update_string_field(&mut session_meta, "item_id", &prompt_index);
    }
    hermes_stamp_time(&mut session_meta, &hermes_event_dt(base_dt, 0));
    events.push(session_meta);

    let conversations = record
        .get("conversations")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut event_index = 1usize;
    for (conv_idx, item) in conversations.iter().enumerate() {
        let role = to_str(item.get("from"));
        let value = to_str(item.get("value"));
        let current_turn_for_item = current_turn;

        let next_uid = |suffix: &str| {
            event_uid(
                ctx.source_file,
                ctx.source_generation,
                ctx.source_line_no,
                ctx.source_offset,
                &compact_json(item),
                &format!("hermes:{conv_idx}:{suffix}"),
            )
        };

        match role.as_str() {
            "system" => {
                let mut row = Value::Object(base_event_obj(
                    ctx,
                    &next_uid("system"),
                    "system",
                    "system",
                    "system",
                    &value,
                    &compact_json(item),
                ));
                if !model.is_empty() {
                    update_string_field(&mut row, "model", &model);
                }
                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                events.push(row);
                event_index += 1;
            }
            "human" => {
                current_turn = current_turn.saturating_add(1).max(1);
                let mut row = Value::Object(base_event_obj(
                    ctx,
                    &next_uid("human"),
                    "message",
                    "message",
                    "user",
                    &value,
                    &compact_json(item),
                ));
                update_string_field(&mut row, "model", &model);
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("content_types".to_string(), json!(["text"]));
                    obj.insert("turn_index".to_string(), json!(current_turn));
                }
                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                events.push(row);
                event_index += 1;
            }
            "gpt" => {
                let segments = parse_hermes_segments(&value);
                for (segment_idx, segment) in segments.into_iter().enumerate() {
                    let suffix = match &segment {
                        HermesSegment::Text(_) => format!("assistant:text:{segment_idx}"),
                        HermesSegment::Think(_) => format!("assistant:think:{segment_idx}"),
                        HermesSegment::ToolCall(_) => format!("assistant:tool_call:{segment_idx}"),
                        HermesSegment::ToolResponse(_) => {
                            format!("assistant:tool_response:{segment_idx}")
                        }
                    };
                    let segment_uid = next_uid(&suffix);

                    match segment {
                        HermesSegment::Text(text) => {
                            if text.is_empty() {
                                continue;
                            }
                            let mut row = Value::Object(base_event_obj(
                                ctx,
                                &segment_uid,
                                "message",
                                "message",
                                "assistant",
                                &text,
                                &compact_json(&json!({
                                    "from": "gpt",
                                    "type": "text",
                                    "value": text,
                                })),
                            ));
                            if let Some(obj) = row.as_object_mut() {
                                obj.insert("content_types".to_string(), json!(["text"]));
                                obj.insert("turn_index".to_string(), json!(current_turn_for_item));
                            }
                            update_string_field(&mut row, "model", &model);
                            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                            events.push(row);
                            event_index += 1;
                        }
                        HermesSegment::Think(thinking) => {
                            let mut row = Value::Object(base_event_obj(
                                ctx,
                                &segment_uid,
                                "reasoning",
                                "thinking",
                                "assistant",
                                &thinking,
                                &compact_json(&json!({
                                    "from": "gpt",
                                    "type": "thinking",
                                    "value": thinking,
                                })),
                            ));
                            if let Some(obj) = row.as_object_mut() {
                                obj.insert("content_types".to_string(), json!(["thinking"]));
                                obj.insert("has_reasoning".to_string(), json!(1u8));
                                obj.insert("turn_index".to_string(), json!(current_turn_for_item));
                            }
                            update_string_field(&mut row, "model", &model);
                            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                            events.push(row);
                            event_index += 1;
                        }
                        HermesSegment::ToolCall(raw_call) => {
                            let parsed_call = parse_json_string(&raw_call)
                                .unwrap_or_else(|| json!({ "raw": raw_call }));
                            let tool_name = to_str(parsed_call.get("name"));
                            let arguments =
                                parsed_call.get("arguments").cloned().unwrap_or(Value::Null);
                            let input_json = compact_json(&arguments);
                            let input_text = {
                                let extracted = extract_message_text(&arguments);
                                if extracted.is_empty() {
                                    input_json.clone()
                                } else {
                                    extracted
                                }
                            };
                            let mut tool_call_id = to_str(parsed_call.get("tool_call_id"));
                            if tool_call_id.is_empty() {
                                tool_call_id = format!("hermes-call-{conv_idx}-{segment_idx}");
                            }

                            let mut row = Value::Object(base_event_obj(
                                ctx,
                                &segment_uid,
                                "tool_call",
                                "tool_use",
                                "assistant",
                                &input_text,
                                &compact_json(&parsed_call),
                            ));
                            if let Some(obj) = row.as_object_mut() {
                                obj.insert("content_types".to_string(), json!(["tool_use"]));
                                obj.insert("turn_index".to_string(), json!(current_turn_for_item));
                            }
                            update_string_field(&mut row, "tool_call_id", &tool_call_id);
                            update_string_field(&mut row, "tool_name", &tool_name);
                            update_string_field(&mut row, "model", &model);
                            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                            events.push(row);

                            let tool_idx = tools.len();
                            tools.push(build_tool_row(
                                ctx,
                                &segment_uid,
                                &tool_call_id,
                                "",
                                &tool_name,
                                "request",
                                0,
                                &input_json,
                                "",
                                "",
                            ));
                            pending_tool_calls.push_back(HermesPendingToolCall {
                                event_idx: events.len() - 1,
                                tool_idx,
                                tool_call_id,
                                tool_name,
                            });
                            event_index += 1;
                        }
                        HermesSegment::ToolResponse(raw_response) => {
                            let text = raw_response.trim();
                            if !text.is_empty() {
                                let mut row = Value::Object(base_event_obj(
                                    ctx,
                                    &segment_uid,
                                    "tool_result",
                                    "tool_result",
                                    "tool",
                                    text,
                                    &compact_json(&json!({
                                        "from": "gpt",
                                        "type": "tool_response_text",
                                        "value": text,
                                    })),
                                ));
                                if let Some(obj) = row.as_object_mut() {
                                    obj.insert("content_types".to_string(), json!(["tool_result"]));
                                    obj.insert(
                                        "turn_index".to_string(),
                                        json!(current_turn_for_item),
                                    );
                                }
                                update_string_field(&mut row, "model", &model);
                                update_u8_field(&mut row, "tool_error", 1);
                                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                                events.push(row);
                                event_index += 1;
                            }
                        }
                    }
                }
            }
            "tool" => {
                let segments = parse_hermes_segments(&value);
                for (segment_idx, segment) in segments.into_iter().enumerate() {
                    match segment {
                        HermesSegment::ToolResponse(raw_response) => {
                            let parsed_response = parse_json_string(&raw_response)
                                .unwrap_or_else(|| json!({ "content": raw_response }));
                            let pending = pending_tool_calls.pop_front();
                            let pending_tool_name = pending
                                .as_ref()
                                .map(|call| call.tool_name.clone())
                                .unwrap_or_default();
                            let response_call_id = to_str(parsed_response.get("tool_call_id"));
                            let mut tool_call_id = response_call_id.clone();
                            if tool_call_id.is_empty() {
                                if let Some(call) = pending.as_ref() {
                                    tool_call_id = call.tool_call_id.clone();
                                } else {
                                    tool_call_id =
                                        format!("hermes-result-{conv_idx}-{segment_idx}");
                                }
                            }

                            if let Some(call) = pending {
                                if response_call_id != call.tool_call_id
                                    && !response_call_id.is_empty()
                                {
                                    update_string_field(
                                        &mut events[call.event_idx],
                                        "tool_call_id",
                                        &response_call_id,
                                    );
                                    update_string_field(
                                        &mut tools[call.tool_idx],
                                        "tool_call_id",
                                        &response_call_id,
                                    );
                                    tool_call_id = response_call_id;
                                }
                            }

                            let tool_name = {
                                let name = to_str(parsed_response.get("name"));
                                if !name.is_empty() {
                                    name
                                } else {
                                    pending_tool_name
                                }
                            };
                            let content = parsed_response
                                .get("content")
                                .cloned()
                                .unwrap_or(Value::Null);
                            let output_json = compact_json(&content);
                            let output_text = {
                                let extracted = extract_message_text(&content);
                                if extracted.is_empty() {
                                    output_json.clone()
                                } else {
                                    extracted
                                }
                            };
                            let segment_uid = next_uid(&format!("tool:response:{segment_idx}"));
                            let mut row = Value::Object(base_event_obj(
                                ctx,
                                &segment_uid,
                                "tool_result",
                                "tool_result",
                                "tool",
                                &output_text,
                                &compact_json(&parsed_response),
                            ));
                            if let Some(obj) = row.as_object_mut() {
                                obj.insert("content_types".to_string(), json!(["tool_result"]));
                                obj.insert("turn_index".to_string(), json!(current_turn_for_item));
                            }
                            update_string_field(&mut row, "tool_call_id", &tool_call_id);
                            update_string_field(&mut row, "tool_name", &tool_name);
                            update_string_field(&mut row, "model", &model);
                            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                            events.push(row);
                            tools.push(build_tool_row(
                                ctx,
                                &segment_uid,
                                &tool_call_id,
                                "",
                                &tool_name,
                                "response",
                                0,
                                "",
                                &output_json,
                                &output_text,
                            ));
                            event_index += 1;
                        }
                        HermesSegment::Text(text) => {
                            if text.is_empty() {
                                continue;
                            }
                            let segment_uid = next_uid(&format!("tool:text:{segment_idx}"));
                            let mut row = Value::Object(base_event_obj(
                                ctx,
                                &segment_uid,
                                "tool_result",
                                "tool_result",
                                "tool",
                                &text,
                                &compact_json(&json!({
                                    "from": "tool",
                                    "type": "tool_text",
                                    "value": text,
                                })),
                            ));
                            if let Some(obj) = row.as_object_mut() {
                                obj.insert("content_types".to_string(), json!(["tool_result"]));
                                obj.insert("turn_index".to_string(), json!(current_turn_for_item));
                            }
                            update_string_field(&mut row, "model", &model);
                            update_u8_field(&mut row, "tool_error", 1);
                            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                            events.push(row);
                            event_index += 1;
                        }
                        HermesSegment::Think(_) | HermesSegment::ToolCall(_) => {}
                    }
                }
            }
            _ => {
                let segment_uid = next_uid("unknown");
                let mut row = Value::Object(base_event_obj(
                    ctx,
                    &segment_uid,
                    "unknown",
                    "unknown",
                    "system",
                    &value,
                    &compact_json(item),
                ));
                update_string_field(&mut row, "model", &model);
                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, event_index));
                events.push(row);
                event_index += 1;
            }
        }
    }

    (events, links, tools)
}

/// Normalize a synthetic `session_meta` record emitted by the session_json
/// processor for Hermes live sessions. The record carries the session header
/// (session_id, model, base_url, platform, session_start, last_updated,
/// system_prompt, tools, message_count). We emit a single `session_meta` event.
fn normalize_hermes_session_meta(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let tools = Vec::<Value>::new();

    let mut events = events;
    let base_dt = parse_record_ts(ctx.record_ts);
    let platform = to_str(record.get("platform"));
    let base_url = to_str(record.get("base_url"));
    let model_raw = to_str(record.get("model"));
    let (_vendor, model) = split_hermes_vendor_model(&model_raw);

    let mut meta_payload = Map::<String, Value>::new();
    meta_payload.insert(
        "session_id".to_string(),
        Value::String(to_str(record.get("session_id"))),
    );
    meta_payload.insert("model".to_string(), Value::String(model.clone()));
    meta_payload.insert("base_url".to_string(), Value::String(base_url.clone()));
    meta_payload.insert("platform".to_string(), Value::String(platform.clone()));
    meta_payload.insert(
        "session_start".to_string(),
        Value::String(to_str(record.get("session_start"))),
    );
    meta_payload.insert(
        "last_updated".to_string(),
        Value::String(to_str(record.get("last_updated"))),
    );
    if let Some(system_prompt) = record.get("system_prompt") {
        meta_payload.insert("system_prompt".to_string(), system_prompt.clone());
    }
    if let Some(tools_value) = record.get("tools") {
        meta_payload.insert("tools".to_string(), tools_value.clone());
    }
    if let Some(message_count) = record.get("message_count") {
        meta_payload.insert("message_count".to_string(), message_count.clone());
    }

    let payload_json = compact_json(&Value::Object(meta_payload));

    let uid = event_uid(
        ctx.source_file,
        ctx.source_generation,
        ctx.source_line_no,
        ctx.source_offset,
        &payload_json,
        "session_meta",
    );
    let _ = base_uid;

    let mut row = Value::Object(base_event_obj(
        ctx,
        &uid,
        "session_meta",
        "session_meta",
        "system",
        "",
        &payload_json,
    ));
    if !model.is_empty() {
        update_string_field(&mut row, "model", &model);
    }
    if !platform.is_empty() {
        update_string_field(&mut row, "agent_label", &platform);
    }
    hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, 0));
    events.push(row);

    (events, links, tools)
}

/// Normalize a synthetic `session_message` record: one OpenAI chat-completions
/// message from a live Hermes session (role ∈ {user, assistant, tool, system},
/// plus optional tool_calls / reasoning / tool_call_id). Tool call / result
/// correlation travels through `tool_call_id` on each emitted row — the
/// OpenAI schema carries it on both sides, so no in-record tracking is needed.
#[allow(unused_assignments)]
fn normalize_hermes_session_message(
    record: &Value,
    ctx: &RecordContext<'_>,
    base_uid: &str,
    model: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();
    let base_dt = parse_record_ts(ctx.record_ts);
    let model = model.to_string();
    let _ = base_uid;

    let message = match record.get("message") {
        Some(Value::Object(_)) => record.get("message").unwrap(),
        _ => return (events, links, tools),
    };

    let message_index = record
        .get("message_index")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let role = to_str(message.get("role"));
    // For turn_index we use a 1-based message index: plenty for ordering, and
    // ClickHouse schema uses UInt32.
    let turn_index: u32 = ((message_index + 1).min(u32::MAX as u64)) as u32;

    let compact_message = compact_json(message);
    let next_uid = |suffix: &str| {
        event_uid(
            ctx.source_file,
            ctx.source_generation,
            ctx.source_line_no,
            ctx.source_offset,
            &compact_message,
            &format!("hermes_session:{message_index}:{suffix}"),
        )
    };

    let content_value = message.get("content").cloned().unwrap_or(Value::Null);

    let mut sub_event_index = 0usize;

    match role.as_str() {
        "user" => {
            let text = extract_message_text(&content_value);
            let mut row = Value::Object(base_event_obj(
                ctx,
                &next_uid("user"),
                "message",
                "user_message",
                "user",
                &text,
                &compact_json(message),
            ));
            if !model.is_empty() {
                update_string_field(&mut row, "model", &model);
            }
            if let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(&content_value)),
                );
                obj.insert("turn_index".to_string(), json!(turn_index));
            }
            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
            events.push(row);
            sub_event_index += 1;
        }
        "assistant" => {
            // Emit reasoning first if present — matches the wall-clock order of
            // thinking → text → tool_calls in a single assistant turn.
            let reasoning = message.get("reasoning").cloned().unwrap_or(Value::Null);
            let reasoning_text = match &reasoning {
                Value::Null => String::new(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if !reasoning_text.trim().is_empty() {
                let mut row = Value::Object(base_event_obj(
                    ctx,
                    &next_uid("reasoning"),
                    "reasoning",
                    "thinking",
                    "assistant",
                    &reasoning_text,
                    &compact_json(&json!({
                        "role": "assistant",
                        "reasoning": reasoning,
                    })),
                ));
                if !model.is_empty() {
                    update_string_field(&mut row, "model", &model);
                }
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("content_types".to_string(), json!(["thinking"]));
                    obj.insert("has_reasoning".to_string(), json!(1u8));
                    obj.insert("turn_index".to_string(), json!(turn_index));
                }
                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
                events.push(row);
                sub_event_index += 1;
            }

            let text = extract_message_text(&content_value);
            if !text.trim().is_empty() {
                let mut row = Value::Object(base_event_obj(
                    ctx,
                    &next_uid("assistant"),
                    "message",
                    "agent_message",
                    "assistant",
                    &text,
                    &compact_json(&json!({
                        "role": "assistant",
                        "content": content_value,
                    })),
                ));
                if !model.is_empty() {
                    update_string_field(&mut row, "model", &model);
                }
                if let Some(obj) = row.as_object_mut() {
                    obj.insert(
                        "content_types".to_string(),
                        json!(extract_content_types(&content_value)),
                    );
                    obj.insert("turn_index".to_string(), json!(turn_index));
                }
                let finish_reason = to_str(message.get("finish_reason"));
                if !finish_reason.is_empty() {
                    update_string_field(&mut row, "op_status", &finish_reason);
                }
                hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
                events.push(row);
                sub_event_index += 1;
            }

            if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                for (call_idx, call) in tool_calls.iter().enumerate() {
                    let tool_call_id = to_str(call.get("id"));
                    let function = call.get("function").cloned().unwrap_or(Value::Null);
                    let tool_name = to_str(function.get("name"));
                    let arguments_raw = to_str(function.get("arguments"));
                    let arguments = parse_json_string(&arguments_raw).unwrap_or_else(|| {
                        if arguments_raw.is_empty() {
                            Value::Object(Map::new())
                        } else {
                            json!({ "raw": arguments_raw })
                        }
                    });
                    let input_json = compact_json(&arguments);
                    let input_text = {
                        let extracted = extract_message_text(&arguments);
                        if extracted.is_empty() {
                            input_json.clone()
                        } else {
                            extracted
                        }
                    };

                    let uid = next_uid(&format!("tool_call:{call_idx}"));
                    let mut row = Value::Object(base_event_obj(
                        ctx,
                        &uid,
                        "tool_call",
                        "tool_use",
                        "assistant",
                        &input_text,
                        &compact_json(call),
                    ));
                    if !model.is_empty() {
                        update_string_field(&mut row, "model", &model);
                    }
                    update_string_field(&mut row, "tool_call_id", &tool_call_id);
                    update_string_field(&mut row, "tool_name", &tool_name);
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert("content_types".to_string(), json!(["tool_use"]));
                        obj.insert("turn_index".to_string(), json!(turn_index));
                    }
                    hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
                    events.push(row);
                    sub_event_index += 1;

                    tools.push(build_tool_row(
                        ctx,
                        &uid,
                        &tool_call_id,
                        "",
                        &tool_name,
                        "request",
                        0,
                        &input_json,
                        "",
                        "",
                    ));
                }
            }
        }
        "tool" => {
            let tool_call_id = to_str(message.get("tool_call_id"));
            let tool_name = to_str(message.get("name"));
            let text = extract_message_text(&content_value);
            let output_json = compact_json(&content_value);

            let uid = next_uid("tool_result");
            let mut row = Value::Object(base_event_obj(
                ctx,
                &uid,
                "tool_result",
                "tool_result",
                "tool",
                &text,
                &compact_json(message),
            ));
            if !model.is_empty() {
                update_string_field(&mut row, "model", &model);
            }
            update_string_field(&mut row, "tool_call_id", &tool_call_id);
            update_string_field(&mut row, "tool_name", &tool_name);
            if let Some(obj) = row.as_object_mut() {
                obj.insert("content_types".to_string(), json!(["tool_result"]));
                obj.insert("turn_index".to_string(), json!(turn_index));
            }
            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
            events.push(row);
            sub_event_index += 1;

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                &tool_name,
                "response",
                0,
                "",
                &output_json,
                &text,
            ));
        }
        "system" => {
            let text = extract_message_text(&content_value);
            let mut row = Value::Object(base_event_obj(
                ctx,
                &next_uid("system"),
                "system",
                "system",
                "system",
                &text,
                &compact_json(message),
            ));
            if !model.is_empty() {
                update_string_field(&mut row, "model", &model);
            }
            if let Some(obj) = row.as_object_mut() {
                obj.insert("turn_index".to_string(), json!(turn_index));
            }
            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
            events.push(row);
            sub_event_index += 1;
        }
        _ => {
            let text = extract_message_text(&content_value);
            let mut row = Value::Object(base_event_obj(
                ctx,
                &next_uid("unknown"),
                "unknown",
                "unknown",
                "system",
                &text,
                &compact_json(message),
            ));
            if !model.is_empty() {
                update_string_field(&mut row, "model", &model);
            }
            hermes_stamp_time(&mut row, &hermes_event_dt(base_dt, sub_event_index));
            events.push(row);
            sub_event_index += 1;
        }
    }

    (events, links, tools)
}

fn factory_droid_stamp_common(
    row: &mut Map<String, Value>,
    record: &Value,
    message: Option<&Value>,
    model: &str,
) {
    let message = message.unwrap_or(&Value::Null);
    row.insert("item_id".to_string(), json!(to_str(record.get("id"))));
    row.insert(
        "request_id".to_string(),
        json!(to_str(message.get("openaiMessageId"))),
    );
    row.insert(
        "trace_id".to_string(),
        json!(to_str(message.get("openaiReasoningId"))),
    );
    row.insert(
        "op_status".to_string(),
        json!(to_str(message.get("openaiPhase"))),
    );
    if !model.is_empty() {
        row.insert("model".to_string(), json!(model));
    }
}

fn factory_droid_push_parent_link(
    links: &mut Vec<Value>,
    ctx: &RecordContext<'_>,
    event_uid: &str,
    record: &Value,
) {
    let parent_id =
        to_str(record.get("parentId")).or_else_nonempty(|| to_str(record.get("parent_id")));
    if parent_id.is_empty() {
        return;
    }
    links.push(build_external_link_row(
        ctx,
        event_uid,
        &parent_id,
        "parent_event",
        "{}",
    ));
}

fn factory_droid_tool_call_id(block: &Value, fallback: &str) -> String {
    to_str(block.get("id"))
        .or_else_nonempty(|| to_str(block.get("tool_call_id")))
        .or_else_nonempty(|| to_str(block.get("call_id")))
        .or_else_nonempty(|| fallback.to_string())
}

fn normalize_factory_droid_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();
    let payload_json = factory_droid_payload_json(record);

    match top_type {
        "session_settings" => {
            let settings = record.get("settings").unwrap_or(record);
            let token_usage = settings.get("tokenUsage").cloned().unwrap_or(Value::Null);
            let settings_uid = event_uid(
                ctx.source_file,
                ctx.source_generation,
                0,
                0,
                ctx.session_id,
                "factory_droid:settings",
            );
            let mut row = base_event_obj(
                ctx,
                &settings_uid,
                "session_meta",
                "session_meta",
                "system",
                "",
                &payload_json,
            );
            row.insert("item_id".to_string(), json!(ctx.session_id));
            row.insert("agent_label".to_string(), json!("Factory Droid"));
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            row.insert(
                "input_tokens".to_string(),
                json!(to_u32(token_usage.get("inputTokens"))),
            );
            row.insert(
                "output_tokens".to_string(),
                json!(to_u32(token_usage.get("outputTokens"))),
            );
            row.insert(
                "cache_read_tokens".to_string(),
                json!(to_u32(token_usage.get("cacheReadTokens"))),
            );
            row.insert(
                "cache_write_tokens".to_string(),
                json!(to_u32(token_usage.get("cacheCreationTokens"))),
            );
            row.insert(
                "latency_ms".to_string(),
                json!(to_u32(settings.get("assistantActiveTimeMs"))),
            );
            if !token_usage.is_null() {
                row.insert(
                    "token_usage_json".to_string(),
                    json!(compact_json(&token_usage)),
                );
            }
            events.push(Value::Object(row));
        }
        "session_start" => {
            let title =
                to_str(record.get("sessionTitle")).or_else_nonempty(|| to_str(record.get("title")));
            let cwd = to_str(record.get("cwd"));
            let text = [title.as_str(), cwd.as_str()]
                .into_iter()
                .filter(|part| !part.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "session_meta",
                "session_meta",
                "system",
                &text,
                &payload_json,
            );
            let session_id = to_str(record.get("id"));
            row.insert("item_id".to_string(), json!(session_id.clone()));
            row.insert("agent_run_id".to_string(), json!(session_id));
            row.insert("agent_label".to_string(), json!("Factory Droid"));
            row.insert("coord_group_label".to_string(), json!(cwd));
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            events.push(Value::Object(row));
        }
        "message" => {
            let message = record.get("message").unwrap_or(&Value::Null);
            let role = to_str(message.get("role"));
            let actor = match role.as_str() {
                "assistant" => "assistant",
                "tool" => "tool",
                "system" => "system",
                _ => "user",
            };
            let content = message.get("content").cloned().unwrap_or(Value::Null);
            let turn_index = ctx.source_line_no.min(u32::MAX as u64) as u32;
            let base_message_payload = pruned_factory_droid_payload(message);
            let mut sub_index = 0usize;

            let reasoning = to_str(message.get("chatCompletionReasoningContent"))
                .or_else_nonempty(|| to_str(message.get("reasoning")));
            if !reasoning.trim().is_empty() {
                let uid = event_uid(
                    ctx.source_file,
                    ctx.source_generation,
                    ctx.source_line_no,
                    ctx.source_offset,
                    &payload_json,
                    "factory_droid:message:reasoning",
                );
                let mut row = base_event_obj(
                    ctx,
                    &uid,
                    "reasoning",
                    "thinking",
                    "assistant",
                    &reasoning,
                    &factory_droid_payload_json(&json!({
                        "role": "assistant",
                        "chatCompletionReasoningField": message.get("chatCompletionReasoningField").cloned().unwrap_or(Value::Null),
                        "chatCompletionReasoningContent": reasoning,
                    })),
                );
                row.insert("has_reasoning".to_string(), json!(1u8));
                row.insert("content_types".to_string(), json!(["thinking"]));
                row.insert("turn_index".to_string(), json!(turn_index));
                factory_droid_stamp_common(&mut row, record, Some(message), model);
                events.push(Value::Object(row));
                if let Some(uid) = events
                    .last()
                    .and_then(|v| v.get("event_uid"))
                    .and_then(Value::as_str)
                {
                    factory_droid_push_parent_link(&mut links, ctx, uid, record);
                }
                sub_index += 1;
            }

            match content {
                Value::Array(items) if !items.is_empty() => {
                    for (idx, item) in items.iter().enumerate() {
                        let block_type = to_str(item.get("type"));
                        let uid = event_uid(
                            ctx.source_file,
                            ctx.source_generation,
                            ctx.source_line_no,
                            ctx.source_offset,
                            &factory_droid_payload_json(item),
                            &format!("factory_droid:message:block:{idx}"),
                        );
                        let mut row = match block_type.as_str() {
                            "thinking" => {
                                let mut r = base_event_obj(
                                    ctx,
                                    &uid,
                                    "reasoning",
                                    "thinking",
                                    "assistant",
                                    &extract_message_text(item),
                                    &factory_droid_payload_json(item),
                                );
                                r.insert("has_reasoning".to_string(), json!(1u8));
                                r.insert("content_types".to_string(), json!(["thinking"]));
                                r
                            }
                            "tool_use" | "tool_call" | "function_call" => {
                                let fallback_id =
                                    format!("factory-droid-call-{}-{idx}", ctx.source_line_no);
                                let tool_call_id = factory_droid_tool_call_id(item, &fallback_id);
                                let tool_name = to_str(item.get("name")).or_else_nonempty(|| {
                                    to_str(item.get("function").and_then(|f| f.get("name")))
                                });
                                let input = item
                                    .get("input")
                                    .or_else(|| item.get("arguments"))
                                    .or_else(|| {
                                        item.get("function").and_then(|f| f.get("arguments"))
                                    })
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                let parsed_input = match input {
                                    Value::String(ref raw) => parse_json_string(raw)
                                        .unwrap_or_else(|| json!({ "raw": raw })),
                                    other => other,
                                };
                                let input_json = compact_json(&parsed_input);
                                let input_text = {
                                    let extracted = extract_message_text(&parsed_input);
                                    if extracted.is_empty() {
                                        input_json.clone()
                                    } else {
                                        extracted
                                    }
                                };
                                let mut r = base_event_obj(
                                    ctx,
                                    &uid,
                                    "tool_call",
                                    "tool_use",
                                    "assistant",
                                    &input_text,
                                    &factory_droid_payload_json(item),
                                );
                                r.insert("content_types".to_string(), json!(["tool_use"]));
                                r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                                r.insert("tool_name".to_string(), json!(tool_name.clone()));
                                tools.push(build_tool_row(
                                    ctx,
                                    &uid,
                                    &tool_call_id,
                                    "",
                                    &tool_name,
                                    "request",
                                    0,
                                    &input_json,
                                    "",
                                    "",
                                ));
                                r
                            }
                            "tool_result" | "function_call_output" => {
                                let tool_call_id = to_str(item.get("tool_use_id"))
                                    .or_else_nonempty(|| to_str(item.get("tool_call_id")))
                                    .or_else_nonempty(|| to_str(item.get("call_id")));
                                let content = item
                                    .get("content")
                                    .or_else(|| item.get("output"))
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                let output_json = compact_json(&content);
                                let output_text = {
                                    let extracted = extract_message_text(&content);
                                    if extracted.is_empty() {
                                        output_json.clone()
                                    } else {
                                        extracted
                                    }
                                };
                                let tool_error = to_u8_bool(item.get("is_error"));
                                let mut r = base_event_obj(
                                    ctx,
                                    &uid,
                                    "tool_result",
                                    "tool_result",
                                    "tool",
                                    &output_text,
                                    &factory_droid_payload_json(item),
                                );
                                r.insert("content_types".to_string(), json!(["tool_result"]));
                                r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                                r.insert("tool_error".to_string(), json!(tool_error));
                                tools.push(build_tool_row(
                                    ctx,
                                    &uid,
                                    &tool_call_id,
                                    "",
                                    "",
                                    "response",
                                    tool_error,
                                    "",
                                    &output_json,
                                    &output_text,
                                ));
                                r
                            }
                            _ => {
                                let mut r = base_event_obj(
                                    ctx,
                                    &uid,
                                    if actor == "tool" {
                                        "tool_result"
                                    } else {
                                        "message"
                                    },
                                    if block_type.is_empty() {
                                        if actor == "assistant" {
                                            "agent_message"
                                        } else if actor == "user" {
                                            "user_message"
                                        } else {
                                            "text"
                                        }
                                    } else {
                                        block_type.as_str()
                                    },
                                    actor,
                                    &extract_message_text(item),
                                    &factory_droid_payload_json(item),
                                );
                                if !block_type.is_empty() {
                                    r.insert("content_types".to_string(), json!([block_type]));
                                }
                                r
                            }
                        };
                        row.insert("turn_index".to_string(), json!(turn_index));
                        factory_droid_stamp_common(&mut row, record, Some(message), model);
                        events.push(Value::Object(row));
                        if let Some(uid) = events
                            .last()
                            .and_then(|v| v.get("event_uid"))
                            .and_then(Value::as_str)
                        {
                            factory_droid_push_parent_link(&mut links, ctx, uid, record);
                        }
                    }
                }
                _ => {
                    let text = extract_message_text(&base_message_payload);
                    let uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &payload_json,
                        &format!("factory_droid:message:{sub_index}"),
                    );
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        if actor == "tool" {
                            "tool_result"
                        } else {
                            "message"
                        },
                        if actor == "assistant" {
                            "agent_message"
                        } else if actor == "user" {
                            "user_message"
                        } else if actor == "system" {
                            "system"
                        } else {
                            "tool_result"
                        },
                        actor,
                        &text,
                        &factory_droid_payload_json(&base_message_payload),
                    );
                    row.insert("turn_index".to_string(), json!(turn_index));
                    row.insert(
                        "content_types".to_string(),
                        json!(extract_content_types(
                            message.get("content").unwrap_or(&Value::Null)
                        )),
                    );
                    factory_droid_stamp_common(&mut row, record, Some(message), model);
                    events.push(Value::Object(row));
                    if let Some(uid) = events
                        .last()
                        .and_then(|v| v.get("event_uid"))
                        .and_then(Value::as_str)
                    {
                        factory_droid_push_parent_link(&mut links, ctx, uid, record);
                    }
                }
            }

            if let Some(tool_calls) = message
                .get("tool_calls")
                .or_else(|| message.get("toolCalls"))
                .and_then(Value::as_array)
            {
                for (call_idx, call) in tool_calls.iter().enumerate() {
                    let uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &factory_droid_payload_json(call),
                        &format!("factory_droid:message:tool_call:{call_idx}"),
                    );
                    let fallback_id =
                        format!("factory-droid-call-{}-{call_idx}", ctx.source_line_no);
                    let tool_call_id = factory_droid_tool_call_id(call, &fallback_id);
                    let function = call.get("function").unwrap_or(&Value::Null);
                    let tool_name =
                        to_str(call.get("name")).or_else_nonempty(|| to_str(function.get("name")));
                    let arguments_raw = to_str(function.get("arguments"))
                        .or_else_nonempty(|| to_str(call.get("arguments")));
                    let arguments = parse_json_string(&arguments_raw).unwrap_or_else(|| {
                        if arguments_raw.is_empty() {
                            Value::Object(Map::new())
                        } else {
                            json!({ "raw": arguments_raw })
                        }
                    });
                    let input_json = compact_json(&arguments);
                    let input_text = {
                        let extracted = extract_message_text(&arguments);
                        if extracted.is_empty() {
                            input_json.clone()
                        } else {
                            extracted
                        }
                    };
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        "tool_call",
                        "tool_use",
                        "assistant",
                        &input_text,
                        &factory_droid_payload_json(call),
                    );
                    row.insert("turn_index".to_string(), json!(turn_index));
                    row.insert("content_types".to_string(), json!(["tool_use"]));
                    row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                    row.insert("tool_name".to_string(), json!(tool_name.clone()));
                    factory_droid_stamp_common(&mut row, record, Some(message), model);
                    events.push(Value::Object(row));
                    tools.push(build_tool_row(
                        ctx,
                        &uid,
                        &tool_call_id,
                        "",
                        &tool_name,
                        "request",
                        0,
                        &input_json,
                        "",
                        "",
                    ));
                    if let Some(uid) = events
                        .last()
                        .and_then(|v| v.get("event_uid"))
                        .and_then(Value::as_str)
                    {
                        factory_droid_push_parent_link(&mut links, ctx, uid, record);
                    }
                }
            }
        }
        "compaction_state" => {
            let summary = to_str(record.get("summaryText"));
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "summary",
                "summary",
                "system",
                &summary,
                &payload_json,
            );
            row.insert("item_id".to_string(), json!(to_str(record.get("id"))));
            row.insert("agent_label".to_string(), json!("Factory Droid"));
            row.insert(
                "origin_event_id".to_string(),
                json!(to_str(
                    record.get("anchorMessage").and_then(|a| a.get("id"))
                )),
            );
            row.insert(
                "turn_index".to_string(),
                json!(to_u32(
                    record.get("anchorMessage").and_then(|a| a.get("index"))
                )),
            );
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            let summary_tokens = to_u32(record.get("summaryTokens"));
            if summary_tokens > 0 {
                row.insert(
                    "token_usage_json".to_string(),
                    json!(compact_json(&json!({ "summaryTokens": summary_tokens }))),
                );
            }
            events.push(Value::Object(row));
            if let Some(uid) = events
                .last()
                .and_then(|v| v.get("event_uid"))
                .and_then(Value::as_str)
            {
                let anchor_id = to_str(record.get("anchorMessage").and_then(|a| a.get("id")));
                if !anchor_id.is_empty() {
                    links.push(build_external_link_row(
                        ctx,
                        uid,
                        &anchor_id,
                        "parent_event",
                        &compact_json(record.get("anchorMessage").unwrap_or(&Value::Null)),
                    ));
                }
            }
        }
        _ => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "unknown",
                if top_type.is_empty() {
                    "unknown"
                } else {
                    top_type
                },
                "system",
                &extract_message_text(record),
                &payload_json,
            );
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            events.push(Value::Object(row));
        }
    }

    (events, links, tools)
}

fn normalize_codex_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model_hint: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let payload = record.get("payload").cloned().unwrap_or(Value::Null);
    let payload_obj = payload.as_object().cloned().unwrap_or_else(Map::new);
    let payload_json = compact_json(&Value::Object(payload_obj.clone()));

    let push_parent_link = |links: &mut Vec<Value>, uid: &str, parent: &str| {
        if !parent.is_empty() {
            links.push(build_external_link_row(
                ctx,
                uid,
                parent,
                "parent_event",
                "{}",
            ));
        }
    };

    match top_type {
        "session_meta" => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "session_meta",
                "session_meta",
                "system",
                "",
                &payload_json,
            );
            row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
            events.push(Value::Object(row));
        }
        "turn_context" => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "turn_context",
                "turn_context",
                "system",
                "",
                &payload_json,
            );
            row.insert(
                "turn_index".to_string(),
                json!(to_u32(payload_obj.get("turn_id"))),
            );
            let turn_id = to_str(payload_obj.get("turn_id"));
            if !turn_id.is_empty() {
                row.insert("request_id".to_string(), json!(turn_id.clone()));
                row.insert("item_id".to_string(), json!(turn_id));
            }
            let model = canonicalize_model("codex", &to_str(payload_obj.get("model")));
            if !model.is_empty() {
                row.insert("model".to_string(), json!(model));
            }
            events.push(Value::Object(row));
        }
        "response_item" => {
            let payload_type = to_str(payload_obj.get("type"));
            match payload_type.as_str() {
                "message" => {
                    let role = to_str(payload_obj.get("role"));
                    let content = payload_obj.get("content").cloned().unwrap_or(Value::Null);
                    let text = extract_message_text(&content);
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "message",
                        "message",
                        if role.is_empty() {
                            "assistant"
                        } else {
                            role.as_str()
                        },
                        &text,
                        &payload_json,
                    );
                    row.insert(
                        "content_types".to_string(),
                        json!(extract_content_types(&content)),
                    );
                    row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
                    row.insert(
                        "op_status".to_string(),
                        json!(to_str(payload_obj.get("phase"))),
                    );
                    events.push(Value::Object(row));
                }
                "function_call" => {
                    let args = to_str(payload_obj.get("arguments"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let name = to_str(payload_obj.get("name"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "function_call",
                        "assistant",
                        &args,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(name.clone()));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx, base_uid, &call_id, "", &name, "request", 0, &args, "", "",
                    ));
                }
                "function_call_output" => {
                    let output = to_str(payload_obj.get("output"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_result",
                        "function_call_output",
                        "tool",
                        &output,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        "",
                        "response",
                        0,
                        "",
                        &compact_json(payload_obj.get("output").unwrap_or(&Value::Null)),
                        &output,
                    ));
                }
                "custom_tool_call" => {
                    let input = to_str(payload_obj.get("input"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let name = to_str(payload_obj.get("name"));
                    let status = to_str(payload_obj.get("status"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "custom_tool_call",
                        "assistant",
                        &input,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(name.clone()));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx, base_uid, &call_id, "", &name, "request", 0, &input, "", "",
                    ));
                }
                "custom_tool_call_output" => {
                    let output = to_str(payload_obj.get("output"));
                    let call_id = to_str(payload_obj.get("call_id"));
                    let status = to_str(payload_obj.get("status"));
                    let output_json = serde_json::from_str::<Value>(&output)
                        .map(|parsed| compact_json(&parsed))
                        .unwrap_or_else(|_| {
                            compact_json(payload_obj.get("output").unwrap_or(&Value::Null))
                        });

                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_result",
                        "custom_tool_call_output",
                        "tool",
                        &output,
                        &payload_json,
                    );
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));

                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        "",
                        "response",
                        0,
                        "",
                        &output_json,
                        &output,
                    ));
                }
                "web_search_call" => {
                    let action = payload_obj.get("action").cloned().unwrap_or(Value::Null);
                    let action_type = to_str(action.get("type"));
                    let status = to_str(payload_obj.get("status"));
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "web_search_call",
                        "assistant",
                        &extract_message_text(&action),
                        &payload_json,
                    );
                    row.insert("tool_name".to_string(), json!("web_search"));
                    row.insert("op_kind".to_string(), json!(action_type));
                    row.insert("op_status".to_string(), json!(status.clone()));
                    row.insert("tool_phase".to_string(), json!(status));
                    events.push(Value::Object(row));
                }
                "reasoning" => {
                    let summary = payload_obj.get("summary").cloned().unwrap_or(Value::Null);
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "reasoning",
                        "reasoning",
                        "assistant",
                        &extract_message_text(&summary),
                        &payload_json,
                    );
                    row.insert("has_reasoning".to_string(), json!(1u8));
                    row.insert("item_id".to_string(), json!(to_str(payload_obj.get("id"))));
                    events.push(Value::Object(row));
                }
                _ => {
                    events.push(Value::Object(base_event_obj(
                        ctx,
                        base_uid,
                        "unknown",
                        if payload_type.is_empty() {
                            "response_item"
                        } else {
                            payload_type.as_str()
                        },
                        "system",
                        &extract_message_text(&payload),
                        &payload_json,
                    )));
                }
            }
        }
        "event_msg" => {
            let payload_type = to_str(payload_obj.get("type"));
            let actor = match payload_type.as_str() {
                "user_message" => "user",
                "agent_message" | "agent_reasoning" => "assistant",
                _ => "system",
            };
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "event_msg",
                if payload_type.is_empty() {
                    "event_msg"
                } else {
                    payload_type.as_str()
                },
                actor,
                &extract_message_text(&payload),
                &payload_json,
            );
            let turn_id = to_str(payload_obj.get("turn_id"));
            if !turn_id.is_empty() {
                row.insert("request_id".to_string(), json!(turn_id.clone()));
                row.insert("item_id".to_string(), json!(turn_id));
            }
            let status = to_str(payload_obj.get("status"));
            if !status.is_empty() {
                row.insert("op_status".to_string(), json!(status));
            }
            if payload_type == "token_count" {
                let usage = payload_obj
                    .get("info")
                    .and_then(|v| v.get("last_token_usage"));
                let input_tokens = to_u32(usage.and_then(|v| v.get("input_tokens")));
                let output_tokens = to_u32(usage.and_then(|v| v.get("output_tokens")));
                let cache_read_tokens = to_u32(
                    usage
                        .and_then(|v| v.get("cached_input_tokens"))
                        .or_else(|| usage.and_then(|v| v.get("cache_read_input_tokens"))),
                );
                let cache_write_tokens = to_u32(
                    usage
                        .and_then(|v| v.get("cache_creation_input_tokens"))
                        .or_else(|| usage.and_then(|v| v.get("cache_write_input_tokens"))),
                );

                let model = to_str(
                    payload_obj
                        .get("rate_limits")
                        .and_then(|v| v.get("limit_name")),
                );
                let fallback_model = to_str(payload_obj.get("model"));
                let fallback_limit_id = to_str(
                    payload_obj
                        .get("rate_limits")
                        .and_then(|v| v.get("limit_id")),
                );
                let resolved_model = if !model.is_empty() {
                    canonicalize_model("codex", &model)
                } else if !fallback_model.is_empty() {
                    canonicalize_model("codex", &fallback_model)
                } else if !fallback_limit_id.is_empty() {
                    canonicalize_model("codex", &fallback_limit_id)
                } else {
                    canonicalize_model("codex", model_hint)
                };

                row.insert("input_tokens".to_string(), json!(input_tokens));
                row.insert("output_tokens".to_string(), json!(output_tokens));
                row.insert("cache_read_tokens".to_string(), json!(cache_read_tokens));
                row.insert("cache_write_tokens".to_string(), json!(cache_write_tokens));
                if !resolved_model.is_empty() {
                    row.insert("model".to_string(), json!(resolved_model));
                }
                row.insert(
                    "service_tier".to_string(),
                    json!(to_str(
                        payload_obj
                            .get("rate_limits")
                            .and_then(|v| v.get("plan_type"))
                    )),
                );
                row.insert(
                    "token_usage_json".to_string(),
                    json!(compact_json(&payload)),
                );
            } else if payload_type == "agent_reasoning" {
                row.insert("has_reasoning".to_string(), json!(1u8));
            }
            events.push(Value::Object(row));
        }
        "compacted" => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "compacted_raw",
                "compacted",
                "system",
                "",
                &payload_json,
            )));

            if let Some(Value::Array(items)) = payload_obj.get("replacement_history") {
                for (idx, item) in items.iter().enumerate() {
                    let item_uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &compact_json(item),
                        &format!("compacted:{}", idx),
                    );
                    let item_type = to_str(item.get("type"));

                    let (kind, payload_type, actor, text) = match item_type.as_str() {
                        "message" => (
                            "message",
                            "message",
                            to_str(item.get("role")),
                            extract_message_text(item.get("content").unwrap_or(&Value::Null)),
                        ),
                        "function_call" => (
                            "tool_call",
                            "function_call",
                            "assistant".to_string(),
                            to_str(item.get("arguments")),
                        ),
                        "function_call_output" => (
                            "tool_result",
                            "function_call_output",
                            "tool".to_string(),
                            to_str(item.get("output")),
                        ),
                        "reasoning" => (
                            "reasoning",
                            "reasoning",
                            "assistant".to_string(),
                            extract_message_text(item.get("summary").unwrap_or(&Value::Null)),
                        ),
                        _ => (
                            "unknown",
                            if item_type.is_empty() {
                                "unknown"
                            } else {
                                item_type.as_str()
                            },
                            "system".to_string(),
                            extract_message_text(item),
                        ),
                    };

                    let mut row = base_event_obj(
                        ctx,
                        &item_uid,
                        kind,
                        payload_type,
                        if actor.is_empty() {
                            "assistant"
                        } else {
                            actor.as_str()
                        },
                        &text,
                        &compact_json(item),
                    );
                    row.insert("origin_event_id".to_string(), json!(base_uid));
                    events.push(Value::Object(row));

                    links.push(build_event_link_row(
                        ctx,
                        &item_uid,
                        base_uid,
                        "compacted_parent",
                        "{}",
                    ));
                }
            }
        }
        "message" | "function_call" | "function_call_output" | "reasoning" => {
            let event = if top_type == "message" {
                let role = to_str(record.get("role"));
                let text = extract_message_text(record.get("content").unwrap_or(&Value::Null));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    "message",
                    if role.is_empty() {
                        "assistant"
                    } else {
                        role.as_str()
                    },
                    &text,
                    &compact_json(record),
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(
                        record.get("content").unwrap_or(&Value::Null)
                    )),
                );
                Value::Object(row)
            } else if top_type == "function_call" {
                let args = to_str(record.get("arguments"));
                let call_id = to_str(record.get("call_id"));
                let name = to_str(record.get("name"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "tool_call",
                    "function_call",
                    "assistant",
                    &args,
                    &compact_json(record),
                );
                row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                row.insert("tool_name".to_string(), json!(name.clone()));
                tools.push(build_tool_row(
                    ctx, base_uid, &call_id, "", &name, "request", 0, &args, "", "",
                ));
                Value::Object(row)
            } else if top_type == "function_call_output" {
                let output = to_str(record.get("output"));
                let call_id = to_str(record.get("call_id"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "tool_result",
                    "function_call_output",
                    "tool",
                    &output,
                    &compact_json(record),
                );
                row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                tools.push(build_tool_row(
                    ctx,
                    base_uid,
                    &call_id,
                    "",
                    "",
                    "response",
                    0,
                    "",
                    &compact_json(record.get("output").unwrap_or(&Value::Null)),
                    &output,
                ));
                Value::Object(row)
            } else {
                let summary = record.get("summary").cloned().unwrap_or(Value::Null);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "reasoning",
                    "reasoning",
                    "assistant",
                    &extract_message_text(&summary),
                    &compact_json(record),
                );
                row.insert("has_reasoning".to_string(), json!(1u8));
                Value::Object(row)
            };

            events.push(event);
        }
        _ => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "unknown",
                if top_type.is_empty() {
                    "unknown"
                } else {
                    top_type
                },
                "system",
                &extract_message_text(record),
                &compact_json(record),
            )));
        }
    }

    let payload_model = canonicalize_model("codex", &to_str(payload_obj.get("model")));
    let inherited_model = canonicalize_model("codex", model_hint);
    for event in &mut events {
        if let Some(row) = event.as_object_mut() {
            let row_model = canonicalize_model("codex", &to_str(row.get("model")));
            let resolved_model = if !row_model.is_empty() {
                row_model
            } else if !payload_model.is_empty() {
                payload_model.clone()
            } else {
                inherited_model.clone()
            };

            if !resolved_model.is_empty() {
                row.insert("model".to_string(), json!(resolved_model));
            }
        }
    }

    let parent = to_str(record.get("parent_id"));
    if !events.is_empty() && !parent.is_empty() {
        if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
            push_parent_link(&mut links, uid, &parent);
        }
    }

    (events, links, tools)
}

fn normalize_claude_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let mut links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let parent_uuid = to_str(record.get("parentUuid"));
    let request_id = to_str(record.get("requestId"));
    let trace_id = to_str(record.get("requestId"));
    let agent_run_id = to_str(record.get("agentId"));
    let agent_label = to_str(record.get("agentName"));
    let coord_group_label = to_str(record.get("teamName"));
    let is_substream = to_u8_bool(record.get("isSidechain"));

    let message = record.get("message").cloned().unwrap_or(Value::Null);
    let msg_role = to_str(message.get("role"));
    let model = canonicalize_model("claude-code", &to_str(message.get("model")));

    let usage = message.get("usage").cloned().unwrap_or(Value::Null);
    let input_tokens = to_u32(usage.get("input_tokens"));
    let output_tokens = to_u32(usage.get("output_tokens"));
    let cache_read_tokens = to_u32(usage.get("cache_read_input_tokens"));
    let cache_write_tokens = to_u32(usage.get("cache_creation_input_tokens"));
    let service_tier = to_str(usage.get("service_tier"));

    let stamp_common = |obj: &mut Map<String, Value>| {
        obj.insert("request_id".to_string(), json!(request_id.clone()));
        obj.insert("trace_id".to_string(), json!(trace_id.clone()));
        obj.insert("agent_run_id".to_string(), json!(agent_run_id.clone()));
        obj.insert("agent_label".to_string(), json!(agent_label.clone()));
        obj.insert(
            "coord_group_label".to_string(),
            json!(coord_group_label.clone()),
        );
        obj.insert("is_substream".to_string(), json!(is_substream));
        obj.insert("model".to_string(), json!(model.clone()));
        obj.insert("input_tokens".to_string(), json!(input_tokens));
        obj.insert("output_tokens".to_string(), json!(output_tokens));
        obj.insert("cache_read_tokens".to_string(), json!(cache_read_tokens));
        obj.insert("cache_write_tokens".to_string(), json!(cache_write_tokens));
        obj.insert("service_tier".to_string(), json!(service_tier.clone()));
        obj.insert("item_id".to_string(), json!(to_str(record.get("uuid"))));
        obj.insert(
            "origin_event_id".to_string(),
            json!(to_str(record.get("sourceToolAssistantUUID"))),
        );
        obj.insert(
            "origin_tool_call_id".to_string(),
            json!(to_str(record.get("sourceToolUseID"))),
        );
    };

    if top_type == "assistant" || top_type == "user" {
        let actor = if top_type == "assistant" {
            "assistant"
        } else if msg_role == "assistant" {
            "assistant"
        } else {
            "user"
        };

        let content = message.get("content").cloned().unwrap_or_else(|| {
            record
                .get("message")
                .and_then(|m| m.get("content"))
                .cloned()
                .unwrap_or(Value::Null)
        });

        match content {
            Value::Array(items) if !items.is_empty() => {
                for (idx, item) in items.iter().enumerate() {
                    let block_type = to_str(item.get("type"));
                    let suffix = format!("claude:block:{}", idx);
                    let block_uid = event_uid(
                        ctx.source_file,
                        ctx.source_generation,
                        ctx.source_line_no,
                        ctx.source_offset,
                        &compact_json(item),
                        &suffix,
                    );

                    let mut row = match block_type.as_str() {
                        "thinking" => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "reasoning",
                                "thinking",
                                "assistant",
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            r.insert("has_reasoning".to_string(), json!(1u8));
                            r.insert("content_types".to_string(), json!(["thinking"]));
                            r
                        }
                        "tool_use" => {
                            let tool_call_id = to_str(item.get("id"));
                            let tool_name = to_str(item.get("name"));
                            let input_json =
                                compact_json(item.get("input").unwrap_or(&Value::Null));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_call",
                                "tool_use",
                                "assistant",
                                &extract_message_text(item.get("input").unwrap_or(&Value::Null)),
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_use"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_name".to_string(), json!(tool_name.clone()));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                &tool_name,
                                "request",
                                0,
                                &input_json,
                                "",
                                "",
                            ));
                            r
                        }
                        "tool_result" => {
                            let tool_call_id = to_str(item.get("tool_use_id"));
                            let output_json =
                                compact_json(item.get("content").unwrap_or(&Value::Null));
                            let output_text =
                                extract_message_text(item.get("content").unwrap_or(&Value::Null));
                            let tool_error = to_u8_bool(item.get("is_error"));
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "tool_result",
                                "tool_result",
                                "tool",
                                &output_text,
                                &compact_json(item),
                            );
                            r.insert("content_types".to_string(), json!(["tool_result"]));
                            r.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
                            r.insert("tool_error".to_string(), json!(tool_error));
                            tools.push(build_tool_row(
                                ctx,
                                &block_uid,
                                &tool_call_id,
                                &to_str(record.get("parentToolUseID")),
                                "",
                                "response",
                                tool_error,
                                "",
                                &output_json,
                                &output_text,
                            ));
                            r
                        }
                        _ => {
                            let mut r = base_event_obj(
                                ctx,
                                &block_uid,
                                "message",
                                if block_type.is_empty() {
                                    "text"
                                } else {
                                    block_type.as_str()
                                },
                                actor,
                                &extract_message_text(item),
                                &compact_json(item),
                            );
                            if !block_type.is_empty() {
                                r.insert("content_types".to_string(), json!([block_type]));
                            }
                            r
                        }
                    };

                    stamp_common(&mut row);
                    row.insert(
                        "parent_tool_call_id".to_string(),
                        json!(to_str(record.get("parentToolUseID"))),
                    );
                    row.insert(
                        "origin_tool_call_id".to_string(),
                        json!(to_str(record.get("sourceToolUseID"))),
                    );
                    row.insert(
                        "tool_phase".to_string(),
                        json!(to_str(record.get("stop_reason"))),
                    );
                    events.push(Value::Object(row));

                    if !parent_uuid.is_empty() {
                        links.push(build_external_link_row(
                            ctx,
                            &block_uid,
                            &parent_uuid,
                            "parent_uuid",
                            "{}",
                        ));
                    }
                }
            }
            _ => {
                let text = extract_message_text(&message);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    "message",
                    actor,
                    &text,
                    &compact_json(record),
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(
                        message.get("content").unwrap_or(&Value::Null)
                    )),
                );
                stamp_common(&mut row);
                events.push(Value::Object(row));
                if !parent_uuid.is_empty() {
                    links.push(build_external_link_row(
                        ctx,
                        base_uid,
                        &parent_uuid,
                        "parent_uuid",
                        "{}",
                    ));
                }
            }
        }
    } else {
        let event_kind = match top_type {
            "progress" => "progress",
            "system" => "system",
            "summary" => "summary",
            "queue-operation" => "queue_operation",
            "file-history-snapshot" => "file_history_snapshot",
            _ => "unknown",
        };

        let payload_type = if top_type == "progress" {
            to_str(record.get("data").and_then(|d| d.get("type")))
        } else if top_type == "system" {
            to_str(record.get("subtype"))
        } else {
            top_type.to_string()
        };

        let mut row = base_event_obj(
            ctx,
            base_uid,
            event_kind,
            if payload_type.is_empty() {
                top_type
            } else {
                payload_type.as_str()
            },
            "system",
            &extract_message_text(record),
            &compact_json(record),
        );
        row.insert("op_kind".to_string(), json!(payload_type));
        row.insert("op_status".to_string(), json!(to_str(record.get("status"))));
        row.insert(
            "latency_ms".to_string(),
            json!(to_u32(record.get("durationMs"))),
        );
        row.insert(
            "retry_count".to_string(),
            json!(to_u16(record.get("retryAttempt"))),
        );
        stamp_common(&mut row);
        events.push(Value::Object(row));

        if !parent_uuid.is_empty() {
            links.push(build_external_link_row(
                ctx,
                base_uid,
                &parent_uuid,
                "parent_uuid",
                "{}",
            ));
        }
    }

    if !events.is_empty() {
        let tool_use_id = to_str(record.get("toolUseID"));
        if !tool_use_id.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &tool_use_id,
                    "tool_use_id",
                    "{}",
                ));
            }
        }

        let source_tool_assistant = to_str(record.get("sourceToolAssistantUUID"));
        if !source_tool_assistant.is_empty() {
            if let Some(uid) = events[0].get("event_uid").and_then(|v| v.as_str()) {
                links.push(build_external_link_row(
                    ctx,
                    uid,
                    &source_tool_assistant,
                    "source_tool_assistant",
                    "{}",
                ));
            }
        }
    }

    (events, links, tools)
}

fn kimi_cli_event_uid(ctx: &RecordContext<'_>, record_fingerprint: &str, suffix: &str) -> String {
    event_uid(
        ctx.source_file,
        ctx.source_generation,
        ctx.source_line_no,
        ctx.source_offset,
        record_fingerprint,
        &format!("kimi-cli:{suffix}"),
    )
}

fn normalize_kimi_cli_context_record(
    record: &Value,
    ctx: &RecordContext<'_>,
    _base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    let role = to_str(record.get("role"));
    let content = record.get("content").unwrap_or(&Value::Null);
    let record_json = compact_json(record);

    match role.as_str() {
        "_system_prompt" => {
            let text = to_str(record.get("content"));
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:system_prompt");
            events.push(Value::Object(base_event_obj(
                ctx,
                &uid,
                "system",
                "system",
                "system",
                &text,
                &record_json,
            )));
        }
        "_usage" => {
            let token_count = to_u32(record.get("token_count"));
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:usage");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "event_msg",
                "token_count",
                "system",
                "",
                &record_json,
            );
            row.insert("input_tokens".to_string(), json!(token_count));
            row.insert("token_usage_json".to_string(), json!(record_json.clone()));
            events.push(Value::Object(row));
        }
        "_checkpoint" => {
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:checkpoint");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "progress",
                "progress",
                "system",
                "",
                &record_json,
            );
            row.insert("item_id".to_string(), json!(to_str(record.get("id"))));
            row.insert("op_kind".to_string(), json!("checkpoint"));
            events.push(Value::Object(row));
        }
        "user" => {
            let text = extract_message_text(content);
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:user");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "message",
                "user_message",
                "user",
                &text,
                &record_json,
            );
            row.insert(
                "content_types".to_string(),
                json!(extract_content_types(content)),
            );
            events.push(Value::Object(row));
        }
        "assistant" => {
            if let Value::Array(parts) = content {
                for (idx, part) in parts.iter().enumerate() {
                    let part_type = to_str(part.get("type"));
                    let part_text = extract_message_text(part);
                    let part_json = compact_json(part);
                    let suffix = format!("context:assistant:{idx}:{part_type}");
                    let part_uid = kimi_cli_event_uid(ctx, &part_json, &suffix);

                    let row = match part_type.as_str() {
                        "think" => {
                            let mut r = base_event_obj(
                                ctx,
                                &part_uid,
                                "reasoning",
                                "thinking",
                                "assistant",
                                &part_text,
                                &part_json,
                            );
                            r.insert("has_reasoning".to_string(), json!(1u8));
                            r.insert("content_types".to_string(), json!(["thinking"]));
                            r
                        }
                        _ => {
                            let mut r = base_event_obj(
                                ctx,
                                &part_uid,
                                "message",
                                "agent_message",
                                "assistant",
                                &part_text,
                                &part_json,
                            );
                            if !part_type.is_empty() {
                                r.insert("content_types".to_string(), json!([part_type]));
                            }
                            r
                        }
                    };
                    events.push(Value::Object(row));
                }
            } else {
                let text = extract_message_text(content);
                let uid = kimi_cli_event_uid(ctx, &record_json, "context:assistant");
                let mut row = base_event_obj(
                    ctx,
                    &uid,
                    "message",
                    "agent_message",
                    "assistant",
                    &text,
                    &record_json,
                );
                row.insert(
                    "content_types".to_string(),
                    json!(extract_content_types(content)),
                );
                events.push(Value::Object(row));
            }
        }
        "tool" => {
            let tool_call_id = to_str(record.get("tool_call_id"));
            let text = extract_message_text(content);
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:tool");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "tool_result",
                "tool_result",
                "tool",
                &text,
                &record_json,
            );
            row.insert("content_types".to_string(), json!(["tool_result"]));
            row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
            events.push(Value::Object(row));

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                "",
                "response",
                0,
                "",
                &compact_json(content),
                &text,
            ));
        }
        _ => {
            let text = extract_message_text(content);
            let uid = kimi_cli_event_uid(ctx, &record_json, "context:unknown");
            events.push(Value::Object(base_event_obj(
                ctx,
                &uid,
                "unknown",
                "unknown",
                "system",
                &text,
                &record_json,
            )));
        }
    }

    (events, links, tools)
}

fn normalize_kimi_cli_wire_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    _base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();

    if record.get("type").and_then(Value::as_str) == Some("metadata") {
        let record_json = compact_json(record);
        let uid = kimi_cli_event_uid(ctx, &record_json, "wire:metadata");
        let mut row = base_event_obj(
            ctx,
            &uid,
            "session_meta",
            "session_meta",
            "system",
            "",
            &record_json,
        );
        if let Some(pv) = record.get("protocol_version").and_then(Value::as_str) {
            row.insert("item_id".to_string(), json!(pv));
        }
        events.push(Value::Object(row));
        return (events, links, tools);
    }

    let message = record.get("message").unwrap_or(record);
    let msg_type = {
        let message_type = to_str(message.get("type"));
        if message_type.is_empty() {
            top_type.to_string()
        } else {
            message_type
        }
    };
    let payload = message.get("payload").unwrap_or(record);
    let payload_json = compact_json(payload);

    let mut push_progress = |suffix: &str, kind: &str, text: String, payload_type: &str| {
        let uid = kimi_cli_event_uid(ctx, &payload_json, suffix);
        let mut row = base_event_obj(
            ctx,
            &uid,
            kind,
            payload_type,
            "system",
            &text,
            &payload_json,
        );
        row.insert("op_kind".to_string(), json!(msg_type));
        events.push(Value::Object(row));
    };

    match msg_type.as_str() {
        "TurnBegin" | "SteerInput" => {
            let input = payload.get("user_input").unwrap_or(&Value::Null);
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:user_input");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "message",
                "user_message",
                "user",
                &extract_message_text(input),
                &payload_json,
            );
            row.insert(
                "content_types".to_string(),
                json!(extract_content_types(input)),
            );
            events.push(Value::Object(row));
        }
        "TurnEnd" => {
            push_progress("wire:turn_end", "summary", String::new(), "summary");
        }
        "StepBegin" => {
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:step_begin");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "progress",
                "progress",
                "system",
                "",
                &payload_json,
            );
            row.insert("turn_index".to_string(), json!(to_u32(payload.get("n"))));
            row.insert("op_kind".to_string(), json!("step_begin"));
            events.push(Value::Object(row));
        }
        "StepInterrupted" => {
            push_progress(
                "wire:step_interrupted",
                "progress",
                extract_message_text(payload),
                "progress",
            );
        }
        "ContentPart" => {
            let part_type = to_str(payload.get("type"));
            match part_type.as_str() {
                "think" => {
                    let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:content:think");
                    let text = to_str(payload.get("think"));
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        "reasoning",
                        "thinking",
                        "assistant",
                        &text,
                        &payload_json,
                    );
                    row.insert("has_reasoning".to_string(), json!(1u8));
                    row.insert("content_types".to_string(), json!(["thinking"]));
                    events.push(Value::Object(row));
                }
                _ => {
                    let text = if let Some(text) = payload.get("text") {
                        extract_message_text(text)
                    } else {
                        extract_message_text(payload)
                    };
                    let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:content:text");
                    let mut row = base_event_obj(
                        ctx,
                        &uid,
                        "message",
                        "agent_message",
                        "assistant",
                        &text,
                        &payload_json,
                    );
                    if !part_type.is_empty() {
                        row.insert("content_types".to_string(), json!([part_type]));
                    }
                    events.push(Value::Object(row));
                }
            }
        }
        "ToolCall" => {
            let function = payload.get("function").unwrap_or(&Value::Null);
            let tool_name = to_str(function.get("name"));
            let arguments = to_str(function.get("arguments"));
            let tool_call_id = to_str(payload.get("id"));
            let args = parse_json_string(&arguments).unwrap_or_else(|| {
                if arguments.is_empty() {
                    Value::Object(Map::new())
                } else {
                    json!({ "raw": arguments })
                }
            });
            let input_json = compact_json(&args);
            let input_text = {
                let extracted = extract_message_text(&args);
                if extracted.is_empty() {
                    input_json.clone()
                } else {
                    extracted
                }
            };

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_call");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "tool_call",
                "tool_use",
                "assistant",
                &input_text,
                &payload_json,
            );
            row.insert("content_types".to_string(), json!(["tool_use"]));
            row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
            row.insert("tool_name".to_string(), json!(tool_name.clone()));
            events.push(Value::Object(row));

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                &tool_name,
                "request",
                0,
                &input_json,
                "",
                "",
            ));
        }
        "ToolCallPart" => {
            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_call_part");
            let args_part = to_str(payload.get("arguments_part"));
            let mut row = base_event_obj(
                ctx,
                &uid,
                "progress",
                "tool_use",
                "assistant",
                &args_part,
                &payload_json,
            );
            row.insert("op_kind".to_string(), json!("tool_call_part"));
            events.push(Value::Object(row));
        }
        "ToolResult" => {
            let tool_call_id = to_str(payload.get("tool_call_id"));
            let return_value = payload.get("return_value").unwrap_or(&Value::Null);
            let is_error = to_u8_bool(return_value.get("is_error"));
            let output = to_str(return_value.get("output"));
            let message_text = to_str(return_value.get("message"));

            let output_text = if !output.is_empty() {
                output.clone()
            } else {
                message_text.clone()
            };
            let output_json = compact_json(return_value);

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:tool_result");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "tool_result",
                "tool_result",
                "tool",
                &output_text,
                &payload_json,
            );
            row.insert("content_types".to_string(), json!(["tool_result"]));
            row.insert("tool_call_id".to_string(), json!(tool_call_id.clone()));
            row.insert("tool_error".to_string(), json!(is_error));
            events.push(Value::Object(row));

            tools.push(build_tool_row(
                ctx,
                &uid,
                &tool_call_id,
                "",
                "",
                "response",
                is_error,
                "",
                &output_json,
                &output_text,
            ));
        }
        "StatusUpdate" => {
            let token_usage = payload.get("token_usage").unwrap_or(&Value::Null);
            let input_other = to_u32(token_usage.get("input_other"));
            let input_cache_read = to_u32(token_usage.get("input_cache_read"));
            let input_cache_creation = to_u32(token_usage.get("input_cache_creation"));
            let input_tokens = input_other
                .saturating_add(input_cache_read)
                .saturating_add(input_cache_creation);
            let output = to_u32(token_usage.get("output"));

            let uid = kimi_cli_event_uid(ctx, &payload_json, "wire:status_update");
            let mut row = base_event_obj(
                ctx,
                &uid,
                "event_msg",
                "token_count",
                "system",
                "",
                &payload_json,
            );
            row.insert("input_tokens".to_string(), json!(input_tokens));
            row.insert("output_tokens".to_string(), json!(output));
            row.insert("cache_read_tokens".to_string(), json!(input_cache_read));
            row.insert(
                "cache_write_tokens".to_string(),
                json!(input_cache_creation),
            );
            row.insert(
                "token_usage_json".to_string(),
                json!(compact_json(token_usage)),
            );
            row.insert(
                "item_id".to_string(),
                json!(to_str(payload.get("message_id"))),
            );
            events.push(Value::Object(row));
        }
        "CompactionBegin" | "CompactionEnd" => {
            push_progress("wire:compaction", "summary", String::new(), "summary");
        }
        "HookTriggered" | "HookResolved" => {
            let event = to_str(payload.get("event"));
            let target = to_str(payload.get("target"));
            let text = if !event.is_empty() && !target.is_empty() {
                format!("{event}: {target}")
            } else {
                event
            };
            push_progress("wire:hook", "event_msg", text, "event_msg");
        }
        "SubagentEvent" => {
            // Parent wires duplicate these events; the sub-agent wire carries
            // the real record. Keep the raw row, but avoid double-counting a
            // synthetic parent progress row.
        }
        "MCPLoadingBegin" | "MCPLoadingEnd" | "MCPStatusSnapshot" | "BtwBegin" | "BtwEnd"
        | "Notification" | "PlanDisplay" | "ApprovalRequest" | "ApprovalResponse"
        | "QuestionRequest" | "QuestionResponse" => {
            push_progress(
                &format!("wire:{msg_type}"),
                "progress",
                extract_message_text(payload),
                "progress",
            );
        }
        _ => {
            push_progress(
                "wire:unknown",
                "unknown",
                extract_message_text(payload),
                "unknown",
            );
        }
    }

    (events, links, tools)
}

fn normalize_kimi_cli_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let (mut events, links, tools) = if record.get("role").is_some() {
        normalize_kimi_cli_context_record(record, ctx, base_uid)
    } else {
        normalize_kimi_cli_wire_event(record, ctx, top_type, base_uid)
    };

    // The Kimi wire/context schemas do not expose the active model. Use the
    // harness slug so Kimi rows remain visible in model-based token analytics.
    for row in events.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            obj.insert("model".to_string(), json!("kimi-cli"));
        }
    }

    (events, links, tools)
}

fn normalize_opencode_event(
    record: &Value,
    ctx: &RecordContext<'_>,
    top_type: &str,
    base_uid: &str,
    model: &str,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut events = Vec::<Value>::new();
    let links = Vec::<Value>::new();
    let mut tools = Vec::<Value>::new();
    let payload_json = compact_json(record);
    let message = record.get("message").unwrap_or(&Value::Null);
    let part = record.get("part").unwrap_or(&Value::Null);
    let role = to_str(message.get("role"));
    let actor = if role == "user" { "user" } else { "assistant" };
    let stamp_model = |row: &mut Map<String, Value>| {
        if !model.is_empty() {
            row.insert("model".to_string(), json!(model));
        }
        row.insert("item_id".to_string(), json!(to_str(record.get("row_id"))));
        row.insert(
            "agent_label".to_string(),
            json!(to_str(message.get("agent"))),
        );
        row.insert("op_kind".to_string(), json!(to_str(part.get("type"))));
    };

    match top_type {
        "opencode_session" => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "session_meta",
                "session_meta",
                "system",
                &to_str(record.get("title")),
                &payload_json,
            )));
        }
        "opencode_message" => {
            let mut row = base_event_obj(
                ctx,
                base_uid,
                "progress",
                "progress",
                actor,
                &extract_message_text(message),
                &payload_json,
            );
            stamp_model(&mut row);
            events.push(Value::Object(row));
        }
        "opencode_part" => match to_str(part.get("type")).as_str() {
            "text" => {
                let text = to_str(part.get("text"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "message",
                    if actor == "user" {
                        "user_message"
                    } else {
                        "agent_message"
                    },
                    actor,
                    &text,
                    &compact_json(part),
                );
                stamp_model(&mut row);
                events.push(Value::Object(row));
            }
            "reasoning" => {
                let text = to_str(part.get("text"));
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "reasoning",
                    "thinking",
                    "assistant",
                    &text,
                    &compact_json(part),
                );
                stamp_model(&mut row);
                row.insert("has_reasoning".to_string(), json!(1u8));
                events.push(Value::Object(row));
            }
            "tool" => {
                let state = part.get("state").unwrap_or(&Value::Null);
                let status = to_str(state.get("status"));
                let call_id = to_str(part.get("callID"));
                let tool_name = to_str(part.get("tool"));
                let input = state.get("input").cloned().unwrap_or(Value::Null);
                let input_json = compact_json(&input);
                let input_text = extract_message_text(&input);
                if status == "pending" || status == "running" {
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_call",
                        "tool_use",
                        "assistant",
                        if input_text.is_empty() {
                            &input_json
                        } else {
                            &input_text
                        },
                        &compact_json(part),
                    );
                    stamp_model(&mut row);
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(tool_name.clone()));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));
                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        &tool_name,
                        "request",
                        0,
                        &input_json,
                        "",
                        "",
                    ));
                } else {
                    let output_text = if status == "error" {
                        to_str(state.get("error"))
                    } else {
                        to_str(state.get("output"))
                    };
                    let error = u8::from(status == "error");
                    let mut row = base_event_obj(
                        ctx,
                        base_uid,
                        "tool_result",
                        "tool_result",
                        "tool",
                        &output_text,
                        &compact_json(part),
                    );
                    stamp_model(&mut row);
                    row.insert("tool_call_id".to_string(), json!(call_id.clone()));
                    row.insert("tool_name".to_string(), json!(tool_name.clone()));
                    row.insert("tool_error".to_string(), json!(error));
                    row.insert("op_status".to_string(), json!(status));
                    events.push(Value::Object(row));
                    tools.push(build_tool_row(
                        ctx,
                        base_uid,
                        &call_id,
                        "",
                        &tool_name,
                        "response",
                        error,
                        "",
                        &compact_json(state),
                        &output_text,
                    ));
                }
            }
            "step-finish" => {
                let tokens = part.get("tokens").unwrap_or(&Value::Null);
                let cache = tokens.get("cache").unwrap_or(&Value::Null);
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "progress",
                    "progress",
                    "system",
                    &to_str(part.get("reason")),
                    &compact_json(part),
                );
                stamp_model(&mut row);
                row.insert("op_status".to_string(), json!(to_str(part.get("reason"))));
                row.insert(
                    "input_tokens".to_string(),
                    json!(to_u32(tokens.get("input"))),
                );
                row.insert(
                    "output_tokens".to_string(),
                    json!(to_u32(tokens.get("output"))),
                );
                row.insert(
                    "cache_read_tokens".to_string(),
                    json!(to_u32(cache.get("read"))),
                );
                row.insert(
                    "cache_write_tokens".to_string(),
                    json!(to_u32(cache.get("write"))),
                );
                row.insert("token_usage_json".to_string(), json!(compact_json(tokens)));
                events.push(Value::Object(row));
            }
            "patch" | "file" | "snapshot" => {
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "file_history_snapshot",
                    "file-history-snapshot",
                    "system",
                    &extract_message_text(part),
                    &compact_json(part),
                );
                stamp_model(&mut row);
                events.push(Value::Object(row));
            }
            "compaction" => {
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "summary",
                    "compacted",
                    "system",
                    "",
                    &compact_json(part),
                );
                stamp_model(&mut row);
                events.push(Value::Object(row));
            }
            _ => {
                let mut row = base_event_obj(
                    ctx,
                    base_uid,
                    "progress",
                    "progress",
                    "system",
                    &extract_message_text(part),
                    &compact_json(part),
                );
                stamp_model(&mut row);
                events.push(Value::Object(row));
            }
        },
        _ => {
            events.push(Value::Object(base_event_obj(
                ctx,
                base_uid,
                "unknown",
                "unknown",
                "system",
                &extract_message_text(record),
                &payload_json,
            )));
        }
    }

    (events, links, tools)
}

pub fn normalize_record(
    record: &Value,
    source_name: &str,
    harness: &str,
    source_file: &str,
    source_inode: u64,
    source_generation: u32,
    source_line_no: u64,
    source_offset: u64,
    session_hint: &str,
    model_hint: &str,
) -> Result<NormalizedRecord> {
    let harness = Harness::parse(harness)?;
    let harness_name = harness.as_str();

    // For most harnesses `inference_provider` is a static property of the
    // harness. Hermes is different: the vendor is encoded inside the record's
    // `model` field as `vendor/model`, so we parse it here and use the parsed
    // value throughout the context.
    let (inference_provider, hermes_model, opencode_model, factory_droid_model) =
        if harness == Harness::Hermes {
            let (vendor, model) = split_hermes_vendor_model(&to_str(record.get("model")));
            (vendor, model, String::new(), String::new())
        } else if harness == Harness::OpenCode {
            let (provider, model) = opencode_provider_model(record);
            (provider, String::new(), model, String::new())
        } else if harness == Harness::FactoryDroid {
            let (provider, model) = factory_droid_provider_model(record, model_hint);
            (provider, String::new(), String::new(), model)
        } else {
            (
                harness.inference_provider().to_string(),
                String::new(),
                String::new(),
                String::new(),
            )
        };

    let (record_ts, event_ts, event_ts_parse_failed) = if harness == Harness::KimiCli {
        parse_kimi_timestamp(record, source_line_no)
    } else {
        let record_ts = to_str(record.get("timestamp"));
        let (event_ts, event_ts_parse_failed) = parse_event_ts(&record_ts);
        (record_ts, event_ts, event_ts_parse_failed)
    };
    let top_type = if harness == Harness::Hermes {
        let explicit = to_str(record.get("type"));
        if explicit.is_empty() {
            "trajectory".to_string()
        } else {
            explicit
        }
    } else if harness == Harness::KimiCli {
        let message_type = to_str(record.get("message").and_then(|v| v.get("type")));
        if !message_type.is_empty() {
            message_type
        } else {
            let explicit = to_str(record.get("type"));
            if !explicit.is_empty() {
                explicit
            } else {
                to_str(record.get("role"))
            }
        }
    } else {
        to_str(record.get("type"))
    };

    let mut session_id = if harness == Harness::ClaudeCode {
        to_str(record.get("sessionId"))
    } else if harness == Harness::FactoryDroid {
        factory_droid_session_id(source_file, session_hint, &top_type, record)
    } else if harness == Harness::KimiCli {
        kimi_session_id(source_file, session_hint)
    } else {
        String::new()
    };
    if harness == Harness::OpenCode {
        let explicit = to_str(record.get("session_id"));
        if !explicit.is_empty() {
            session_id = format!("opencode:{explicit}");
        }
    }
    if session_id.is_empty()
        && harness != Harness::Hermes
        && harness != Harness::KimiCli
        && harness != Harness::FactoryDroid
    {
        session_id = if session_hint.is_empty() {
            infer_session_id_from_file(source_file)
        } else {
            session_hint.to_string()
        };
    }

    if harness == Harness::Codex && top_type == "session_meta" {
        let payload = record.get("payload").cloned().unwrap_or(Value::Null);
        let payload_id = to_str(payload.get("id"));
        if !payload_id.is_empty() {
            session_id = payload_id;
        }
    }

    let session_date = infer_session_date_from_file(source_file, &record_ts);

    let raw_json = compact_json(record);
    let stored_raw_json = truncate_chars(&raw_json, RAW_JSON_LIMIT);
    let effective_source_offset = if harness == Harness::OpenCode {
        0
    } else {
        source_offset
    };
    let record_fingerprint = if harness == Harness::OpenCode {
        let row_id = to_str(record.get("row_id"));
        if row_id.is_empty() {
            raw_json.clone()
        } else {
            format!("{top_type}:{row_id}")
        }
    } else {
        raw_json.clone()
    };
    let base_uid = event_uid(
        source_file,
        source_generation,
        source_line_no,
        effective_source_offset,
        &record_fingerprint,
        "raw",
    );
    if harness == Harness::Hermes {
        let explicit_session_id = to_str(record.get("session_id"));
        session_id = if explicit_session_id.is_empty() {
            hermes_session_id(&base_uid)
        } else {
            format!("hermes:{}", explicit_session_id)
        };
    }

    let raw_row = json!({
        "source_name": source_name,
        "harness": harness_name,
        "inference_provider": inference_provider,
        "source_file": source_file,
        "source_inode": source_inode,
        "source_generation": source_generation,
        "source_line_no": source_line_no,
        "source_offset": effective_source_offset,
        "record_ts": record_ts,
        "top_type": top_type,
        "session_id": session_id,
        "raw_json": stored_raw_json,
        "raw_json_hash": raw_hash(&raw_json),
        "event_uid": base_uid,
    });

    let mut error_rows = Vec::<Value>::new();
    if event_ts_parse_failed {
        error_rows.push(json!({
            "source_name": source_name,
            "harness": harness_name,
            "inference_provider": inference_provider,
            "source_file": source_file,
            "source_inode": source_inode,
            "source_generation": source_generation,
            "source_line_no": source_line_no,
            "source_offset": effective_source_offset,
            "error_kind": "timestamp_parse_error",
            "error_text": format!(
                "timestamp is missing or not supported ISO8601/RFC3339; used {} UTC fallback",
                UNPARSEABLE_EVENT_TS
            ),
            "raw_fragment": truncate_chars(&record_ts, 20_000),
        }));
    }

    let ctx = RecordContext {
        source_name,
        harness: harness_name,
        inference_provider: &inference_provider,
        session_id: &session_id,
        session_date: &session_date,
        source_file,
        source_inode,
        source_generation,
        source_line_no,
        source_offset: effective_source_offset,
        record_ts: &record_ts,
        event_ts: &event_ts,
    };

    let (event_rows, link_rows, tool_rows) = match harness {
        Harness::ClaudeCode => normalize_claude_event(record, &ctx, &top_type, &base_uid),
        Harness::Hermes => match top_type.as_str() {
            "session_meta" => normalize_hermes_session_meta(record, &ctx, &base_uid),
            "session_message" => {
                normalize_hermes_session_message(record, &ctx, &base_uid, &hermes_model)
            }
            _ => normalize_hermes_trajectory(record, &ctx, &base_uid, &hermes_model),
        },
        Harness::Codex => normalize_codex_event(record, &ctx, &top_type, &base_uid, model_hint),
        Harness::FactoryDroid => {
            normalize_factory_droid_event(record, &ctx, &top_type, &base_uid, &factory_droid_model)
        }
        Harness::KimiCli => normalize_kimi_cli_event(record, &ctx, &top_type, &base_uid),
        Harness::OpenCode => {
            normalize_opencode_event(record, &ctx, &top_type, &base_uid, &opencode_model)
        }
    };

    // For Hermes, resolve_model_hint's fallback should be the already-split
    // model (the part after the vendor slash) so downstream checkpoints store
    // the verbatim model rather than the combined `vendor/model` string.
    let hint_fallback = if harness == Harness::Hermes {
        hermes_model.as_str()
    } else if harness == Harness::OpenCode {
        opencode_model.as_str()
    } else if harness == Harness::FactoryDroid {
        factory_droid_model.as_str()
    } else {
        model_hint
    };
    let model_hint = resolve_model_hint(&event_rows, harness_name, hint_fallback);

    Ok(NormalizedRecord {
        raw_row,
        event_rows,
        link_rows,
        tool_rows,
        error_rows,
        session_hint: session_id,
        model_hint,
    })
}

fn map_redaction_mode(mode: moraine_config::RedactionMode) -> moraine_privacy::RedactionMode {
    match mode {
        moraine_config::RedactionMode::StoreRaw => moraine_privacy::RedactionMode::StoreRaw,
        moraine_config::RedactionMode::HashRaw => moraine_privacy::RedactionMode::HashRaw,
        moraine_config::RedactionMode::RedactRaw => moraine_privacy::RedactionMode::RedactRaw,
        moraine_config::RedactionMode::DropRaw => moraine_privacy::RedactionMode::DropRaw,
        moraine_config::RedactionMode::EncryptRaw => moraine_privacy::RedactionMode::EncryptRaw,
    }
}

fn mode_requires_encryption(mode: moraine_config::RedactionMode) -> bool {
    mode == moraine_config::RedactionMode::EncryptRaw
}

fn resolve_privacy_encryption_key(
    privacy: &moraine_config::PrivacyConfig,
) -> Result<Option<moraine_privacy::EncryptionKey>> {
    if !mode_requires_encryption(privacy.raw_events_mode)
        && !mode_requires_encryption(privacy.text_content_mode)
        && !mode_requires_encryption(privacy.payload_json_mode)
        && !mode_requires_encryption(privacy.tool_io_mode)
    {
        return Ok(None);
    }

    let key_id = privacy.encryption_key_id.trim();
    if key_id.is_empty() {
        return Err(anyhow!(
            "privacy encrypt_raw requires privacy.encryption_key_id"
        ));
    }

    let env_name = privacy.encryption_key_env.trim();
    if !env_name.is_empty() {
        if let Ok(value) = std::env::var(env_name) {
            return moraine_privacy::EncryptionKey::from_material(key_id, value.as_bytes())
                .map(Some)
                .map_err(|err| anyhow!("invalid privacy encryption key from ${env_name}: {err}"));
        }
    }

    let key_file = privacy.encryption_key_file.trim();
    if !key_file.is_empty() {
        return moraine_privacy::EncryptionKey::from_file(key_id, std::path::Path::new(key_file))
            .map(Some)
            .map_err(|err| anyhow!("invalid privacy encryption key file {key_file}: {err}"));
    }

    Err(anyhow!(
        "privacy encrypt_raw requires privacy.encryption_key_env or privacy.encryption_key_file"
    ))
}

#[derive(Default)]
struct PrivacyRowStats {
    count: u64,
    kinds: BTreeSet<String>,
    key_id: String,
}

impl PrivacyRowStats {
    fn add(
        &mut self,
        result: &moraine_privacy::RedactionResult,
        mode: moraine_config::RedactionMode,
        encryption_key: Option<&moraine_privacy::EncryptionKey>,
    ) {
        if !result.was_redacted {
            return;
        }
        self.count = self.count.saturating_add(result.count as u64);
        for kind in &result.kinds {
            self.kinds.insert(kind.clone());
        }
        if mode_requires_encryption(mode) {
            if let Some(key) = encryption_key {
                self.key_id = key.key_id.clone();
            }
        }
    }
}

fn init_privacy_metadata(row: &mut Value, policy_version: &str) {
    let Value::Object(map) = row else {
        return;
    };
    map.insert(
        "privacy_policy_version".to_string(),
        Value::String(policy_version.to_string()),
    );
    map.insert("privacy_redaction_applied".to_string(), json!(0_u8));
    map.insert("privacy_redaction_count".to_string(), json!(0_u64));
    map.insert(
        "privacy_redaction_kinds".to_string(),
        Value::Array(Vec::new()),
    );
    map.insert("privacy_key_id".to_string(), Value::String(String::new()));
}

fn finish_privacy_metadata(row: &mut Value, stats: PrivacyRowStats) {
    let Value::Object(map) = row else {
        return;
    };
    map.insert(
        "privacy_redaction_applied".to_string(),
        json!(u8::from(stats.count > 0)),
    );
    map.insert("privacy_redaction_count".to_string(), json!(stats.count));
    map.insert(
        "privacy_redaction_kinds".to_string(),
        Value::Array(stats.kinds.into_iter().map(Value::String).collect()),
    );
    map.insert("privacy_key_id".to_string(), Value::String(stats.key_id));
}

fn apply_privacy_to_string_field(
    row: &mut Value,
    field: &str,
    mode: moraine_config::RedactionMode,
    detectors: &[moraine_privacy::RegexDetector],
    encryption_key: Option<&moraine_privacy::EncryptionKey>,
    stats: &mut PrivacyRowStats,
) -> Result<()> {
    if mode == moraine_config::RedactionMode::StoreRaw {
        return Ok(());
    }

    if let Some(Value::String(text)) = row.get_mut(field) {
        let result =
            moraine_privacy::redact_text(text, map_redaction_mode(mode), detectors, encryption_key)
                .map_err(|err| anyhow!("privacy redaction failed for {field}: {err}"))?;
        if result.was_redacted {
            *text = result.text.clone();
        }
        stats.add(&result, mode, encryption_key);
    }
    Ok(())
}

/// Apply privacy redaction to a normalized record according to config.
pub fn apply_privacy_redaction(
    record: &mut NormalizedRecord,
    privacy: &moraine_config::PrivacyConfig,
) -> Result<()> {
    if !privacy.enabled {
        return Ok(());
    }

    let detectors = moraine_privacy::BuiltinDetectors::all();
    let encryption_key = resolve_privacy_encryption_key(privacy)?;
    let encryption_key_ref = encryption_key.as_ref();

    init_privacy_metadata(&mut record.raw_row, &privacy.redaction_policy_version);
    let mut raw_stats = PrivacyRowStats::default();
    apply_privacy_to_string_field(
        &mut record.raw_row,
        "raw_json",
        privacy.raw_events_mode,
        &detectors,
        encryption_key_ref,
        &mut raw_stats,
    )?;
    finish_privacy_metadata(&mut record.raw_row, raw_stats);

    for event in &mut record.event_rows {
        init_privacy_metadata(event, &privacy.redaction_policy_version);
        let mut stats = PrivacyRowStats::default();
        apply_privacy_to_string_field(
            event,
            "text_content",
            privacy.text_content_mode,
            &detectors,
            encryption_key_ref,
            &mut stats,
        )?;
        apply_privacy_to_string_field(
            event,
            "payload_json",
            privacy.payload_json_mode,
            &detectors,
            encryption_key_ref,
            &mut stats,
        )?;
        finish_privacy_metadata(event, stats);
    }

    for tool in &mut record.tool_rows {
        init_privacy_metadata(tool, &privacy.redaction_policy_version);
        let mut stats = PrivacyRowStats::default();
        for field in ["input_json", "output_json", "output_text"] {
            apply_privacy_to_string_field(
                tool,
                field,
                privacy.tool_io_mode,
                &detectors,
                encryption_key_ref,
                &mut stats,
            )?;
        }
        finish_privacy_metadata(tool, stats);
    }

    for error in &mut record.error_rows {
        init_privacy_metadata(error, &privacy.redaction_policy_version);
        finish_privacy_metadata(error, PrivacyRowStats::default());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_privacy_redaction, build_link_row, normalize_record, RecordContext};
    use serde_json::{json, Value};
    use std::collections::HashMap;

    #[test]
    fn codex_tool_call_normalization() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_123",
                "name": "Read",
                "arguments": "{\"path\":\"README.md\"}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/13/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
            123,
            1,
            42,
            1024,
            "",
            "",
        )
        .expect("codex tool call should normalize");

        assert_eq!(out.event_rows.len(), 1);
        assert_eq!(out.tool_rows.len(), 1);
        assert!(out.error_rows.is_empty());
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(row.get("tool_name").unwrap().as_str().unwrap(), "Read");
    }

    #[test]
    fn codex_turn_context_promotes_model_and_turn_id() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:42.191Z",
            "type": "turn_context",
            "payload": {
                "turn_id": "019c5f6a-49bd-7920-ac67-1dd8e33b0e95",
                "model": "gpt-5.3-codex"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            1,
            1,
            "",
            "",
        )
        .expect("codex turn context should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "turn_context"
        );
        assert_eq!(row.get("model").unwrap().as_str().unwrap(), "gpt-5.3-codex");
        assert_eq!(
            row.get("request_id").unwrap().as_str().unwrap(),
            "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
        );
        assert_eq!(
            row.get("item_id").unwrap().as_str().unwrap(),
            "019c5f6a-49bd-7920-ac67-1dd8e33b0e95"
        );
    }

    #[test]
    fn codex_token_count_promotes_usage_fields() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 65323,
                        "output_tokens": 445,
                        "cached_input_tokens": 58624
                    }
                },
                "rate_limits": {
                    "limit_name": "GPT-5.3-Codex-Spark",
                    "limit_id": "codex_bengalfox",
                    "plan_type": "pro"
                }
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            2,
            2,
            "",
            "",
        )
        .expect("codex token count should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "token_count"
        );
        assert_eq!(row.get("input_tokens").unwrap().as_u64().unwrap(), 65323);
        assert_eq!(row.get("output_tokens").unwrap().as_u64().unwrap(), 445);
        assert_eq!(
            row.get("cache_read_tokens").unwrap().as_u64().unwrap(),
            58624
        );
        assert_eq!(
            row.get("model").unwrap().as_str().unwrap(),
            "gpt-5.3-codex-spark"
        );
        assert_eq!(row.get("service_tier").unwrap().as_str().unwrap(), "pro");
        assert!(!row
            .get("token_usage_json")
            .unwrap()
            .as_str()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn codex_token_count_alias_codex_maps_to_xhigh() {
        let record = json!({
            "timestamp": "2026-02-15T04:52:55.538Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 72636,
                        "output_tokens": 285,
                        "cached_input_tokens": 70784
                    }
                },
                "rate_limits": {
                    "limit_id": "codex",
                    "limit_name": null,
                    "plan_type": "pro"
                }
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            4,
            4,
            "",
            "",
        )
        .expect("codex token count alias should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("model").unwrap().as_str().unwrap(),
            "gpt-5.3-codex-xhigh"
        );
    }

    #[test]
    fn codex_custom_tool_call_promotes_tool_fields() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call",
                "call_id": "call_abc",
                "name": "apply_patch",
                "status": "completed",
                "input": "*** Begin Patch\n*** End Patch\n"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            3,
            3,
            "",
            "",
        )
        .expect("codex custom tool call should normalize");

        assert_eq!(out.event_rows.len(), 1);
        assert_eq!(out.tool_rows.len(), 1);
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(
            row.get("tool_call_id").unwrap().as_str().unwrap(),
            "call_abc"
        );
        assert_eq!(
            row.get("tool_name").unwrap().as_str().unwrap(),
            "apply_patch"
        );
        assert_eq!(row.get("op_status").unwrap().as_str().unwrap(), "completed");
    }

    #[test]
    fn claude_tool_use_and_result_blocks() {
        let record = json!({
            "type": "assistant",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "uuid": "assistant-1",
            "parentUuid": "user-1",
            "requestId": "req-1",
            "timestamp": "2026-01-19T15:58:41.421Z",
            "message": {
                "model": "claude-opus-4-5-20251101",
                "role": "assistant",
                "usage": {
                    "input_tokens": 9,
                    "output_tokens": 5,
                    "cache_creation_input_tokens": 19630,
                    "cache_read_input_tokens": 0,
                    "service_tier": "standard"
                },
                "content": [
                    {
                        "type": "tool_use",
                        "id": "toolu_1",
                        "name": "WebFetch",
                        "input": {"url": "https://example.com"}
                    },
                    {
                        "type": "text",
                        "text": "done"
                    }
                ]
            }
        });

        let out = normalize_record(
            &record,
            "claude",
            "claude-code",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            55,
            2,
            10,
            100,
            "",
            "",
        )
        .expect("claude event should normalize");

        assert_eq!(out.event_rows.len(), 2);
        assert_eq!(out.tool_rows.len(), 1);

        let first = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            first.get("event_kind").unwrap().as_str().unwrap(),
            "tool_call"
        );
        assert_eq!(
            first.get("harness").unwrap().as_str().unwrap(),
            "claude-code"
        );
        assert_eq!(
            first.get("inference_provider").unwrap().as_str().unwrap(),
            "anthropic"
        );
        assert!(out.error_rows.is_empty());
    }

    #[test]
    fn factory_droid_settings_promote_provider_model_and_tokens() {
        let record = json!({
            "type": "session_settings",
            "timestamp": "2026-04-22T08:18:41.713Z",
            "session_id": "65082ccf-d3b7-46ac-916e-e3b1cedac604",
            "settings": {
                "model": "claude-opus-4-7",
                "providerLock": "anthropic",
                "assistantActiveTimeMs": 11833,
                "tokenUsage": {
                    "inputTokens": 18189,
                    "outputTokens": 90,
                    "cacheCreationTokens": 12,
                    "cacheReadTokens": 34,
                    "thinkingTokens": 76
                }
            }
        });

        let out = normalize_record(
            &record,
            "factory-droid",
            "factory-droid",
            "/Users/eric/.factory/sessions/project/65082ccf-d3b7-46ac-916e-e3b1cedac604.settings.json",
            1,
            1,
            0,
            0,
            "",
            "anthropic/claude-opus-4-7",
        )
        .expect("factory droid settings should normalize");

        assert_eq!(out.event_rows.len(), 1);
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").and_then(Value::as_str),
            Some("session_meta")
        );
        assert_eq!(
            row.get("inference_provider").and_then(Value::as_str),
            Some("anthropic")
        );
        assert_eq!(
            row.get("model").and_then(Value::as_str),
            Some("claude-opus-4-7")
        );
        assert_eq!(row.get("input_tokens").and_then(Value::as_u64), Some(18189));
        assert_eq!(row.get("output_tokens").and_then(Value::as_u64), Some(90));
        assert_eq!(
            row.get("cache_read_tokens").and_then(Value::as_u64),
            Some(34)
        );
        assert_eq!(
            row.get("cache_write_tokens").and_then(Value::as_u64),
            Some(12)
        );
        assert_eq!(row.get("latency_ms").and_then(Value::as_u64), Some(11833));
        assert_eq!(out.session_hint, "65082ccf-d3b7-46ac-916e-e3b1cedac604");
        assert_eq!(out.model_hint, "claude-opus-4-7");
    }

    #[test]
    fn factory_droid_message_prunes_encrypted_payload_and_links_parent() {
        let record = json!({
            "type": "message",
            "id": "assistant-1",
            "parentId": "user-1",
            "timestamp": "2026-04-22T08:17:47.786Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "short thought"},
                    {"type": "text", "text": "Done"}
                ],
                "openaiMessageId": "msg_123",
                "openaiReasoningId": "rs_123",
                "openaiPhase": "final_answer",
                "openaiEncryptedContent": "encrypted-noise"
            }
        });

        let out = normalize_record(
            &record,
            "factory-droid",
            "factory-droid",
            "/Users/eric/.factory/sessions/project/65082ccf-d3b7-46ac-916e-e3b1cedac604.jsonl",
            1,
            1,
            3,
            200,
            "65082ccf-d3b7-46ac-916e-e3b1cedac604",
            "anthropic/claude-opus-4-7",
        )
        .expect("factory droid message should normalize");

        assert_eq!(out.event_rows.len(), 2);
        assert_eq!(out.link_rows.len(), 2);
        let reasoning = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            reasoning.get("event_kind").and_then(Value::as_str),
            Some("reasoning")
        );
        assert_eq!(
            reasoning.get("has_reasoning").and_then(Value::as_u64),
            Some(1)
        );
        let text = out.event_rows[1].as_object().unwrap();
        assert_eq!(
            text.get("event_kind").and_then(Value::as_str),
            Some("message")
        );
        assert_eq!(
            text.get("session_id").and_then(Value::as_str),
            Some("65082ccf-d3b7-46ac-916e-e3b1cedac604")
        );
        assert_eq!(
            text.get("actor_kind").and_then(Value::as_str),
            Some("assistant")
        );
        assert_eq!(
            text.get("text_content").and_then(Value::as_str),
            Some("Done")
        );
        assert_eq!(
            text.get("request_id").and_then(Value::as_str),
            Some("msg_123")
        );
        assert_eq!(text.get("trace_id").and_then(Value::as_str), Some("rs_123"));
        let payload = text.get("payload_json").and_then(Value::as_str).unwrap();
        assert!(!payload.contains("encrypted-noise"));
        let link = out.link_rows[0].as_object().unwrap();
        assert_eq!(
            link.get("linked_external_id").and_then(Value::as_str),
            Some("user-1")
        );
    }

    #[test]
    fn factory_droid_tool_blocks_emit_tool_io() {
        let record = json!({
            "type": "message",
            "id": "assistant-tool",
            "timestamp": "2026-04-22T08:17:48.000Z",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "toolu_1",
                        "name": "Read",
                        "input": {"path": "README.md"}
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_1",
                        "content": [{"type": "text", "text": "contents"}]
                    }
                ]
            }
        });

        let out = normalize_record(
            &record,
            "factory-droid",
            "factory-droid",
            "/Users/eric/.factory/sessions/project/65082ccf-d3b7-46ac-916e-e3b1cedac604.jsonl",
            1,
            1,
            4,
            300,
            "65082ccf-d3b7-46ac-916e-e3b1cedac604",
            "anthropic/claude-opus-4-7",
        )
        .expect("factory droid tool blocks should normalize");

        assert_eq!(out.event_rows.len(), 2);
        assert_eq!(out.tool_rows.len(), 2);
        assert_eq!(
            out.event_rows[0].get("event_kind").and_then(Value::as_str),
            Some("tool_call")
        );
        assert_eq!(
            out.event_rows[1].get("event_kind").and_then(Value::as_str),
            Some("tool_result")
        );
        assert_eq!(
            out.tool_rows[0].get("tool_phase").and_then(Value::as_str),
            Some("request")
        );
        assert_eq!(
            out.tool_rows[1].get("tool_phase").and_then(Value::as_str),
            Some("response")
        );
    }

    #[test]
    fn kimi_cli_status_update_stamps_placeholder_model_and_sums_input_buckets() {
        let record = json!({
            "timestamp": 1776735761.27701_f64,
            "message": {
                "type": "StatusUpdate",
                "payload": {
                    "message_id": "msg_abc",
                    "context_usage": 0.42,
                    "token_usage": {
                        "input_other": 1234,
                        "input_cache_read": 56,
                        "input_cache_creation": 78,
                        "output": 90
                    }
                }
            }
        });

        let out = normalize_record(
            &record,
            "kimi-cli",
            "kimi-cli",
            "/Users/eric/.kimi/sessions/work-abc/sess-xyz/wire.jsonl",
            1,
            1,
            5,
            500,
            "",
            "",
        )
        .expect("kimi status update should normalize");

        assert_eq!(out.event_rows.len(), 1);
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(row.get("model").and_then(Value::as_str), Some("kimi-cli"));
        assert_eq!(row.get("input_tokens").and_then(Value::as_u64), Some(1368));
        assert_eq!(row.get("output_tokens").and_then(Value::as_u64), Some(90));
        assert_eq!(out.model_hint, "kimi-cli");
    }

    #[test]
    fn kimi_cli_content_part_stamps_placeholder_model() {
        let record = json!({
            "timestamp": 1776735761.27701_f64,
            "message": {
                "type": "ContentPart",
                "payload": {
                    "type": "text",
                    "text": "hello there"
                }
            }
        });

        let out = normalize_record(
            &record,
            "kimi-cli",
            "kimi-cli",
            "/Users/eric/.kimi/sessions/work-abc/sess-xyz/wire.jsonl",
            1,
            1,
            6,
            600,
            "",
            "",
        )
        .expect("kimi content part should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(row.get("model").and_then(Value::as_str), Some("kimi-cli"));
    }

    #[test]
    fn invalid_timestamp_uses_epoch_and_emits_timestamp_parse_error() {
        let record = json!({
            "timestamp": "not-a-timestamp",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_bad_ts",
                "name": "Read",
                "arguments": "{}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            9,
            2,
            7,
            99,
            "",
            "",
        )
        .expect("codex event with invalid timestamp should normalize");

        let event_row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            event_row.get("event_ts").unwrap().as_str().unwrap(),
            "1970-01-01 00:00:00.000"
        );
        assert_eq!(
            event_row.get("session_date").unwrap().as_str().unwrap(),
            "1970-01-01"
        );

        assert_eq!(out.error_rows.len(), 1);
        let error = out.error_rows[0].as_object().unwrap();
        assert_eq!(
            error.get("error_kind").unwrap().as_str().unwrap(),
            "timestamp_parse_error"
        );
        assert_eq!(
            error.get("raw_fragment").unwrap().as_str().unwrap(),
            "not-a-timestamp"
        );
    }

    #[test]
    fn invalid_timestamp_preserves_session_date_from_source_path() {
        let record = json!({
            "timestamp": "still-not-a-timestamp",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_bad_ts",
                "name": "Read",
                "arguments": "{}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/16/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            11,
            4,
            12,
            144,
            "",
            "",
        )
        .expect("codex event should normalize while preserving session date from path");

        let event_row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            event_row.get("event_ts").unwrap().as_str().unwrap(),
            "1970-01-01 00:00:00.000"
        );
        assert_eq!(
            event_row.get("session_date").unwrap().as_str().unwrap(),
            "2026-02-16"
        );
        assert_eq!(out.error_rows.len(), 1);
    }

    #[test]
    fn unknown_harness_is_rejected() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:42.191Z",
            "type": "turn_context",
        });

        let err = normalize_record(
            &record,
            "unknown",
            "unknown",
            "/tmp/sessions/session-1.jsonl",
            1,
            1,
            1,
            1,
            "",
            "",
        )
        .expect_err("unknown harness should be rejected");

        assert!(
            err.to_string().contains("unsupported harness"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn legacy_claude_harness_value_is_rejected() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:42.191Z",
            "type": "assistant",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "uuid": "assistant-1",
            "message": {"role": "assistant", "content": "done"}
        });

        let err = normalize_record(
            &record,
            "claude",
            "claude",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            1,
            1,
            1,
            1,
            "",
            "",
        )
        .expect_err("legacy `claude` harness value should be rejected");

        assert!(
            err.to_string().contains("unsupported harness"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn codex_event_populates_inference_provider_openai() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "call_id": "call_ip",
                "name": "Read",
                "arguments": "{}"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/14/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
            10,
            1,
            1,
            1,
            "",
            "",
        )
        .expect("codex event should normalize");

        assert_eq!(
            out.raw_row
                .get("inference_provider")
                .unwrap()
                .as_str()
                .unwrap(),
            "openai"
        );
        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(row.get("harness").unwrap().as_str().unwrap(), "codex");
        assert_eq!(
            row.get("inference_provider").unwrap().as_str().unwrap(),
            "openai"
        );
        let tool_row = out.tool_rows[0].as_object().unwrap();
        assert_eq!(
            tool_row
                .get("inference_provider")
                .unwrap()
                .as_str()
                .unwrap(),
            "openai"
        );
    }

    #[test]
    fn oversized_raw_and_payload_json_are_capped_for_clickhouse_rows() {
        let text = "x".repeat(super::RAW_JSON_LIMIT + 1024);
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": text }]
        });
        let full_raw_json = super::compact_json(&record);

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/14/session-019c59f9-6389-77a1-a0cb-304eecf935b6.jsonl",
            10,
            1,
            1,
            1024,
            "",
            "",
        )
        .expect("large codex message should normalize");

        let raw_json = out
            .raw_row
            .get("raw_json")
            .and_then(Value::as_str)
            .expect("raw_json should be stored as a string");
        assert!(raw_json.len() <= super::RAW_JSON_LIMIT);
        assert_ne!(raw_json, full_raw_json);
        assert_eq!(
            out.raw_row
                .get("raw_json_hash")
                .and_then(Value::as_u64)
                .expect("raw_json_hash should be numeric"),
            super::raw_hash(&full_raw_json)
        );

        let payload_json = out.event_rows[0]
            .get("payload_json")
            .and_then(Value::as_str)
            .expect("payload_json should be stored as a string");
        assert!(payload_json.len() <= super::TEXT_LIMIT);
    }

    #[test]
    fn claude_links_split_event_uids_from_external_ids() {
        let record = json!({
            "type": "assistant",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "uuid": "assistant-2",
            "parentUuid": "user-parent-2",
            "toolUseID": "toolu_42",
            "sourceToolAssistantUUID": "assistant-root-1",
            "requestId": "req-2",
            "timestamp": "2026-01-19T15:59:41.421Z",
            "message": {
                "role": "assistant",
                "content": "done"
            }
        });

        let out = normalize_record(
            &record,
            "claude",
            "claude-code",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            55,
            2,
            11,
            101,
            "",
            "",
        )
        .expect("claude assistant record should normalize");

        assert_eq!(out.link_rows.len(), 3);

        let by_type = out
            .link_rows
            .iter()
            .map(|row| {
                let obj = row.as_object().expect("link row object");
                let link_type = obj
                    .get("link_type")
                    .and_then(|v| v.as_str())
                    .expect("link_type")
                    .to_string();
                (link_type, obj.clone())
            })
            .collect::<HashMap<_, _>>();

        let parent = by_type.get("parent_uuid").expect("parent_uuid link");
        assert_eq!(
            parent
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "user-parent-2"
        );
        assert_eq!(
            parent
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );

        let tool_use = by_type.get("tool_use_id").expect("tool_use_id link");
        assert_eq!(
            tool_use
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "toolu_42"
        );
        assert_eq!(
            tool_use
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );

        let source_tool = by_type
            .get("source_tool_assistant")
            .expect("source_tool_assistant link");
        assert_eq!(
            source_tool
                .get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            "assistant-root-1"
        );
        assert_eq!(
            source_tool
                .get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );
    }

    #[test]
    fn codex_compacted_parent_link_uses_event_uid_target() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "compacted",
            "payload": {
                "replacement_history": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "hello"}
                        ]
                    }
                ]
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            12,
            12,
            "",
            "",
        )
        .expect("compacted record should normalize");

        let compacted_uid = out.event_rows[0]
            .get("event_uid")
            .and_then(|v| v.as_str())
            .expect("compacted event uid");
        let link = out.link_rows[0].as_object().expect("compacted link");

        assert_eq!(
            link.get("link_type").and_then(|v| v.as_str()).unwrap(),
            "compacted_parent"
        );
        assert_eq!(
            link.get("linked_event_uid")
                .and_then(|v| v.as_str())
                .unwrap(),
            compacted_uid
        );
        assert_eq!(
            link.get("linked_external_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            ""
        );
    }

    #[test]
    fn codex_unknown_payload_type_is_canonicalized() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "response_item",
            "payload": {
                "type": "brand_new_payload_type",
                "body": "x"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            5,
            5,
            "",
            "",
        )
        .expect("record should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(row.get("event_kind").unwrap().as_str().unwrap(), "unknown");
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "unknown"
        );
    }

    #[test]
    fn codex_event_msg_known_operational_payload_type_is_preserved() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "event_msg",
            "payload": {
                "type": "task_started",
                "status": "in_progress"
            }
        });

        let out = normalize_record(
            &record,
            "codex",
            "codex",
            "/Users/eric/.codex/sessions/2026/02/15/session-019c5f6a-49bd-7920-ac67-1dd8e33b0e95.jsonl",
            1,
            1,
            6,
            6,
            "",
            "",
        )
        .expect("record should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(
            row.get("event_kind").unwrap().as_str().unwrap(),
            "event_msg"
        );
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "task_started"
        );
    }

    #[test]
    fn claude_progress_unknown_payload_type_moves_to_unknown_and_preserves_op_kind() {
        let record = json!({
            "timestamp": "2026-02-15T03:50:50.838Z",
            "type": "progress",
            "sessionId": "7c666c01-d38e-4658-8650-854ffb5b626e",
            "data": {
                "type": "provider_extension_step"
            },
            "status": "ok"
        });

        let out = normalize_record(
            &record,
            "claude",
            "claude-code",
            "/Users/eric/.claude/projects/p1/s1.jsonl",
            1,
            1,
            6,
            6,
            "",
            "",
        )
        .expect("record should normalize");

        let row = out.event_rows[0].as_object().unwrap();
        assert_eq!(row.get("event_kind").unwrap().as_str().unwrap(), "progress");
        assert_eq!(
            row.get("payload_type").unwrap().as_str().unwrap(),
            "unknown"
        );
        assert_eq!(
            row.get("op_kind").unwrap().as_str().unwrap(),
            "provider_extension_step"
        );
    }

    #[test]
    fn hermes_sharegpt_trajectory_normalizes_messages_and_tool_io() {
        let record = json!({
            "timestamp": "2026-03-30T14:22:31.456789",
            "model": "anthropic/claude-sonnet-4.6",
            "prompt_index": 7,
            "completed": true,
            "partial": false,
            "api_calls": 1,
            "conversations": [
                {
                    "from": "system",
                    "value": "You are a careful assistant."
                },
                {
                    "from": "human",
                    "value": "Find the weather in Boston."
                },
                {
                    "from": "gpt",
                    "value": "<think>Need to search first.</think>\n<tool_call>{\"name\":\"weather\",\"arguments\":{\"location\":\"Boston, MA\"}}</tool_call>"
                },
                {
                    "from": "tool",
                    "value": "<tool_response>{\"tool_call_id\":\"call_abc123\",\"name\":\"weather\",\"content\":{\"forecast\":\"rain\"}}</tool_response>"
                },
                {
                    "from": "gpt",
                    "value": "It looks rainy in Boston."
                }
            ]
        });

        let out = normalize_record(
            &record,
            "hermes-batch",
            "hermes",
            "/tmp/hermes/batch-output.jsonl",
            1,
            1,
            1,
            128,
            "",
            "",
        )
        .expect("hermes record should normalize");

        assert!(
            out.error_rows.is_empty(),
            "unexpected errors: {:?}",
            out.error_rows
        );
        assert_eq!(out.event_rows.len(), 7);
        assert_eq!(out.tool_rows.len(), 2);

        let session_id = out.session_hint.clone();
        assert!(session_id.starts_with("hermes:"));
        assert_eq!(
            out.raw_row
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap(),
            session_id
        );
        assert_eq!(
            out.raw_row.get("top_type").and_then(Value::as_str).unwrap(),
            "trajectory"
        );

        let meta = out.event_rows[0].as_object().expect("session meta row");
        assert_eq!(
            meta.get("event_kind").and_then(Value::as_str),
            Some("session_meta")
        );
        assert_eq!(
            meta.get("op_status").and_then(Value::as_str),
            Some("completed")
        );
        assert_eq!(
            meta.get("record_ts").and_then(Value::as_str),
            Some("2026-03-30T14:22:31.456789Z")
        );

        let user_message = out
            .event_rows
            .iter()
            .find(|row| {
                row.get("actor_kind") == Some(&json!("user"))
                    && row.get("event_kind") == Some(&json!("message"))
            })
            .expect("user message row");
        assert_eq!(
            user_message.get("turn_index").and_then(Value::as_u64),
            Some(1)
        );

        let reasoning = out
            .event_rows
            .iter()
            .find(|row| row.get("event_kind") == Some(&json!("reasoning")))
            .expect("reasoning row");
        assert_eq!(
            reasoning.get("payload_type").and_then(Value::as_str),
            Some("thinking")
        );
        assert_eq!(
            reasoning.get("has_reasoning").and_then(Value::as_u64),
            Some(1)
        );

        let tool_call = out
            .event_rows
            .iter()
            .find(|row| row.get("event_kind") == Some(&json!("tool_call")))
            .expect("tool call row");
        assert_eq!(
            tool_call.get("tool_name").and_then(Value::as_str),
            Some("weather")
        );
        assert_eq!(
            tool_call.get("tool_call_id").and_then(Value::as_str),
            Some("call_abc123")
        );

        let tool_result = out
            .event_rows
            .iter()
            .find(|row| {
                row.get("event_kind") == Some(&json!("tool_result"))
                    && row.get("tool_call_id") == Some(&json!("call_abc123"))
            })
            .expect("tool result row");
        assert_eq!(
            tool_result.get("tool_name").and_then(Value::as_str),
            Some("weather")
        );
        assert!(tool_result
            .get("record_ts")
            .and_then(Value::as_str)
            .unwrap()
            .ends_with('Z'));

        let final_message = out
            .event_rows
            .iter()
            .find(|row| row.get("text_content") == Some(&json!("It looks rainy in Boston.")))
            .expect("final assistant message");
        assert_eq!(
            final_message.get("model").and_then(Value::as_str),
            Some("claude-sonnet-4.6"),
            "vendor/model split should strip the leading `anthropic/` from model",
        );
        assert_eq!(
            final_message
                .get("inference_provider")
                .and_then(Value::as_str),
            Some("anthropic"),
            "inference_provider should be parsed from the record's vendor prefix",
        );
        // raw_row and tool/link rows should carry the same parsed vendor.
        assert_eq!(
            out.raw_row
                .get("inference_provider")
                .and_then(Value::as_str),
            Some("anthropic"),
        );

        let tool_request = out
            .tool_rows
            .iter()
            .find(|row| row.get("tool_phase") == Some(&json!("request")))
            .expect("tool request row");
        assert_eq!(
            tool_request.get("tool_call_id").and_then(Value::as_str),
            Some("call_abc123")
        );
        let tool_response = out
            .tool_rows
            .iter()
            .find(|row| row.get("tool_phase") == Some(&json!("response")))
            .expect("tool response row");
        assert_eq!(
            tool_response.get("output_text").and_then(Value::as_str),
            Some("{\"forecast\":\"rain\"}")
        );
    }

    #[test]
    fn link_type_is_canonicalized_to_domain() {
        let ctx = RecordContext {
            source_name: "codex",
            harness: "codex",
            inference_provider: "openai",
            session_id: "s1",
            session_date: "2026-02-15",
            source_file: "/tmp/s1.jsonl",
            source_inode: 1,
            source_generation: 1,
            source_line_no: 1,
            source_offset: 1,
            record_ts: "2026-02-15T03:50:50.838Z",
            event_ts: "2026-02-15 03:50:50.838",
        };

        let link = build_link_row(&ctx, "e1", "e2", "", "new_link_type", "{}");
        let link_obj = link.as_object().unwrap();
        assert_eq!(
            link_obj.get("link_type").unwrap().as_str().unwrap(),
            "unknown"
        );
    }

    #[test]
    fn privacy_redaction_scrubs_secrets_from_raw_and_events() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "content": [{ "type": "text", "text": "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR" }]
            }
        });

        let mut out = normalize_record(
            &record,
            "codex",
            "codex",
            "/tmp/s1.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
        )
        .expect("should normalize");

        let privacy = moraine_config::PrivacyConfig {
            enabled: true,
            redaction_policy_version: "1".to_string(),
            raw_events_mode: moraine_config::RedactionMode::RedactRaw,
            text_content_mode: moraine_config::RedactionMode::RedactRaw,
            payload_json_mode: moraine_config::RedactionMode::StoreRaw,
            tool_io_mode: moraine_config::RedactionMode::StoreRaw,
            ..moraine_config::PrivacyConfig::default()
        };

        apply_privacy_redaction(&mut out, &privacy).expect("privacy redaction");

        let raw_json = out
            .raw_row
            .get("raw_json")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(
            raw_json.contains("[REDACTED:openai_api_key]"),
            "raw_json should be redacted: {}",
            raw_json
        );
        assert!(!raw_json.contains("sk-abcdefghijklmnopqrstuvwxyz"));

        let event = out
            .event_rows
            .iter()
            .find(|r| r.get("event_kind") == Some(&json!("message")))
            .expect("message event");
        let text_content = event
            .get("text_content")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(
            text_content.contains("[REDACTED:openai_api_key]"),
            "text_content should be redacted: {}",
            text_content
        );
        assert!(!text_content.contains("sk-abcdefghijklmnopqrstuvwxyz"));
        assert_eq!(
            event.get("privacy_policy_version").and_then(Value::as_str),
            Some("1")
        );
        assert_eq!(
            event
                .get("privacy_redaction_applied")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            event.get("privacy_redaction_count").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn privacy_redaction_disabled_is_noop() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "content": [{ "type": "text", "text": "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR" }]
            }
        });

        let mut out = normalize_record(
            &record,
            "codex",
            "codex",
            "/tmp/s1.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
        )
        .expect("should normalize");

        let privacy = moraine_config::PrivacyConfig {
            enabled: false,
            redaction_policy_version: "1".to_string(),
            raw_events_mode: moraine_config::RedactionMode::RedactRaw,
            text_content_mode: moraine_config::RedactionMode::RedactRaw,
            payload_json_mode: moraine_config::RedactionMode::StoreRaw,
            tool_io_mode: moraine_config::RedactionMode::StoreRaw,
            ..moraine_config::PrivacyConfig::default()
        };

        apply_privacy_redaction(&mut out, &privacy).expect("privacy disabled");

        let raw_json = out
            .raw_row
            .get("raw_json")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(raw_json.contains("sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR"));
    }

    #[test]
    fn privacy_encrypt_raw_encrypts_whole_field_and_sets_metadata() {
        let record = json!({
            "timestamp": "2026-02-14T02:28:00.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "content": [{ "type": "text", "text": "plain text without detector hits" }]
            }
        });

        let mut out = normalize_record(
            &record,
            "codex",
            "codex",
            "/tmp/s1.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
        )
        .expect("should normalize");

        let key_path = std::env::temp_dir().join(format!(
            "moraine-privacy-key-{}-{}.hex",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let key_hex = "08".repeat(32);
        std::fs::write(&key_path, &key_hex).expect("write key");

        let privacy = moraine_config::PrivacyConfig {
            enabled: true,
            redaction_policy_version: "42".to_string(),
            text_content_mode: moraine_config::RedactionMode::EncryptRaw,
            encryption_key_id: "local-test".to_string(),
            encryption_key_file: key_path.display().to_string(),
            ..moraine_config::PrivacyConfig::default()
        };

        apply_privacy_redaction(&mut out, &privacy).expect("privacy encryption");
        std::fs::remove_file(&key_path).ok();

        let event = out
            .event_rows
            .iter()
            .find(|r| r.get("event_kind") == Some(&json!("message")))
            .expect("message event");
        let encrypted = event
            .get("text_content")
            .and_then(Value::as_str)
            .expect("encrypted text");
        assert!(encrypted.starts_with("moraine:v1:aes-256-gcm:local-test:"));
        assert!(!encrypted.contains("plain text"));
        let key = moraine_privacy::EncryptionKey::from_material("local-test", key_hex.as_bytes())
            .expect("key");
        assert_eq!(
            moraine_privacy::decrypt_text(encrypted, &key).expect("decrypt"),
            "plain text without detector hits"
        );
        assert_eq!(
            event.get("privacy_policy_version").and_then(Value::as_str),
            Some("42")
        );
        assert_eq!(
            event
                .get("privacy_redaction_applied")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            event.get("privacy_key_id").and_then(Value::as_str),
            Some("local-test")
        );
    }

    #[test]
    fn privacy_encrypt_raw_without_key_fails_closed() {
        let mut out = normalize_record(
            &json!({
                "timestamp": "2026-02-14T02:28:00.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "content": [{ "type": "text", "text": "hello" }]
                }
            }),
            "codex",
            "codex",
            "/tmp/s1.jsonl",
            1,
            1,
            1,
            0,
            "",
            "",
        )
        .expect("should normalize");

        let privacy = moraine_config::PrivacyConfig {
            enabled: true,
            redaction_policy_version: "1".to_string(),
            text_content_mode: moraine_config::RedactionMode::EncryptRaw,
            encryption_key_id: "local-test".to_string(),
            ..moraine_config::PrivacyConfig::default()
        };

        let err =
            apply_privacy_redaction(&mut out, &privacy).expect_err("missing key must fail closed");
        assert!(err.to_string().contains("encrypt_raw requires"));
    }
}
