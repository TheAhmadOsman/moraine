//! Privacy redaction and secret detection for Moraine.
//!
//! This crate provides regex-based secret detection and configurable redaction
//! modes for sensitive content at ingest and retrieval time.

use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How to handle a field that contains detected secrets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionMode {
    /// Store the original value unchanged.
    StoreRaw,
    /// Replace the value with a SHA-256 hash (hex, 16 chars).
    HashRaw,
    /// Replace detected secrets with `[REDACTED:<kind>]`.
    RedactRaw,
    /// Drop the entire value (empty string).
    DropRaw,
    /// Encrypt the value (foundation: falls back to HashRaw).
    EncryptRaw,
}

impl Default for RedactionMode {
    fn default() -> Self {
        RedactionMode::RedactRaw
    }
}

/// A detected secret occurrence inside a text buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretMatch {
    pub kind: String,
    pub start: usize,
    pub end: usize,
}

/// Result of applying redaction to a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionResult {
    pub text: String,
    pub was_redacted: bool,
    /// Which detector kinds fired.
    pub kinds: Vec<String>,
}

/// A single secret detector backed by a regex.
pub struct RegexDetector {
    name: String,
    regex: Regex,
}

impl RegexDetector {
    pub fn new(name: impl Into<String>, regex: Regex) -> Self {
        Self {
            name: name.into(),
            regex,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn detect(&self, text: &str) -> Vec<SecretMatch> {
        self.regex
            .find_iter(text)
            .map(|m| SecretMatch {
                kind: self.name.clone(),
                start: m.start(),
                end: m.end(),
            })
            .collect()
    }
}

/// Built-in detector definitions.
pub struct BuiltinDetectors;

impl BuiltinDetectors {
    pub fn all() -> Vec<RegexDetector> {
        vec![
            Self::openai_api_key(),
            Self::anthropic_api_key(),
            Self::aws_access_key_id(),
            Self::aws_secret_access_key(),
            Self::jwt(),
            Self::ssh_private_key(),
            Self::bearer_token(),
            Self::database_url_with_password(),
            Self::env_secret(),
            Self::generic_api_key(),
        ]
    }

    /// OpenAI API key: `sk-` followed by 48 alphanumeric chars.
    pub fn openai_api_key() -> RegexDetector {
        RegexDetector::new(
            "openai_api_key",
            Regex::new(r"sk-[a-zA-Z0-9]{48,}").expect("valid regex"),
        )
    }

    /// Anthropic API key: `sk-ant-api` prefix.
    pub fn anthropic_api_key() -> RegexDetector {
        RegexDetector::new(
            "anthropic_api_key",
            Regex::new(r"sk-ant-api[0-9a-zA-Z\-_]{20,}").expect("valid regex"),
        )
    }

    /// AWS Access Key ID: `AKIA` + 16 alphanumeric.
    pub fn aws_access_key_id() -> RegexDetector {
        RegexDetector::new(
            "aws_access_key_id",
            Regex::new(r"AKIA[0-9A-Z]{16}").expect("valid regex"),
        )
    }

    /// AWS Secret Access Key: 40 base64-ish chars after common labels.
    pub fn aws_secret_access_key() -> RegexDetector {
        RegexDetector::new(
            "aws_secret_access_key",
            Regex::new(r#"(?i)(aws_secret_access_key|aws_secret)\s*[=:]\s*[A-Za-z0-9/+=]{40}"#)
                .expect("valid regex"),
        )
    }

    /// JSON Web Token: three base64url segments separated by dots.
    pub fn jwt() -> RegexDetector {
        RegexDetector::new(
            "jwt",
            Regex::new(r#"eyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*"#)
                .expect("valid regex"),
        )
    }

    /// SSH private key block.
    pub fn ssh_private_key() -> RegexDetector {
        RegexDetector::new(
            "ssh_private_key",
            Regex::new(r#"-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"#).expect("valid regex"),
        )
    }

    /// Bearer token header value.
    pub fn bearer_token() -> RegexDetector {
        RegexDetector::new(
            "bearer_token",
            Regex::new(r#"(?i)bearer\s+[a-zA-Z0-9_\-\.]{20,}"#).expect("valid regex"),
        )
    }

    /// Database URL containing a password.
    pub fn database_url_with_password() -> RegexDetector {
        RegexDetector::new(
            "database_url_with_password",
            Regex::new(r#"(?i)(postgres|mysql|mongodb|redis|sqlite)://[^:/\s]+:[^@\s]+@[^/\s]+"#)
                .expect("valid regex"),
        )
    }

    /// `.env`-style secret assignment.
    pub fn env_secret() -> RegexDetector {
        RegexDetector::new(
            "env_secret",
            Regex::new(r#"(?i)(API_KEY|SECRET|TOKEN|PASSWORD|PASSWD|PWD|ACCESS_KEY|PRIVATE_KEY|AUTH)\s*=\s*[^\s'"]{8,}"#).expect("valid regex"),
        )
    }

    /// Generic hex API key (32+ hex chars).
    pub fn generic_api_key() -> RegexDetector {
        RegexDetector::new(
            "generic_api_key",
            Regex::new(r#"\b[a-f0-9]{32,}\b"#).expect("valid regex"),
        )
    }
}

/// Find all secret matches in text using the provided detectors.
pub fn find_secrets(text: &str, detectors: &[RegexDetector]) -> Vec<SecretMatch> {
    let mut matches = Vec::new();
    for det in detectors {
        matches.extend(det.detect(text));
    }
    // Sort by start position so we can merge overlapping matches.
    matches.sort_by_key(|m| m.start);
    matches
}

/// Merge overlapping secret matches, preferring the longer match.
fn merge_matches(mut matches: Vec<SecretMatch>) -> Vec<SecretMatch> {
    if matches.is_empty() {
        return matches;
    }
    let mut merged = Vec::new();
    let mut current = matches.remove(0);
    for m in matches {
        if m.start <= current.end {
            // Overlap or adjacency: extend if longer.
            if m.end > current.end {
                current.end = m.end;
                current.kind = m.kind;
            }
        } else {
            merged.push(current);
            current = m;
        }
    }
    merged.push(current);
    merged
}

/// Short SHA-256 hex digest (first 16 chars) for hashing redacted content.
pub fn short_hash(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let result = hasher.finalize();
    format!(
        "{:016x}",
        u64::from_be_bytes(result[..8].try_into().unwrap_or([0u8; 8]))
    )
}

/// Apply redaction to plain text.
pub fn redact_text(
    text: &str,
    mode: RedactionMode,
    detectors: &[RegexDetector],
) -> RedactionResult {
    if mode == RedactionMode::StoreRaw {
        return RedactionResult {
            text: text.to_string(),
            was_redacted: false,
            kinds: Vec::new(),
        };
    }

    if mode == RedactionMode::DropRaw {
        let mut all_kinds = Vec::new();
        let matches = find_secrets(text, detectors);
        if matches.is_empty() {
            return RedactionResult {
                text: text.to_string(),
                was_redacted: false,
                kinds: all_kinds,
            };
        }
        for m in &matches {
            all_kinds.push(m.kind.clone());
        }
        all_kinds.sort();
        all_kinds.dedup();
        return RedactionResult {
            text: String::new(),
            was_redacted: true,
            kinds: all_kinds,
        };
    }

    let matches = find_secrets(text, detectors);
    if matches.is_empty() {
        return RedactionResult {
            text: text.to_string(),
            was_redacted: false,
            kinds: Vec::new(),
        };
    }

    let merged = merge_matches(matches);
    let mut kinds: Vec<String> = merged.iter().map(|m| m.kind.clone()).collect();
    kinds.sort();
    kinds.dedup();

    let mut result = String::with_capacity(text.len());
    let mut last_end = 0usize;

    for m in &merged {
        result.push_str(&text[last_end..m.start]);
        let secret = &text[m.start..m.end];
        let replacement = match mode {
            RedactionMode::StoreRaw => unreachable!(),
            RedactionMode::HashRaw => format!("[HASH:{}]", short_hash(secret)),
            RedactionMode::RedactRaw => format!("[REDACTED:{}]", m.kind),
            RedactionMode::DropRaw => unreachable!(),
            RedactionMode::EncryptRaw => format!("[ENCRYPTED:{}]", short_hash(secret)),
        };
        result.push_str(&replacement);
        last_end = m.end;
    }
    result.push_str(&text[last_end..]);

    RedactionResult {
        text: result,
        was_redacted: true,
        kinds,
    }
}

/// Recursively redact string values inside a JSON value.
///
/// Only JSON string values are scanned; numbers, booleans, null, and nested
/// structures are left untouched (but their child strings are processed).
pub fn redact_json_value(
    value: &mut serde_json::Value,
    mode: RedactionMode,
    detectors: &[RegexDetector],
) -> bool {
    match value {
        serde_json::Value::String(s) => {
            let r = redact_text(s, mode, detectors);
            if r.was_redacted {
                *s = r.text;
                true
            } else {
                false
            }
        }
        serde_json::Value::Array(arr) => {
            let mut any = false;
            for item in arr.iter_mut() {
                if redact_json_value(item, mode, detectors) {
                    any = true;
                }
            }
            any
        }
        serde_json::Value::Object(map) => {
            let mut any = false;
            for (_, v) in map.iter_mut() {
                if redact_json_value(v, mode, detectors) {
                    any = true;
                }
            }
            any
        }
        _ => false,
    }
}

/// Convenience: redact specific fields in a JSON object.
pub fn redact_json_fields(
    value: &mut serde_json::Value,
    field_names: &[&str],
    mode: RedactionMode,
    detectors: &[RegexDetector],
) -> bool {
    let serde_json::Value::Object(map) = value else {
        return false;
    };
    let mut any = false;
    for (key, val) in map.iter_mut() {
        if field_names.contains(&key.as_str()) {
            if let serde_json::Value::String(s) = val {
                let r = redact_text(s, mode, detectors);
                if r.was_redacted {
                    *s = r.text;
                    any = true;
                }
            } else {
                if redact_json_value(val, mode, detectors) {
                    any = true;
                }
            }
        }
    }
    any
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_openai_key() {
        let d = BuiltinDetectors::openai_api_key();
        let text = "The key is sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let m = d.detect(text);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].kind, "openai_api_key");
    }

    #[test]
    fn redacts_openai_key() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR done";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:openai_api_key]"));
        assert!(!r.text.contains("sk-abcdefghij"));
    }

    #[test]
    fn hash_mode() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::HashRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.contains("[HASH:"));
    }

    #[test]
    fn drop_mode() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::DropRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.is_empty());
    }

    #[test]
    fn store_mode_passthrough() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::StoreRaw, &detectors);
        assert!(!r.was_redacted);
        assert_eq!(r.text, text);
    }

    #[test]
    fn redacts_jwt() {
        let detectors = vec![BuiltinDetectors::jwt()];
        let text = "token=eyJhbGciOiJIUzI1NiIs.eyJzdWIiOiIxMjM0NTY3ODkwIiw.abc123";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:jwt]"));
    }

    #[test]
    fn redacts_env_secret() {
        let detectors = vec![BuiltinDetectors::env_secret()];
        let text = "API_KEY=supersecretvalue12345678";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:env_secret]"));
    }

    #[test]
    fn redacts_database_url() {
        let detectors = vec![BuiltinDetectors::database_url_with_password()];
        let text = "DATABASE_URL=postgres://user:secretpass@localhost:5432/db";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors);
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:database_url_with_password]"));
    }

    #[test]
    fn redact_json_value_recursively() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let mut value = serde_json::json!({
            "message": "Use key sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR",
            "nested": {
                "token": "sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR"
            },
            "count": 42
        });
        let any = redact_json_value(&mut value, RedactionMode::RedactRaw, &detectors);
        assert!(any);
        let s = serde_json::to_string(&value).unwrap();
        assert!(s.contains("[REDACTED:openai_api_key]"));
        assert!(!s.contains("sk-abc"));
        assert!(s.contains("42"));
    }

    #[test]
    fn merge_overlapping_matches() {
        let mut value = serde_json::json!({
            "text": "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR and bearer abcdefghijklmnopqrstuvwxyz"
        });
        let detectors = vec![
            BuiltinDetectors::openai_api_key(),
            BuiltinDetectors::bearer_token(),
        ];
        let any = redact_json_value(&mut value, RedactionMode::RedactRaw, &detectors);
        assert!(any);
    }
}
