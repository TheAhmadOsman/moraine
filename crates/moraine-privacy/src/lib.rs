//! Privacy redaction and secret detection for Moraine.
//!
//! This crate provides regex-based secret detection, configurable redaction
//! modes, and authenticated encryption for sensitive content at ingest time.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rand::RngCore;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ENVELOPE_VERSION: &str = "v1";
pub const ENVELOPE_ALGORITHM: &str = "aes-256-gcm";
pub const ENVELOPE_PREFIX: &str = "moraine";

#[derive(Debug, thiserror::Error)]
pub enum PrivacyError {
    #[error("missing encryption key for encrypt_raw mode")]
    MissingEncryptionKey,
    #[error("invalid encryption key id: {0}")]
    InvalidKeyId(String),
    #[error("invalid encryption key material: {0}")]
    InvalidKeyMaterial(String),
    #[error("encryption failed")]
    EncryptionFailed,
    #[error("decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("unsupported encryption envelope: {0}")]
    UnsupportedEnvelope(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionKey {
    pub key_id: String,
    key_bytes: [u8; 32],
}

impl EncryptionKey {
    pub fn from_raw(key_id: impl Into<String>, key_bytes: [u8; 32]) -> Result<Self, PrivacyError> {
        let key_id = validate_key_id(key_id.into())?;
        Ok(Self { key_id, key_bytes })
    }

    pub fn from_material(key_id: impl Into<String>, material: &[u8]) -> Result<Self, PrivacyError> {
        if material.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(material);
            return Self::from_raw(key_id, key);
        }

        let text = std::str::from_utf8(material)
            .map(str::trim)
            .map_err(|err| PrivacyError::InvalidKeyMaterial(format!("not utf-8: {err}")))?;
        if text.is_empty() {
            return Err(PrivacyError::InvalidKeyMaterial("empty key".to_string()));
        }

        let decoded = if text.len() == 64 && text.chars().all(|ch| ch.is_ascii_hexdigit()) {
            hex::decode(text).map_err(|err| {
                PrivacyError::InvalidKeyMaterial(format!("hex decode failed: {err}"))
            })?
        } else {
            B64.decode(text).map_err(|err| {
                PrivacyError::InvalidKeyMaterial(format!("base64 decode failed: {err}"))
            })?
        };

        if decoded.len() != 32 {
            return Err(PrivacyError::InvalidKeyMaterial(format!(
                "expected 32 bytes, got {}",
                decoded.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        Self::from_raw(key_id, key)
    }

    pub fn from_env(key_id: impl Into<String>, var_name: &str) -> Result<Self, PrivacyError> {
        let value = std::env::var(var_name).map_err(|_| PrivacyError::MissingEncryptionKey)?;
        Self::from_material(key_id, value.as_bytes())
    }

    pub fn from_file(
        key_id: impl Into<String>,
        path: &std::path::Path,
    ) -> Result<Self, PrivacyError> {
        let bytes = std::fs::read(path).map_err(|err| {
            PrivacyError::InvalidKeyMaterial(format!("failed to read {}: {err}", path.display()))
        })?;
        Self::from_material(key_id, &bytes)
    }
}

fn validate_key_id(key_id: String) -> Result<String, PrivacyError> {
    if key_id.is_empty()
        || !key_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(PrivacyError::InvalidKeyId(key_id));
    }
    Ok(key_id)
}

pub fn encrypt_text(plaintext: &str, key: &EncryptionKey) -> Result<String, PrivacyError> {
    let cipher =
        Aes256Gcm::new_from_slice(&key.key_bytes).map_err(|_| PrivacyError::EncryptionFailed)?;
    let mut nonce_bytes = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|_| PrivacyError::EncryptionFailed)?;

    Ok(format!(
        "{ENVELOPE_PREFIX}:{ENVELOPE_VERSION}:{ENVELOPE_ALGORITHM}:{}:{}:{}",
        key.key_id,
        B64.encode(nonce_bytes),
        B64.encode(ciphertext)
    ))
}

pub fn decrypt_text(envelope: &str, key: &EncryptionKey) -> Result<String, PrivacyError> {
    let parts = envelope.split(':').collect::<Vec<_>>();
    if parts.len() != 6 {
        return Err(PrivacyError::UnsupportedEnvelope(format!(
            "expected 6 colon-separated parts, got {}",
            parts.len()
        )));
    }
    if parts[0] != ENVELOPE_PREFIX {
        return Err(PrivacyError::UnsupportedEnvelope(format!(
            "prefix {}, expected {ENVELOPE_PREFIX}",
            parts[0]
        )));
    }
    if parts[1] != ENVELOPE_VERSION {
        return Err(PrivacyError::UnsupportedEnvelope(format!(
            "version {}, expected {ENVELOPE_VERSION}",
            parts[1]
        )));
    }
    if parts[2] != ENVELOPE_ALGORITHM {
        return Err(PrivacyError::UnsupportedEnvelope(format!(
            "algorithm {}, expected {ENVELOPE_ALGORITHM}",
            parts[2]
        )));
    }
    if parts[3] != key.key_id {
        return Err(PrivacyError::DecryptionFailed(format!(
            "envelope key id {} does not match provided key {}",
            parts[3], key.key_id
        )));
    }

    let nonce_bytes = B64
        .decode(parts[4])
        .map_err(|err| PrivacyError::DecryptionFailed(format!("bad nonce: {err}")))?;
    if nonce_bytes.len() != 12 {
        return Err(PrivacyError::DecryptionFailed(format!(
            "expected 12-byte nonce, got {}",
            nonce_bytes.len()
        )));
    }
    let ciphertext = B64
        .decode(parts[5])
        .map_err(|err| PrivacyError::DecryptionFailed(format!("bad ciphertext: {err}")))?;

    let cipher = Aes256Gcm::new_from_slice(&key.key_bytes)
        .map_err(|err| PrivacyError::DecryptionFailed(err.to_string()))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext.as_ref())
        .map_err(|_| PrivacyError::DecryptionFailed("authentication failed".to_string()))?;
    String::from_utf8(plaintext)
        .map_err(|err| PrivacyError::DecryptionFailed(format!("invalid utf-8: {err}")))
}

/// How to handle a field that contains detected secrets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionMode {
    /// Store the original value unchanged.
    StoreRaw,
    /// Replace the value with a SHA-256 hash (hex, 16 chars).
    HashRaw,
    /// Replace detected secrets with `[REDACTED:<kind>]`.
    #[default]
    RedactRaw,
    /// Drop the entire value (empty string).
    DropRaw,
    /// Encrypt the entire configured field value with authenticated encryption.
    EncryptRaw,
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
    /// Number of merged detector hits or encrypted fields.
    pub count: usize,
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
    encryption_key: Option<&EncryptionKey>,
) -> Result<RedactionResult, PrivacyError> {
    if mode == RedactionMode::StoreRaw {
        return Ok(RedactionResult {
            text: text.to_string(),
            was_redacted: false,
            kinds: Vec::new(),
            count: 0,
        });
    }

    if mode == RedactionMode::EncryptRaw {
        let key = encryption_key.ok_or(PrivacyError::MissingEncryptionKey)?;
        return Ok(RedactionResult {
            text: encrypt_text(text, key)?,
            was_redacted: true,
            kinds: vec!["encrypted".to_string()],
            count: 1,
        });
    }

    if mode == RedactionMode::DropRaw {
        let mut all_kinds = Vec::new();
        let matches = find_secrets(text, detectors);
        if matches.is_empty() {
            return Ok(RedactionResult {
                text: text.to_string(),
                was_redacted: false,
                kinds: all_kinds,
                count: 0,
            });
        }
        for m in &matches {
            all_kinds.push(m.kind.clone());
        }
        all_kinds.sort();
        all_kinds.dedup();
        return Ok(RedactionResult {
            text: String::new(),
            was_redacted: true,
            kinds: all_kinds,
            count: matches.len(),
        });
    }

    let matches = find_secrets(text, detectors);
    if matches.is_empty() {
        return Ok(RedactionResult {
            text: text.to_string(),
            was_redacted: false,
            kinds: Vec::new(),
            count: 0,
        });
    }

    let merged = merge_matches(matches);
    let mut kinds: Vec<String> = merged.iter().map(|m| m.kind.clone()).collect();
    kinds.sort();
    kinds.dedup();
    let count = merged.len();

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
            RedactionMode::EncryptRaw => unreachable!(),
        };
        result.push_str(&replacement);
        last_end = m.end;
    }
    result.push_str(&text[last_end..]);

    Ok(RedactionResult {
        text: result,
        was_redacted: true,
        kinds,
        count,
    })
}

/// Recursively redact string values inside a JSON value.
///
/// Only JSON string values are scanned; numbers, booleans, null, and nested
/// structures are left untouched (but their child strings are processed).
pub fn redact_json_value(
    value: &mut serde_json::Value,
    mode: RedactionMode,
    detectors: &[RegexDetector],
    encryption_key: Option<&EncryptionKey>,
) -> Result<bool, PrivacyError> {
    match value {
        serde_json::Value::String(s) => {
            let r = redact_text(s, mode, detectors, encryption_key)?;
            if r.was_redacted {
                *s = r.text;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        serde_json::Value::Array(arr) => {
            let mut any = false;
            for item in arr.iter_mut() {
                if redact_json_value(item, mode, detectors, encryption_key)? {
                    any = true;
                }
            }
            Ok(any)
        }
        serde_json::Value::Object(map) => {
            let mut any = false;
            for (_, v) in map.iter_mut() {
                if redact_json_value(v, mode, detectors, encryption_key)? {
                    any = true;
                }
            }
            Ok(any)
        }
        _ => Ok(false),
    }
}

/// Convenience: redact specific fields in a JSON object.
pub fn redact_json_fields(
    value: &mut serde_json::Value,
    field_names: &[&str],
    mode: RedactionMode,
    detectors: &[RegexDetector],
    encryption_key: Option<&EncryptionKey>,
) -> Result<bool, PrivacyError> {
    let serde_json::Value::Object(map) = value else {
        return Ok(false);
    };
    let mut any = false;
    for (key, val) in map.iter_mut() {
        if field_names.contains(&key.as_str()) {
            if let serde_json::Value::String(s) = val {
                let r = redact_text(s, mode, detectors, encryption_key)?;
                if r.was_redacted {
                    *s = r.text;
                    any = true;
                }
            } else {
                if redact_json_value(val, mode, detectors, encryption_key)? {
                    any = true;
                }
            }
        }
    }
    Ok(any)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> EncryptionKey {
        EncryptionKey::from_raw("test-key", [7u8; 32]).expect("valid key")
    }

    fn other_test_key() -> EncryptionKey {
        EncryptionKey::from_raw("test-key", [9u8; 32]).expect("valid key")
    }

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
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors, None).expect("redact");
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:openai_api_key]"));
        assert!(!r.text.contains("sk-abcdefghij"));
    }

    #[test]
    fn hash_mode() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::HashRaw, &detectors, None).expect("hash");
        assert!(r.was_redacted);
        assert!(r.text.contains("[HASH:"));
    }

    #[test]
    fn drop_mode() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::DropRaw, &detectors, None).expect("drop");
        assert!(r.was_redacted);
        assert!(r.text.is_empty());
    }

    #[test]
    fn store_mode_passthrough() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "key=sk-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQR";
        let r = redact_text(text, RedactionMode::StoreRaw, &detectors, None).expect("store");
        assert!(!r.was_redacted);
        assert_eq!(r.text, text);
    }

    #[test]
    fn redacts_jwt() {
        let detectors = vec![BuiltinDetectors::jwt()];
        let text = "token=eyJhbGciOiJIUzI1NiIs.eyJzdWIiOiIxMjM0NTY3ODkwIiw.abc123";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors, None).expect("redact");
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:jwt]"));
    }

    #[test]
    fn redacts_env_secret() {
        let detectors = vec![BuiltinDetectors::env_secret()];
        let text = "API_KEY=supersecretvalue12345678";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors, None).expect("redact");
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:env_secret]"));
    }

    #[test]
    fn redacts_database_url() {
        let detectors = vec![BuiltinDetectors::database_url_with_password()];
        let text = "DATABASE_URL=postgres://user:secretpass@localhost:5432/db";
        let r = redact_text(text, RedactionMode::RedactRaw, &detectors, None).expect("redact");
        assert!(r.was_redacted);
        assert!(r.text.contains("[REDACTED:database_url_with_password]"));
    }

    #[test]
    fn encrypt_raw_encrypts_whole_field_and_decrypts() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let text = "plain text without detector hits";
        let key = test_key();
        let r =
            redact_text(text, RedactionMode::EncryptRaw, &detectors, Some(&key)).expect("encrypt");
        assert!(r.was_redacted);
        assert_eq!(r.kinds, vec!["encrypted"]);
        assert!(r.text.starts_with("moraine:v1:aes-256-gcm:test-key:"));
        assert!(!r.text.contains("plain text"));
        assert_eq!(decrypt_text(&r.text, &key).expect("decrypt"), text);
    }

    #[test]
    fn encrypt_raw_requires_key_even_without_detector_hit() {
        let detectors = vec![BuiltinDetectors::openai_api_key()];
        let err = redact_text("plain text", RedactionMode::EncryptRaw, &detectors, None)
            .expect_err("missing key should fail");
        assert!(matches!(err, PrivacyError::MissingEncryptionKey));
    }

    #[test]
    fn decrypt_rejects_wrong_key_material() {
        let key = test_key();
        let other = other_test_key();
        let envelope = encrypt_text("secret", &key).expect("encrypt");
        let err = decrypt_text(&envelope, &other).expect_err("wrong key should fail");
        assert!(matches!(err, PrivacyError::DecryptionFailed(_)));
    }

    #[test]
    fn parses_base64_and_hex_key_material() {
        let raw = [3u8; 32];
        let b64 = B64.encode(raw);
        let hex = hex::encode(raw);
        assert_eq!(
            EncryptionKey::from_material("b64", b64.as_bytes())
                .expect("base64 key")
                .key_bytes,
            raw
        );
        assert_eq!(
            EncryptionKey::from_material("hex", hex.as_bytes())
                .expect("hex key")
                .key_bytes,
            raw
        );
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
        let any = redact_json_value(&mut value, RedactionMode::RedactRaw, &detectors, None)
            .expect("json");
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
        let any = redact_json_value(&mut value, RedactionMode::RedactRaw, &detectors, None)
            .expect("json");
        assert!(any);
    }
}
