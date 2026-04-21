use anyhow::{anyhow, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_config::IngestSource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthStatus {
    Disabled,
    Ok,
    Warning,
    Error,
    Unknown,
}

impl SourceHealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceStatusRow {
    pub name: String,
    pub harness: String,
    pub format: String,
    pub enabled: bool,
    pub glob: String,
    pub watch_root: String,
    pub status: SourceHealthStatus,
    pub checkpoint_count: u64,
    pub latest_checkpoint_at: Option<String>,
    pub raw_event_count: u64,
    pub ingest_error_count: u64,
    pub latest_error_at: Option<String>,
    pub latest_error_kind: Option<String>,
    pub latest_error_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceStatusSnapshot {
    pub sources: Vec<SourceStatusRow>,
    pub query_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceDetailSnapshot {
    pub source: SourceStatusRow,
    pub query_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourceCheckpointStatsRow {
    source_name: String,
    checkpoint_count: u64,
    latest_checkpoint_at: String,
}

#[derive(Debug, Deserialize)]
struct SourceCountRow {
    source_name: String,
    count: u64,
}

#[derive(Debug, Deserialize)]
struct SourceErrorStatsRow {
    source_name: String,
    ingest_error_count: u64,
    latest_error_at: String,
    latest_error_kind: String,
    latest_error_text: String,
}

// ---------------------------------------------------------------------------
// Deep source diagnostics — per-file view
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SourceFileRow {
    pub path: String,
    pub size_bytes: u64,
    pub modified_at: Option<String>,
    pub checkpoint_offset: Option<u64>,
    pub checkpoint_line_no: Option<u64>,
    pub checkpoint_status: Option<String>,
    pub checkpoint_updated_at: Option<String>,
    pub raw_event_count: u64,
    pub latest_error_at: Option<String>,
    pub latest_error_kind: Option<String>,
    pub latest_error_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceFilesSnapshot {
    pub source_name: String,
    pub watch_root: String,
    pub glob: String,
    pub files: Vec<SourceFileRow>,
    pub glob_match_count: usize,
    pub fs_error: Option<String>,
    pub query_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FileCheckpointRow {
    source_file: String,
    last_offset: u64,
    last_line_no: u64,
    status: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct FileRawCountRow {
    source_file: String,
    count: u64,
}

#[derive(Debug, Deserialize)]
struct FileLatestErrorRow {
    source_file: String,
    latest_error_at: String,
    error_kind: String,
    error_text: String,
}

// ---------------------------------------------------------------------------
// Deep source diagnostics — per-error view
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SourceErrorRow {
    pub ingested_at: String,
    pub source_file: String,
    pub source_line_no: u64,
    pub source_offset: u64,
    pub error_kind: String,
    pub error_text: String,
    pub raw_fragment: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceErrorsSnapshot {
    pub source_name: String,
    pub errors: Vec<SourceErrorRow>,
    pub query_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IngestErrorDetailRow {
    ingested_at: String,
    source_file: String,
    source_line_no: u64,
    source_offset: u64,
    error_kind: String,
    error_text: String,
    raw_fragment: String,
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

async fn query_source_checkpoint_stats(
    ch: &ClickHouseClient,
    db: &str,
) -> Result<Vec<SourceCheckpointStatsRow>> {
    let query = format!(
        "SELECT \
            source_name, \
            toUInt64(count()) AS checkpoint_count, \
            toString(max(updated_at)) AS latest_checkpoint_at \
         FROM {db}.ingest_checkpoints FINAL \
         GROUP BY source_name"
    );
    ch.query_rows(&query, None).await
}

async fn query_source_raw_counts(ch: &ClickHouseClient, db: &str) -> Result<Vec<SourceCountRow>> {
    let query = format!(
        "SELECT source_name, toUInt64(count()) AS count \
         FROM {db}.raw_events \
         GROUP BY source_name"
    );
    ch.query_rows(&query, None).await
}

async fn query_source_error_stats(
    ch: &ClickHouseClient,
    db: &str,
) -> Result<Vec<SourceErrorStatsRow>> {
    let query = format!(
        "SELECT \
            source_name, \
            toUInt64(count()) AS ingest_error_count, \
            toString(max(ingested_at)) AS latest_error_at, \
            argMax(error_kind, ingested_at) AS latest_error_kind, \
            argMax(error_text, ingested_at) AS latest_error_text \
         FROM {db}.ingest_errors \
         GROUP BY source_name"
    );
    ch.query_rows(&query, None).await
}

struct SourceStatusQueryState {
    checkpoints: BTreeMap<String, SourceCheckpointStatsRow>,
    raw_counts: BTreeMap<String, u64>,
    errors: BTreeMap<String, SourceErrorStatsRow>,
    query_error: Option<String>,
}

fn option_if_non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn build_source_status_row(
    source: &IngestSource,
    checkpoint: Option<&SourceCheckpointStatsRow>,
    raw_event_count: u64,
    error: Option<&SourceErrorStatsRow>,
    query_error: Option<&str>,
) -> SourceStatusRow {
    let checkpoint_count = checkpoint
        .map(|row| row.checkpoint_count)
        .unwrap_or_default();
    let ingest_error_count = error.map(|row| row.ingest_error_count).unwrap_or(0);
    let status = source_health_status(
        source.enabled,
        checkpoint_count,
        raw_event_count,
        ingest_error_count,
        query_error,
    );

    SourceStatusRow {
        name: source.name.clone(),
        harness: source.harness.clone(),
        format: source.format.clone(),
        enabled: source.enabled,
        glob: source.glob.clone(),
        watch_root: source.watch_root.clone(),
        status,
        checkpoint_count,
        latest_checkpoint_at: checkpoint
            .and_then(|row| option_if_non_empty(&row.latest_checkpoint_at)),
        raw_event_count,
        ingest_error_count,
        latest_error_at: error.and_then(|row| option_if_non_empty(&row.latest_error_at)),
        latest_error_kind: error.and_then(|row| option_if_non_empty(&row.latest_error_kind)),
        latest_error_text: error.and_then(|row| option_if_non_empty(&row.latest_error_text)),
    }
}

async fn query_source_status_state(ch: &ClickHouseClient, db: &str) -> SourceStatusQueryState {
    let mut query_error = None::<String>;
    let checkpoint_rows = match query_source_checkpoint_stats(ch, db).await {
        Ok(rows) => rows,
        Err(err) => {
            query_error = Some(format!("checkpoint query failed: {err}"));
            Vec::new()
        }
    };
    let raw_count_rows = match query_source_raw_counts(ch, db).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("raw event count query failed: {err}"));
            }
            Vec::new()
        }
    };
    let error_rows = match query_source_error_stats(ch, db).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("ingest error query failed: {err}"));
            }
            Vec::new()
        }
    };

    SourceStatusQueryState {
        checkpoints: checkpoint_rows
            .into_iter()
            .map(|row| (row.source_name.clone(), row))
            .collect(),
        raw_counts: raw_count_rows
            .into_iter()
            .map(|row| (row.source_name, row.count))
            .collect(),
        errors: error_rows
            .into_iter()
            .map(|row| (row.source_name.clone(), row))
            .collect(),
        query_error,
    }
}

pub fn source_health_status(
    enabled: bool,
    checkpoint_count: u64,
    raw_event_count: u64,
    ingest_error_count: u64,
    query_error: Option<&str>,
) -> SourceHealthStatus {
    if !enabled {
        SourceHealthStatus::Disabled
    } else if query_error.is_some() {
        SourceHealthStatus::Unknown
    } else if ingest_error_count > 0 && raw_event_count == 0 {
        SourceHealthStatus::Error
    } else if checkpoint_count == 0 && raw_event_count == 0 {
        SourceHealthStatus::Unknown
    } else if ingest_error_count > 0 {
        SourceHealthStatus::Warning
    } else {
        SourceHealthStatus::Ok
    }
}

/// Build a source-health snapshot aligned with the `moraine sources status` CLI semantics.
///
/// * `include_disabled` – when `false`, disabled sources are omitted from the snapshot.
///   The monitor usually passes `true` so the dashboard shows every configured source.
pub async fn build_source_status_snapshot(
    cfg: &AppConfig,
    include_disabled: bool,
) -> Result<SourceStatusSnapshot> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let state = query_source_status_state(&ch, &db).await;

    let mut sources = Vec::new();
    for source in &cfg.ingest.sources {
        if !source.enabled && !include_disabled {
            continue;
        }

        let checkpoint = state.checkpoints.get(&source.name);
        let raw_event_count = state.raw_counts.get(&source.name).copied().unwrap_or(0);
        let error = state.errors.get(&source.name);

        sources.push(build_source_status_row(
            source,
            checkpoint,
            raw_event_count,
            error,
            state.query_error.as_deref(),
        ));
    }

    Ok(SourceStatusSnapshot {
        sources,
        query_error: state.query_error,
    })
}

pub async fn build_source_detail_snapshot(
    cfg: &AppConfig,
    source_name: &str,
) -> Result<SourceDetailSnapshot> {
    let source = cfg
        .ingest
        .sources
        .iter()
        .find(|s| s.name == source_name)
        .ok_or_else(|| anyhow!("source '{}' not found in config", source_name))?;

    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let state = query_source_status_state(&ch, &db).await;
    let checkpoint = state.checkpoints.get(source_name);
    let raw_event_count = state.raw_counts.get(source_name).copied().unwrap_or(0);
    let error = state.errors.get(source_name);

    Ok(SourceDetailSnapshot {
        source: build_source_status_row(
            source,
            checkpoint,
            raw_event_count,
            error,
            state.query_error.as_deref(),
        ),
        query_error: state.query_error,
    })
}

// ---------------------------------------------------------------------------
// Deep diagnostics — files
// ---------------------------------------------------------------------------

fn glob_matches(pattern: &str) -> (Vec<String>, Option<String>) {
    let mut paths = Vec::new();
    let mut error = None;

    match glob::glob(pattern) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(path) => {
                        if path.is_file() {
                            paths.push(path.to_string_lossy().to_string());
                        }
                    }
                    Err(err) => {
                        if error.is_none() {
                            error = Some(format!("glob error: {err}"));
                        }
                    }
                }
            }
        }
        Err(err) => {
            error = Some(format!("invalid glob pattern: {err}"));
        }
    }

    paths.sort();
    (paths, error)
}

fn file_modified_at(path: &std::path::Path) -> Option<String> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    let secs = duration.as_secs() as i64;
    // Simple ISO-ish formatting without chrono dependency
    Some(format!("{secs}"))
}

fn file_size_bytes(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).ok().map(|m| m.len()).unwrap_or(0)
}

async fn query_file_checkpoint_stats(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
) -> Result<Vec<FileCheckpointRow>> {
    let query = format!(
        "SELECT \
            source_file, \
            last_offset, \
            last_line_no, \
            status, \
            toString(max(updated_at)) AS updated_at \
         FROM {db}.ingest_checkpoints FINAL \
         WHERE source_name = '{}' \
         GROUP BY source_file, last_offset, last_line_no, status",
        escape_literal(source_name)
    );
    ch.query_rows(&query, None).await
}

async fn query_file_raw_counts(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
) -> Result<Vec<FileRawCountRow>> {
    let query = format!(
        "SELECT source_file, toUInt64(count()) AS count \
         FROM {db}.raw_events \
         WHERE source_name = '{}' \
         GROUP BY source_file",
        escape_literal(source_name)
    );
    ch.query_rows(&query, None).await
}

async fn query_file_latest_errors(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
) -> Result<Vec<FileLatestErrorRow>> {
    let query = format!(
        "SELECT \
            source_file, \
            toString(max(ingested_at)) AS latest_error_at, \
            argMax(error_kind, ingested_at) AS error_kind, \
            argMax(error_text, ingested_at) AS error_text \
         FROM {db}.ingest_errors \
         WHERE source_name = '{}' \
         GROUP BY source_file",
        escape_literal(source_name)
    );
    ch.query_rows(&query, None).await
}

fn escape_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Build a per-file diagnostic snapshot for a single source.
///
/// Combines on-disk filesystem metadata (size, mtime) with ClickHouse
/// checkpoint, raw-event, and error state.
pub async fn build_source_files_snapshot(
    cfg: &AppConfig,
    source_name: &str,
) -> Result<SourceFilesSnapshot> {
    let source = cfg
        .ingest
        .sources
        .iter()
        .find(|s| s.name == source_name)
        .ok_or_else(|| anyhow!("source '{}' not found in config", source_name))?;

    let (disk_paths, fs_error) = glob_matches(&source.glob);
    let glob_match_count = disk_paths.len();

    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);

    let mut query_error = None::<String>;

    let checkpoint_rows = match query_file_checkpoint_stats(&ch, &db, source_name).await {
        Ok(rows) => rows,
        Err(err) => {
            query_error = Some(format!("checkpoint query failed: {err}"));
            Vec::new()
        }
    };
    let raw_count_rows = match query_file_raw_counts(&ch, &db, source_name).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("raw event count query failed: {err}"));
            }
            Vec::new()
        }
    };
    let latest_error_rows = match query_file_latest_errors(&ch, &db, source_name).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("error query failed: {err}"));
            }
            Vec::new()
        }
    };

    let checkpoints: BTreeMap<String, FileCheckpointRow> = checkpoint_rows
        .into_iter()
        .map(|row| (row.source_file.clone(), row))
        .collect();
    let raw_counts: BTreeMap<String, u64> = raw_count_rows
        .into_iter()
        .map(|row| (row.source_file, row.count))
        .collect();
    let latest_errors: BTreeMap<String, FileLatestErrorRow> = latest_error_rows
        .into_iter()
        .map(|row| (row.source_file.clone(), row))
        .collect();

    let mut files = Vec::new();
    for path_str in &disk_paths {
        let path = std::path::Path::new(path_str);
        let size_bytes = file_size_bytes(path);
        let modified_at = file_modified_at(path);

        let checkpoint = checkpoints.get(path_str);
        let raw_event_count = raw_counts.get(path_str).copied().unwrap_or(0);
        let latest_error = latest_errors.get(path_str);

        files.push(SourceFileRow {
            path: path_str.clone(),
            size_bytes,
            modified_at,
            checkpoint_offset: checkpoint.map(|r| r.last_offset),
            checkpoint_line_no: checkpoint.map(|r| r.last_line_no),
            checkpoint_status: checkpoint
                .map(|r| r.status.clone())
                .filter(|s| !s.is_empty()),
            checkpoint_updated_at: checkpoint
                .map(|r| r.updated_at.clone())
                .filter(|s| !s.is_empty()),
            raw_event_count,
            latest_error_at: latest_error
                .map(|r| r.latest_error_at.clone())
                .filter(|s| !s.is_empty()),
            latest_error_kind: latest_error
                .map(|r| r.error_kind.clone())
                .filter(|s| !s.is_empty()),
            latest_error_text: latest_error
                .map(|r| r.error_text.clone())
                .filter(|s| !s.is_empty()),
        });
    }

    // Also include files that have ClickHouse state but are no longer on disk
    for (path_str, checkpoint) in &checkpoints {
        if disk_paths.contains(path_str) {
            continue;
        }
        let raw_event_count = raw_counts.get(path_str).copied().unwrap_or(0);
        let latest_error = latest_errors.get(path_str);

        files.push(SourceFileRow {
            path: path_str.clone(),
            size_bytes: 0,
            modified_at: None,
            checkpoint_offset: Some(checkpoint.last_offset),
            checkpoint_line_no: Some(checkpoint.last_line_no),
            checkpoint_status: Some(checkpoint.status.clone()).filter(|s| !s.is_empty()),
            checkpoint_updated_at: Some(checkpoint.updated_at.clone()).filter(|s| !s.is_empty()),
            raw_event_count,
            latest_error_at: latest_error
                .map(|r| r.latest_error_at.clone())
                .filter(|s| !s.is_empty()),
            latest_error_kind: latest_error
                .map(|r| r.error_kind.clone())
                .filter(|s| !s.is_empty()),
            latest_error_text: latest_error
                .map(|r| r.error_text.clone())
                .filter(|s| !s.is_empty()),
        });
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(SourceFilesSnapshot {
        source_name: source_name.to_string(),
        watch_root: source.watch_root.clone(),
        glob: source.glob.clone(),
        files,
        glob_match_count,
        fs_error,
        query_error,
    })
}

// ---------------------------------------------------------------------------
// Deep diagnostics — errors
// ---------------------------------------------------------------------------

async fn query_source_errors_detail(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
    limit: u32,
) -> Result<Vec<IngestErrorDetailRow>> {
    let query = format!(
        "SELECT \
            toString(ingested_at) AS ingested_at, \
            source_file, \
            source_line_no, \
            source_offset, \
            error_kind, \
            error_text, \
            raw_fragment \
         FROM {db}.ingest_errors \
         WHERE source_name = '{}' \
         ORDER BY ingested_at DESC \
         LIMIT {}",
        escape_literal(source_name),
        limit.clamp(1, 1000)
    );
    ch.query_rows(&query, None).await
}

/// Build a recent-error snapshot for a single source.
pub async fn build_source_errors_snapshot(
    cfg: &AppConfig,
    source_name: &str,
    limit: u32,
) -> Result<SourceErrorsSnapshot> {
    // Validate source exists in config so callers get a clear 404-like error.
    let _source = cfg
        .ingest
        .sources
        .iter()
        .find(|s| s.name == source_name)
        .ok_or_else(|| anyhow!("source '{}' not found in config", source_name))?;

    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);

    let mut query_error = None;
    let errors = match query_source_errors_detail(&ch, &db, source_name, limit).await {
        Ok(rows) => rows
            .into_iter()
            .map(|row| SourceErrorRow {
                ingested_at: row.ingested_at,
                source_file: row.source_file,
                source_line_no: row.source_line_no,
                source_offset: row.source_offset,
                error_kind: row.error_kind,
                error_text: row.error_text,
                raw_fragment: row.raw_fragment,
            })
            .collect(),
        Err(err) => {
            query_error = Some(format!("error query failed: {err}"));
            Vec::new()
        }
    };

    Ok(SourceErrorsSnapshot {
        source_name: source_name.to_string(),
        errors,
        query_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use moraine_config::IngestSource;

    #[test]
    fn source_health_status_matches_cli_semantics() {
        assert_eq!(
            source_health_status(false, 0, 0, 0, None),
            SourceHealthStatus::Disabled
        );
        assert_eq!(
            source_health_status(true, 0, 0, 0, Some("query failed")),
            SourceHealthStatus::Unknown
        );
        assert_eq!(
            source_health_status(true, 0, 0, 0, None),
            SourceHealthStatus::Unknown
        );
        assert_eq!(
            source_health_status(true, 1, 42, 0, None),
            SourceHealthStatus::Ok
        );
        assert_eq!(
            source_health_status(true, 1, 42, 2, None),
            SourceHealthStatus::Warning
        );
        assert_eq!(
            source_health_status(true, 0, 0, 2, None),
            SourceHealthStatus::Error
        );
    }

    #[test]
    fn source_health_status_warns_for_errors_when_data_exists() {
        assert_eq!(
            source_health_status(true, 0, 7, 3, None),
            SourceHealthStatus::Warning
        );
    }

    #[test]
    fn glob_matches_finds_temp_files() {
        let temp = std::env::temp_dir();
        let pattern = temp
            .join("*.moraine-test-not-exists-xyz")
            .to_string_lossy()
            .to_string();
        let (paths, err) = glob_matches(&pattern);
        assert!(paths.is_empty());
        assert!(err.is_none());
    }

    #[test]
    fn file_size_bytes_returns_zero_for_missing() {
        assert_eq!(file_size_bytes(std::path::Path::new("/does/not/exist")), 0);
    }

    #[test]
    fn escape_literal_escapes_quotes() {
        assert_eq!(escape_literal("it's"), "it\\'s");
    }

    #[test]
    fn build_source_status_row_keeps_shared_metadata() {
        let source = IngestSource {
            name: "opencode".to_string(),
            harness: "opencode".to_string(),
            enabled: true,
            glob: "/tmp/opencode.db".to_string(),
            watch_root: "/tmp".to_string(),
            format: "opencode_sqlite".to_string(),
        };
        let checkpoint = SourceCheckpointStatsRow {
            source_name: source.name.clone(),
            checkpoint_count: 3,
            latest_checkpoint_at: "2026-04-20 10:15:00".to_string(),
        };
        let error = SourceErrorStatsRow {
            source_name: source.name.clone(),
            ingest_error_count: 2,
            latest_error_at: "2026-04-20 10:20:00".to_string(),
            latest_error_kind: "schema_drift".to_string(),
            latest_error_text: "missing field".to_string(),
        };

        let row = build_source_status_row(&source, Some(&checkpoint), 42, Some(&error), None);

        assert_eq!(row.status, SourceHealthStatus::Warning);
        assert_eq!(row.harness, "opencode");
        assert_eq!(row.format, "opencode_sqlite");
        assert_eq!(row.watch_root, "/tmp");
        assert_eq!(row.glob, "/tmp/opencode.db");
        assert_eq!(row.checkpoint_count, 3);
        assert_eq!(
            row.latest_checkpoint_at.as_deref(),
            Some("2026-04-20 10:15:00")
        );
        assert_eq!(row.raw_event_count, 42);
        assert_eq!(row.ingest_error_count, 2);
        assert_eq!(row.latest_error_at.as_deref(), Some("2026-04-20 10:20:00"));
        assert_eq!(row.latest_error_kind.as_deref(), Some("schema_drift"));
        assert_eq!(row.latest_error_text.as_deref(), Some("missing field"));
    }

    #[test]
    fn build_source_status_row_filters_empty_latest_metadata() {
        let source = IngestSource {
            name: "idle".to_string(),
            harness: "codex".to_string(),
            enabled: true,
            glob: "*.jsonl".to_string(),
            watch_root: "/logs".to_string(),
            format: "jsonl".to_string(),
        };
        let checkpoint = SourceCheckpointStatsRow {
            source_name: source.name.clone(),
            checkpoint_count: 0,
            latest_checkpoint_at: String::new(),
        };
        let error = SourceErrorStatsRow {
            source_name: source.name.clone(),
            ingest_error_count: 0,
            latest_error_at: String::new(),
            latest_error_kind: String::new(),
            latest_error_text: String::new(),
        };

        let row =
            build_source_status_row(&source, Some(&checkpoint), 0, Some(&error), Some("partial"));

        assert_eq!(row.status, SourceHealthStatus::Unknown);
        assert!(row.latest_checkpoint_at.is_none());
        assert!(row.latest_error_at.is_none());
        assert!(row.latest_error_kind.is_none());
        assert!(row.latest_error_text.is_none());
    }
}
