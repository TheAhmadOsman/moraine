use anyhow::{anyhow, Result};
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_config::IngestSource;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub latest_checkpoint_age_seconds: Option<u64>,
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
    pub runtime: SourceRuntimeSnapshot,
    pub runtime_query_error: Option<String>,
    pub warnings: Vec<SourceDetailWarning>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceRuntimeSnapshot {
    pub latest_heartbeat_at: Option<String>,
    pub latest_heartbeat_age_seconds: Option<u64>,
    pub queue_depth: Option<u64>,
    pub files_active: Option<u64>,
    pub files_watched: Option<u64>,
    pub append_to_visible_p50_ms: Option<u64>,
    pub append_to_visible_p95_ms: Option<u64>,
    pub watcher_backend: Option<String>,
    pub watcher_error_count: Option<u64>,
    pub watcher_reset_count: Option<u64>,
    pub watcher_last_reset_at: Option<String>,
    pub heartbeat_cadence_seconds: f64,
    pub reconcile_cadence_seconds: f64,
    pub lag_indicator: Option<SourceLagIndicator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceWarningKind {
    FileState,
    IngestHeartbeat,
    Watcher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceWarningSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceDetailWarning {
    pub kind: SourceWarningKind,
    pub severity: SourceWarningSeverity,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceLagIndicator {
    Healthy,
    Delayed,
    Stale,
    Unknown,
}

#[derive(Debug, Deserialize)]
struct SourceCheckpointStatsRow {
    source_name: String,
    checkpoint_count: u64,
    latest_checkpoint_at: String,
    latest_checkpoint_age_seconds: u64,
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

#[derive(Debug, Deserialize)]
struct SourceRuntimeRow {
    latest_heartbeat_at: String,
    latest_heartbeat_age_seconds: u64,
    queue_depth: u64,
    files_active: u64,
    files_watched: u64,
    append_to_visible_p50_ms: u64,
    append_to_visible_p95_ms: u64,
    watcher_backend: String,
    watcher_error_count: u64,
    watcher_reset_count: u64,
    watcher_last_reset_at: String,
}

#[derive(Debug, Deserialize)]
struct LegacySourceRuntimeRow {
    latest_heartbeat_at: String,
    latest_heartbeat_age_seconds: u64,
    queue_depth: u64,
    files_active: u64,
    files_watched: u64,
    append_to_visible_p50_ms: u64,
    append_to_visible_p95_ms: u64,
}

// ---------------------------------------------------------------------------
// Deep source diagnostics — per-file view
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SourceFileRow {
    pub path: String,
    pub on_disk: bool,
    pub size_bytes: u64,
    pub modified_at: Option<String>,
    pub modified_age_seconds: Option<u64>,
    pub checkpoint_offset: Option<u64>,
    pub checkpoint_line_no: Option<u64>,
    pub checkpoint_status: Option<String>,
    pub checkpoint_updated_at: Option<String>,
    pub checkpoint_age_seconds: Option<u64>,
    pub raw_event_count: u64,
    pub canonical_event_count: u64,
    pub latest_raw_event_at: Option<String>,
    pub latest_raw_event_age_seconds: Option<u64>,
    pub latest_error_at: Option<String>,
    pub latest_error_age_seconds: Option<u64>,
    pub latest_error_kind: Option<String>,
    pub latest_error_text: Option<String>,
    pub stale_reason: Option<String>,
    pub sqlite_wal_present: Option<bool>,
    pub sqlite_shm_present: Option<bool>,
    pub issues: Vec<SourceFileIssue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFileIssue {
    MissingOnDisk,
    Stale,
    Erroring,
    SqliteWalPresent,
    SqliteShmPresent,
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
    updated_age_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct FileRawStatsRow {
    source_file: String,
    count: u64,
    latest_raw_event_at: String,
    latest_raw_event_age_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct FileCanonicalStatsRow {
    source_file: String,
    count: u64,
}

#[derive(Debug, Deserialize)]
struct FileLatestErrorRow {
    source_file: String,
    latest_error_at: String,
    latest_error_age_seconds: u64,
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

// ---------------------------------------------------------------------------
// Source drift diagnostics — source/file consistency view
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDriftStatus {
    Disabled,
    Ok,
    Info,
    Warning,
    Error,
    Unknown,
}

impl SourceDriftStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Ok => "ok",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDriftSeverity {
    Info,
    Warning,
    Error,
}

impl SourceDriftSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDriftFindingKind {
    ExpectedIdle,
    PartialQuery,
    FilesystemError,
    MissingOnDisk,
    UnobservedDiskFiles,
    StaleFiles,
    CheckpointOnlyFiles,
    RawWithoutCanonical,
    CanonicalWithoutRaw,
    IngestErrors,
    SqliteSidecars,
}

impl SourceDriftFindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExpectedIdle => "expected_idle",
            Self::PartialQuery => "partial_query",
            Self::FilesystemError => "filesystem_error",
            Self::MissingOnDisk => "missing_on_disk",
            Self::UnobservedDiskFiles => "unobserved_disk_files",
            Self::StaleFiles => "stale_files",
            Self::CheckpointOnlyFiles => "checkpoint_only_files",
            Self::RawWithoutCanonical => "raw_without_canonical",
            Self::CanonicalWithoutRaw => "canonical_without_raw",
            Self::IngestErrors => "ingest_errors",
            Self::SqliteSidecars => "sqlite_sidecars",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceDriftFinding {
    pub kind: SourceDriftFindingKind,
    pub severity: SourceDriftSeverity,
    pub count: u64,
    pub summary: String,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceDriftRow {
    pub name: String,
    pub harness: String,
    pub format: String,
    pub enabled: bool,
    pub glob: String,
    pub watch_root: String,
    pub status: SourceDriftStatus,
    pub disk_file_count: u64,
    pub checkpoint_file_count: u64,
    pub raw_file_count: u64,
    pub canonical_file_count: u64,
    pub ingest_error_file_count: u64,
    pub missing_on_disk_file_count: u64,
    pub unobserved_disk_file_count: u64,
    pub stale_file_count: u64,
    pub checkpoint_only_file_count: u64,
    pub raw_without_canonical_file_count: u64,
    pub canonical_without_raw_file_count: u64,
    pub sqlite_sidecar_file_count: u64,
    pub checkpoint_count: u64,
    pub raw_event_count: u64,
    pub canonical_event_count: u64,
    pub ingest_error_count: u64,
    pub findings: Vec<SourceDriftFinding>,
    pub fs_error: Option<String>,
    pub query_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceDriftSnapshot {
    pub generated_unix_seconds: u64,
    pub sources: Vec<SourceDriftRow>,
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
            toString(max(updated_at)) AS latest_checkpoint_at, \
            toUInt64(greatest(dateDiff('second', max(updated_at), now()), 0)) AS latest_checkpoint_age_seconds \
         FROM {db}.ingest_checkpoints FINAL \
         GROUP BY source_name"
    );
    ch.query_rows(&query, None).await
}

async fn query_source_runtime(ch: &ClickHouseClient, db: &str) -> Result<Option<SourceRuntimeRow>> {
    let query = format!(
        "SELECT \
            toString(max(ts)) AS latest_heartbeat_at, \
            toUInt64(greatest(dateDiff('second', max(ts), now()), 0)) AS latest_heartbeat_age_seconds, \
            toUInt64(argMax(queue_depth, ts)) AS queue_depth, \
            toUInt64(argMax(files_active, ts)) AS files_active, \
            toUInt64(argMax(files_watched, ts)) AS files_watched, \
            toUInt64(argMax(append_to_visible_p50_ms, ts)) AS append_to_visible_p50_ms, \
            toUInt64(argMax(append_to_visible_p95_ms, ts)) AS append_to_visible_p95_ms, \
            toString(argMax(watcher_backend, ts)) AS watcher_backend, \
            toUInt64(argMax(watcher_error_count, ts)) AS watcher_error_count, \
            toUInt64(argMax(watcher_reset_count, ts)) AS watcher_reset_count, \
            if(toUInt64(argMax(watcher_last_reset_unix_ms, ts)) = 0, '', toString(fromUnixTimestamp64Milli(toInt64(argMax(watcher_last_reset_unix_ms, ts))))) AS watcher_last_reset_at \
         FROM {db}.ingest_heartbeats"
    );

    match ch.query_rows(&query, None).await {
        Ok(rows) => Ok(rows.into_iter().next()),
        Err(_) => {
            let legacy_query = format!(
                "SELECT \
                    toString(max(ts)) AS latest_heartbeat_at, \
                    toUInt64(greatest(dateDiff('second', max(ts), now()), 0)) AS latest_heartbeat_age_seconds, \
                    toUInt64(argMax(queue_depth, ts)) AS queue_depth, \
                    toUInt64(argMax(files_active, ts)) AS files_active, \
                    toUInt64(argMax(files_watched, ts)) AS files_watched, \
                    toUInt64(argMax(append_to_visible_p50_ms, ts)) AS append_to_visible_p50_ms, \
                    toUInt64(argMax(append_to_visible_p95_ms, ts)) AS append_to_visible_p95_ms \
                 FROM {db}.ingest_heartbeats"
            );
            let rows: Vec<LegacySourceRuntimeRow> = ch.query_rows(&legacy_query, None).await?;
            Ok(rows.into_iter().next().map(|row| SourceRuntimeRow {
                latest_heartbeat_at: row.latest_heartbeat_at,
                latest_heartbeat_age_seconds: row.latest_heartbeat_age_seconds,
                queue_depth: row.queue_depth,
                files_active: row.files_active,
                files_watched: row.files_watched,
                append_to_visible_p50_ms: row.append_to_visible_p50_ms,
                append_to_visible_p95_ms: row.append_to_visible_p95_ms,
                watcher_backend: "unknown".to_string(),
                watcher_error_count: 0,
                watcher_reset_count: 0,
                watcher_last_reset_at: String::new(),
            }))
        }
    }
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
        latest_checkpoint_age_seconds: checkpoint.map(|row| row.latest_checkpoint_age_seconds),
        raw_event_count,
        ingest_error_count,
        latest_error_at: error.and_then(|row| option_if_non_empty(&row.latest_error_at)),
        latest_error_kind: error.and_then(|row| option_if_non_empty(&row.latest_error_kind)),
        latest_error_text: error.and_then(|row| option_if_non_empty(&row.latest_error_text)),
    }
}

fn sanitize_cadence(seconds: f64, minimum_seconds: f64) -> f64 {
    if seconds.is_finite() {
        seconds.max(minimum_seconds)
    } else {
        minimum_seconds
    }
}

fn runtime_lag_indicator(runtime: &SourceRuntimeSnapshot) -> Option<SourceLagIndicator> {
    let age = runtime.latest_heartbeat_age_seconds? as f64;
    let delayed_after = runtime.heartbeat_cadence_seconds * 3.0;
    let stale_after = runtime.heartbeat_cadence_seconds * 6.0;

    Some(if age >= stale_after {
        SourceLagIndicator::Stale
    } else if age >= delayed_after || runtime.queue_depth.unwrap_or(0) > 0 {
        SourceLagIndicator::Delayed
    } else {
        SourceLagIndicator::Healthy
    })
}

fn build_source_runtime_snapshot(
    cfg: &AppConfig,
    runtime: Option<SourceRuntimeRow>,
) -> SourceRuntimeSnapshot {
    let heartbeat_cadence_seconds = sanitize_cadence(cfg.ingest.heartbeat_interval_seconds, 1.0);
    let reconcile_cadence_seconds = sanitize_cadence(cfg.ingest.reconcile_interval_seconds, 5.0);

    let mut snapshot = if let Some(runtime) = runtime {
        SourceRuntimeSnapshot {
            latest_heartbeat_at: option_if_non_empty(&runtime.latest_heartbeat_at),
            latest_heartbeat_age_seconds: Some(runtime.latest_heartbeat_age_seconds),
            queue_depth: Some(runtime.queue_depth),
            files_active: Some(runtime.files_active),
            files_watched: Some(runtime.files_watched),
            append_to_visible_p50_ms: Some(runtime.append_to_visible_p50_ms),
            append_to_visible_p95_ms: Some(runtime.append_to_visible_p95_ms),
            watcher_backend: option_if_non_empty(&runtime.watcher_backend),
            watcher_error_count: Some(runtime.watcher_error_count),
            watcher_reset_count: Some(runtime.watcher_reset_count),
            watcher_last_reset_at: option_if_non_empty(&runtime.watcher_last_reset_at),
            heartbeat_cadence_seconds,
            reconcile_cadence_seconds,
            lag_indicator: None,
        }
    } else {
        SourceRuntimeSnapshot {
            latest_heartbeat_at: None,
            latest_heartbeat_age_seconds: None,
            queue_depth: None,
            files_active: None,
            files_watched: None,
            append_to_visible_p50_ms: None,
            append_to_visible_p95_ms: None,
            watcher_backend: None,
            watcher_error_count: None,
            watcher_reset_count: None,
            watcher_last_reset_at: None,
            heartbeat_cadence_seconds,
            reconcile_cadence_seconds,
            lag_indicator: Some(SourceLagIndicator::Unknown),
        }
    };

    if snapshot.latest_heartbeat_at.is_some() {
        snapshot.lag_indicator = runtime_lag_indicator(&snapshot);
    }

    snapshot
}

fn build_source_detail_warnings(
    source: &SourceStatusRow,
    runtime: &SourceRuntimeSnapshot,
    query_error: Option<&str>,
    runtime_query_error: Option<&str>,
) -> Vec<SourceDetailWarning> {
    let mut warnings = Vec::new();

    if query_error.is_none() {
        match source.status {
            SourceHealthStatus::Error => warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::FileState,
                severity: SourceWarningSeverity::Error,
                summary: "Ingest errors are landing, but this source has not produced usable raw rows yet.".to_string(),
            }),
            SourceHealthStatus::Warning => warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::FileState,
                severity: SourceWarningSeverity::Warning,
                summary: "This source is ingesting data, but recent file processing also recorded ingest errors.".to_string(),
            }),
            SourceHealthStatus::Unknown if source.enabled && source.checkpoint_count == 0 && source.raw_event_count == 0 => {
                warnings.push(SourceDetailWarning {
                    kind: SourceWarningKind::FileState,
                    severity: SourceWarningSeverity::Warning,
                    summary: "No checkpoints or raw rows have been recorded for this source yet.".to_string(),
                });
            }
            _ => {}
        }
    }

    if runtime_query_error.is_none() {
        match runtime.lag_indicator {
            Some(SourceLagIndicator::Stale) => warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::IngestHeartbeat,
                severity: SourceWarningSeverity::Error,
                summary: format!(
                    "Latest ingest heartbeat is {}s old, which is stale for the configured {:.0}s cadence.",
                    runtime.latest_heartbeat_age_seconds.unwrap_or_default(),
                    runtime.heartbeat_cadence_seconds
                ),
            }),
            Some(SourceLagIndicator::Delayed) => warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::IngestHeartbeat,
                severity: SourceWarningSeverity::Warning,
                summary: if runtime.queue_depth.unwrap_or(0) > 0 {
                    format!(
                        "Ingest runtime is carrying a queue depth of {} and may be delayed.",
                        runtime.queue_depth.unwrap_or_default()
                    )
                } else {
                    format!(
                        "Latest ingest heartbeat is {}s old, which is slower than the configured {:.0}s cadence.",
                        runtime.latest_heartbeat_age_seconds.unwrap_or_default(),
                        runtime.heartbeat_cadence_seconds
                    )
                },
            }),
            Some(SourceLagIndicator::Unknown) => warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::IngestHeartbeat,
                severity: SourceWarningSeverity::Warning,
                summary: "No ingest heartbeat has been recorded yet, so runtime lag is unknown.".to_string(),
            }),
            _ => {}
        }

        if runtime.watcher_error_count.unwrap_or(0) > 0
            || runtime.watcher_reset_count.unwrap_or(0) > 0
        {
            warnings.push(SourceDetailWarning {
                kind: SourceWarningKind::Watcher,
                severity: SourceWarningSeverity::Warning,
                summary: format!(
                    "Watcher backend={} with {} errors and {} rescans/resets observed in heartbeat state.",
                    runtime.watcher_backend.as_deref().unwrap_or("unknown"),
                    runtime.watcher_error_count.unwrap_or_default(),
                    runtime.watcher_reset_count.unwrap_or_default()
                ),
            });
        }
    }

    warnings
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
    let (runtime_row, runtime_query_error) = match query_source_runtime(&ch, &db).await {
        Ok(row) => (row, None),
        Err(err) => (None, Some(format!("heartbeat query failed: {err}"))),
    };
    let checkpoint = state.checkpoints.get(source_name);
    let raw_event_count = state.raw_counts.get(source_name).copied().unwrap_or(0);
    let error = state.errors.get(source_name);
    let source = build_source_status_row(
        source,
        checkpoint,
        raw_event_count,
        error,
        state.query_error.as_deref(),
    );
    let runtime = build_source_runtime_snapshot(cfg, runtime_row);
    let warnings = build_source_detail_warnings(
        &source,
        &runtime,
        state.query_error.as_deref(),
        runtime_query_error.as_deref(),
    );

    Ok(SourceDetailSnapshot {
        source,
        query_error: state.query_error,
        runtime,
        runtime_query_error,
        warnings,
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

fn file_modified_age_seconds(path: &std::path::Path, now_unix_seconds: u64) -> Option<u64> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    now_unix_seconds.checked_sub(duration.as_secs())
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
            toString(checkpoint_updated_at) AS updated_at, \
            toUInt64(greatest(dateDiff('second', checkpoint_updated_at, now()), 0)) AS updated_age_seconds \
         FROM ( \
            SELECT \
                source_file, \
                toUInt64(argMax(last_offset, updated_at)) AS last_offset, \
                toUInt64(argMax(last_line_no, updated_at)) AS last_line_no, \
                argMax(status, updated_at) AS status, \
                max(updated_at) AS checkpoint_updated_at \
             FROM {db}.ingest_checkpoints FINAL \
             WHERE source_name = '{}' \
             GROUP BY source_file \
         )",
        escape_literal(source_name)
    );
    ch.query_rows(&query, None).await
}

async fn query_file_raw_stats(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
) -> Result<Vec<FileRawStatsRow>> {
    let query = format!(
        "SELECT \
            source_file, \
            toUInt64(count()) AS count, \
            toString(max(ingested_at)) AS latest_raw_event_at, \
            toUInt64(greatest(dateDiff('second', max(ingested_at), now()), 0)) AS latest_raw_event_age_seconds \
         FROM {db}.raw_events \
         WHERE source_name = '{}' \
         GROUP BY source_file",
        escape_literal(source_name)
    );
    ch.query_rows(&query, None).await
}

async fn query_file_canonical_stats(
    ch: &ClickHouseClient,
    db: &str,
    source_name: &str,
) -> Result<Vec<FileCanonicalStatsRow>> {
    let query = format!(
        "SELECT \
            source_file, \
            toUInt64(count()) AS count \
         FROM {db}.events FINAL \
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
            toUInt64(greatest(dateDiff('second', max(ingested_at), now()), 0)) AS latest_error_age_seconds, \
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

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn latest_observed_progress_unix(
    now_unix_seconds: u64,
    checkpoint: Option<&FileCheckpointRow>,
    raw_stats: Option<&FileRawStatsRow>,
    latest_error: Option<&FileLatestErrorRow>,
) -> Option<u64> {
    [
        checkpoint.and_then(|row| now_unix_seconds.checked_sub(row.updated_age_seconds)),
        raw_stats.and_then(|row| now_unix_seconds.checked_sub(row.latest_raw_event_age_seconds)),
        latest_error.and_then(|row| now_unix_seconds.checked_sub(row.latest_error_age_seconds)),
    ]
    .into_iter()
    .flatten()
    .max()
}

fn stale_threshold_seconds(cfg: &AppConfig) -> u64 {
    cfg.ingest
        .reconcile_interval_seconds
        .max(cfg.ingest.heartbeat_interval_seconds * 2.0)
        .ceil()
        .max(30.0) as u64
}

fn sqlite_sidecar_flags(source: &IngestSource, path: &str) -> (Option<bool>, Option<bool>) {
    if source.format != "opencode_sqlite" || !path.ends_with(".db") {
        return (None, None);
    }

    let wal = std::path::PathBuf::from(format!("{path}-wal")).exists();
    let shm = std::path::PathBuf::from(format!("{path}-shm")).exists();
    (Some(wal), Some(shm))
}

fn stale_reason(
    modified_age_seconds: Option<u64>,
    observed_progress_unix: Option<u64>,
    now_unix_seconds: u64,
    stale_threshold_seconds: u64,
) -> Option<String> {
    let modified_age_seconds = modified_age_seconds?;
    let modified_unix = now_unix_seconds.checked_sub(modified_age_seconds)?;

    match observed_progress_unix {
        Some(progress_unix) if modified_unix > progress_unix.saturating_add(stale_threshold_seconds) => {
            Some(format!(
                "disk writes are newer than the latest observed ingest progress by at least {}s",
                modified_unix.saturating_sub(progress_unix)
            ))
        }
        None if modified_age_seconds >= stale_threshold_seconds => Some(format!(
            "file matches the source glob but has no checkpoint, raw rows, or ingest errors after {}s",
            modified_age_seconds
        )),
        _ => None,
    }
}

fn build_source_file_row(
    cfg: &AppConfig,
    source: &IngestSource,
    path_str: &str,
    on_disk: bool,
    checkpoint: Option<&FileCheckpointRow>,
    raw_stats: Option<&FileRawStatsRow>,
    canonical_stats: Option<&FileCanonicalStatsRow>,
    latest_error: Option<&FileLatestErrorRow>,
    now_unix_seconds: u64,
) -> SourceFileRow {
    let path = std::path::Path::new(path_str);
    let size_bytes = if on_disk { file_size_bytes(path) } else { 0 };
    let modified_at = if on_disk {
        file_modified_at(path)
    } else {
        None
    };
    let modified_age_seconds = if on_disk {
        file_modified_age_seconds(path, now_unix_seconds)
    } else {
        None
    };
    let observed_progress_unix =
        latest_observed_progress_unix(now_unix_seconds, checkpoint, raw_stats, latest_error);
    let stale_reason = stale_reason(
        modified_age_seconds,
        observed_progress_unix,
        now_unix_seconds,
        stale_threshold_seconds(cfg),
    );
    let (sqlite_wal_present, sqlite_shm_present) = sqlite_sidecar_flags(source, path_str);

    let mut issues = Vec::new();
    if !on_disk {
        issues.push(SourceFileIssue::MissingOnDisk);
    }
    if stale_reason.is_some() {
        issues.push(SourceFileIssue::Stale);
    }
    if latest_error.is_some() {
        issues.push(SourceFileIssue::Erroring);
    }
    if sqlite_wal_present == Some(true) {
        issues.push(SourceFileIssue::SqliteWalPresent);
    }
    if sqlite_shm_present == Some(true) {
        issues.push(SourceFileIssue::SqliteShmPresent);
    }

    SourceFileRow {
        path: path_str.to_string(),
        on_disk,
        size_bytes,
        modified_at,
        modified_age_seconds,
        checkpoint_offset: checkpoint.map(|row| row.last_offset),
        checkpoint_line_no: checkpoint.map(|row| row.last_line_no),
        checkpoint_status: checkpoint
            .map(|row| row.status.clone())
            .filter(|value| !value.is_empty()),
        checkpoint_updated_at: checkpoint
            .map(|row| row.updated_at.clone())
            .filter(|value| !value.is_empty()),
        checkpoint_age_seconds: checkpoint.map(|row| row.updated_age_seconds),
        raw_event_count: raw_stats.map(|row| row.count).unwrap_or(0),
        canonical_event_count: canonical_stats.map(|row| row.count).unwrap_or(0),
        latest_raw_event_at: raw_stats
            .map(|row| row.latest_raw_event_at.clone())
            .filter(|value| !value.is_empty()),
        latest_raw_event_age_seconds: raw_stats.map(|row| row.latest_raw_event_age_seconds),
        latest_error_at: latest_error
            .map(|row| row.latest_error_at.clone())
            .filter(|value| !value.is_empty()),
        latest_error_age_seconds: latest_error.map(|row| row.latest_error_age_seconds),
        latest_error_kind: latest_error
            .map(|row| row.error_kind.clone())
            .filter(|value| !value.is_empty()),
        latest_error_text: latest_error
            .map(|row| row.error_text.clone())
            .filter(|value| !value.is_empty()),
        stale_reason,
        sqlite_wal_present,
        sqlite_shm_present,
        issues,
    }
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
    let disk_path_set: BTreeSet<String> = disk_paths.iter().cloned().collect();
    let glob_match_count = disk_paths.len();
    let now_unix_seconds = now_unix_seconds();

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
    let raw_stat_rows = match query_file_raw_stats(&ch, &db, source_name).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("raw event count query failed: {err}"));
            }
            Vec::new()
        }
    };
    let canonical_stat_rows = match query_file_canonical_stats(&ch, &db, source_name).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("canonical event count query failed: {err}"));
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
    let raw_stats: BTreeMap<String, FileRawStatsRow> = raw_stat_rows
        .into_iter()
        .map(|row| (row.source_file.clone(), row))
        .collect();
    let canonical_stats: BTreeMap<String, FileCanonicalStatsRow> = canonical_stat_rows
        .into_iter()
        .map(|row| (row.source_file.clone(), row))
        .collect();
    let latest_errors: BTreeMap<String, FileLatestErrorRow> = latest_error_rows
        .into_iter()
        .map(|row| (row.source_file.clone(), row))
        .collect();

    let mut paths = disk_path_set.clone();
    paths.extend(checkpoints.keys().cloned());
    paths.extend(raw_stats.keys().cloned());
    paths.extend(canonical_stats.keys().cloned());
    paths.extend(latest_errors.keys().cloned());

    let mut files = Vec::new();
    for path_str in paths {
        files.push(build_source_file_row(
            cfg,
            source,
            &path_str,
            disk_path_set.contains(&path_str),
            checkpoints.get(&path_str),
            raw_stats.get(&path_str),
            canonical_stats.get(&path_str),
            latest_errors.get(&path_str),
            now_unix_seconds,
        ));
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
// Deep diagnostics — drift
// ---------------------------------------------------------------------------

fn source_has_checkpoint(file: &SourceFileRow) -> bool {
    file.checkpoint_offset.is_some()
        || file.checkpoint_line_no.is_some()
        || file.checkpoint_status.is_some()
        || file.checkpoint_updated_at.is_some()
}

fn source_file_examples(
    files: &[SourceFileRow],
    predicate: impl Fn(&SourceFileRow) -> bool,
) -> Vec<String> {
    files
        .iter()
        .filter(|file| predicate(file))
        .take(3)
        .map(|file| file.path.clone())
        .collect()
}

fn source_file_count(files: &[SourceFileRow], predicate: impl Fn(&SourceFileRow) -> bool) -> u64 {
    files.iter().filter(|file| predicate(file)).count() as u64
}

fn push_drift_finding(
    findings: &mut Vec<SourceDriftFinding>,
    kind: SourceDriftFindingKind,
    severity: SourceDriftSeverity,
    count: u64,
    summary: impl Into<String>,
    examples: Vec<String>,
) {
    if count == 0 {
        return;
    }

    findings.push(SourceDriftFinding {
        kind,
        severity,
        count,
        summary: summary.into(),
        examples,
    });
}

fn drift_status(
    enabled: bool,
    findings: &[SourceDriftFinding],
    query_error: Option<&str>,
) -> SourceDriftStatus {
    if !enabled {
        SourceDriftStatus::Disabled
    } else if query_error.is_some() {
        SourceDriftStatus::Unknown
    } else if findings
        .iter()
        .any(|finding| finding.severity == SourceDriftSeverity::Error)
    {
        SourceDriftStatus::Error
    } else if findings
        .iter()
        .any(|finding| finding.severity == SourceDriftSeverity::Warning)
    {
        SourceDriftStatus::Warning
    } else if findings
        .iter()
        .any(|finding| finding.severity == SourceDriftSeverity::Info)
    {
        SourceDriftStatus::Info
    } else {
        SourceDriftStatus::Ok
    }
}

fn build_source_drift_row(
    source: &IngestSource,
    status: Option<&SourceStatusRow>,
    status_query_error: Option<&str>,
    files_snapshot: SourceFilesSnapshot,
) -> SourceDriftRow {
    let query_error = files_snapshot
        .query_error
        .clone()
        .or_else(|| status_query_error.map(str::to_string));
    let files = files_snapshot.files;

    let disk_file_count = source_file_count(&files, |file| file.on_disk);
    let checkpoint_file_count = source_file_count(&files, source_has_checkpoint);
    let raw_file_count = source_file_count(&files, |file| file.raw_event_count > 0);
    let canonical_file_count = source_file_count(&files, |file| file.canonical_event_count > 0);
    let ingest_error_file_count = source_file_count(&files, |file| file.latest_error_at.is_some());
    let missing_on_disk_file_count = source_file_count(&files, |file| {
        file.issues.contains(&SourceFileIssue::MissingOnDisk)
    });
    let unobserved_disk_file_count = source_file_count(&files, |file| {
        file.on_disk
            && !source_has_checkpoint(file)
            && file.raw_event_count == 0
            && file.canonical_event_count == 0
            && file.latest_error_at.is_none()
    });
    let stale_file_count =
        source_file_count(&files, |file| file.issues.contains(&SourceFileIssue::Stale));
    let checkpoint_only_file_count = source_file_count(&files, |file| {
        source_has_checkpoint(file)
            && file.raw_event_count == 0
            && file.canonical_event_count == 0
            && file.latest_error_at.is_none()
    });
    let raw_without_canonical_file_count = source_file_count(&files, |file| {
        file.raw_event_count > 0 && file.canonical_event_count == 0
    });
    let canonical_without_raw_file_count = source_file_count(&files, |file| {
        file.canonical_event_count > 0 && file.raw_event_count == 0
    });
    let sqlite_sidecar_file_count = source_file_count(&files, |file| {
        file.sqlite_wal_present == Some(true) || file.sqlite_shm_present == Some(true)
    });

    let raw_event_count = status
        .map(|row| row.raw_event_count)
        .unwrap_or_else(|| files.iter().map(|file| file.raw_event_count).sum());
    let canonical_event_count = files.iter().map(|file| file.canonical_event_count).sum();
    let checkpoint_count = status
        .map(|row| row.checkpoint_count)
        .unwrap_or(checkpoint_file_count);
    let ingest_error_count = status
        .map(|row| row.ingest_error_count)
        .unwrap_or(ingest_error_file_count);

    let mut findings = Vec::new();
    if let Some(error) = &query_error {
        push_drift_finding(
            &mut findings,
            SourceDriftFindingKind::PartialQuery,
            SourceDriftSeverity::Warning,
            1,
            format!("ClickHouse source drift query returned partial results: {error}"),
            Vec::new(),
        );
    }
    if let Some(error) = &files_snapshot.fs_error {
        push_drift_finding(
            &mut findings,
            SourceDriftFindingKind::FilesystemError,
            SourceDriftSeverity::Warning,
            1,
            format!("Configured source glob could not be fully scanned: {error}"),
            Vec::new(),
        );
    }

    if source.enabled
        && disk_file_count == 0
        && checkpoint_count == 0
        && raw_event_count == 0
        && canonical_event_count == 0
        && ingest_error_count == 0
        && query_error.is_none()
        && files_snapshot.fs_error.is_none()
    {
        push_drift_finding(
            &mut findings,
            SourceDriftFindingKind::ExpectedIdle,
            SourceDriftSeverity::Info,
            1,
            "No files currently match this source and no ingest state has been recorded.",
            Vec::new(),
        );
    }

    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::MissingOnDisk,
        SourceDriftSeverity::Warning,
        missing_on_disk_file_count,
        "Files have checkpoint/raw/canonical/error state but no longer exist on disk.",
        source_file_examples(&files, |file| {
            file.issues.contains(&SourceFileIssue::MissingOnDisk)
        }),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::UnobservedDiskFiles,
        SourceDriftSeverity::Info,
        unobserved_disk_file_count,
        "Files match the configured glob but have no checkpoint, raw rows, canonical events, or ingest errors yet.",
        source_file_examples(&files, |file| {
            file.on_disk
                && !source_has_checkpoint(file)
                && file.raw_event_count == 0
                && file.canonical_event_count == 0
                && file.latest_error_at.is_none()
        }),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::StaleFiles,
        SourceDriftSeverity::Warning,
        stale_file_count,
        "Files appear newer on disk than the latest observed ingest progress.",
        source_file_examples(&files, |file| file.issues.contains(&SourceFileIssue::Stale)),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::CheckpointOnlyFiles,
        SourceDriftSeverity::Warning,
        checkpoint_only_file_count,
        "Files have checkpoint state but no raw rows, canonical events, or ingest errors.",
        source_file_examples(&files, |file| {
            source_has_checkpoint(file)
                && file.raw_event_count == 0
                && file.canonical_event_count == 0
                && file.latest_error_at.is_none()
        }),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::RawWithoutCanonical,
        SourceDriftSeverity::Error,
        raw_without_canonical_file_count,
        "Files have raw rows but no canonical events, indicating unnormalized raw input.",
        source_file_examples(&files, |file| {
            file.raw_event_count > 0 && file.canonical_event_count == 0
        }),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::CanonicalWithoutRaw,
        SourceDriftSeverity::Error,
        canonical_without_raw_file_count,
        "Files have canonical events but no raw rows, indicating missing raw backing rows.",
        source_file_examples(&files, |file| {
            file.canonical_event_count > 0 && file.raw_event_count == 0
        }),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::IngestErrors,
        if raw_event_count == 0 && canonical_event_count == 0 {
            SourceDriftSeverity::Error
        } else {
            SourceDriftSeverity::Warning
        },
        ingest_error_count,
        "Ingest errors are recorded for this source.",
        source_file_examples(&files, |file| file.latest_error_at.is_some()),
    );
    push_drift_finding(
        &mut findings,
        SourceDriftFindingKind::SqliteSidecars,
        SourceDriftSeverity::Info,
        sqlite_sidecar_file_count,
        "SQLite WAL/SHM sidecars are visible next to this source database.",
        source_file_examples(&files, |file| {
            file.sqlite_wal_present == Some(true) || file.sqlite_shm_present == Some(true)
        }),
    );

    SourceDriftRow {
        name: source.name.clone(),
        harness: source.harness.clone(),
        format: source.format.clone(),
        enabled: source.enabled,
        glob: source.glob.clone(),
        watch_root: source.watch_root.clone(),
        status: drift_status(source.enabled, &findings, query_error.as_deref()),
        disk_file_count,
        checkpoint_file_count,
        raw_file_count,
        canonical_file_count,
        ingest_error_file_count,
        missing_on_disk_file_count,
        unobserved_disk_file_count,
        stale_file_count,
        checkpoint_only_file_count,
        raw_without_canonical_file_count,
        canonical_without_raw_file_count,
        sqlite_sidecar_file_count,
        checkpoint_count,
        raw_event_count,
        canonical_event_count,
        ingest_error_count,
        findings,
        fs_error: files_snapshot.fs_error,
        query_error,
    }
}

/// Build a drift snapshot that compares configured sources against on-disk files
/// and ClickHouse ingest/canonical state.
pub async fn build_source_drift_snapshot(
    cfg: &AppConfig,
    include_disabled: bool,
) -> Result<SourceDriftSnapshot> {
    let status_snapshot = build_source_status_snapshot(cfg, true).await?;
    let status_query_error = status_snapshot.query_error.clone();
    let status_rows: BTreeMap<String, SourceStatusRow> = status_snapshot
        .sources
        .into_iter()
        .map(|row| (row.name.clone(), row))
        .collect();

    let mut rows = Vec::new();
    for source in &cfg.ingest.sources {
        if !source.enabled && !include_disabled {
            continue;
        }

        let files_snapshot = build_source_files_snapshot(cfg, &source.name).await?;
        rows.push(build_source_drift_row(
            source,
            status_rows.get(&source.name),
            status_query_error.as_deref(),
            files_snapshot,
        ));
    }

    Ok(SourceDriftSnapshot {
        generated_unix_seconds: now_unix_seconds(),
        sources: rows,
        query_error: status_query_error,
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

    fn test_source_file(
        path: &str,
        on_disk: bool,
        checkpoint: bool,
        raw_event_count: u64,
        canonical_event_count: u64,
        error: bool,
        issues: Vec<SourceFileIssue>,
    ) -> SourceFileRow {
        SourceFileRow {
            path: path.to_string(),
            on_disk,
            size_bytes: if on_disk { 42 } else { 0 },
            modified_at: if on_disk { Some("1".to_string()) } else { None },
            modified_age_seconds: if on_disk { Some(1) } else { None },
            checkpoint_offset: checkpoint.then_some(10),
            checkpoint_line_no: checkpoint.then_some(2),
            checkpoint_status: checkpoint.then(|| "ok".to_string()),
            checkpoint_updated_at: checkpoint.then(|| "2".to_string()),
            checkpoint_age_seconds: checkpoint.then_some(2),
            raw_event_count,
            canonical_event_count,
            latest_raw_event_at: (raw_event_count > 0).then(|| "3".to_string()),
            latest_raw_event_age_seconds: (raw_event_count > 0).then_some(3),
            latest_error_at: error.then(|| "4".to_string()),
            latest_error_age_seconds: error.then_some(4),
            latest_error_kind: error.then(|| "parse".to_string()),
            latest_error_text: error.then(|| "bad row".to_string()),
            stale_reason: issues
                .contains(&SourceFileIssue::Stale)
                .then(|| "stale".to_string()),
            sqlite_wal_present: None,
            sqlite_shm_present: None,
            issues,
        }
    }

    fn test_source(name: &str, enabled: bool) -> IngestSource {
        IngestSource {
            name: name.to_string(),
            harness: "codex".to_string(),
            enabled,
            glob: "/tmp/codex/**/*.jsonl".to_string(),
            watch_root: "/tmp/codex".to_string(),
            format: "jsonl".to_string(),
        }
    }

    #[test]
    fn build_source_drift_row_classifies_file_state_mismatches() {
        let source = test_source("codex", true);
        let status = SourceStatusRow {
            name: source.name.clone(),
            harness: source.harness.clone(),
            format: source.format.clone(),
            enabled: true,
            glob: source.glob.clone(),
            watch_root: source.watch_root.clone(),
            status: SourceHealthStatus::Warning,
            checkpoint_count: 4,
            latest_checkpoint_at: Some("2026-04-20 10:15:00".to_string()),
            latest_checkpoint_age_seconds: Some(30),
            raw_event_count: 3,
            ingest_error_count: 1,
            latest_error_at: Some("2026-04-20 10:16:00".to_string()),
            latest_error_kind: Some("parse".to_string()),
            latest_error_text: Some("bad row".to_string()),
        };
        let snapshot = SourceFilesSnapshot {
            source_name: "codex".to_string(),
            watch_root: source.watch_root.clone(),
            glob: source.glob.clone(),
            files: vec![
                test_source_file(
                    "/tmp/codex/raw-only.jsonl",
                    true,
                    true,
                    3,
                    0,
                    false,
                    Vec::new(),
                ),
                test_source_file(
                    "/tmp/codex/canonical-only.jsonl",
                    true,
                    true,
                    0,
                    2,
                    false,
                    Vec::new(),
                ),
                test_source_file(
                    "/tmp/codex/checkpoint-only.jsonl",
                    true,
                    true,
                    0,
                    0,
                    false,
                    Vec::new(),
                ),
                test_source_file(
                    "/tmp/codex/missing.jsonl",
                    false,
                    true,
                    1,
                    1,
                    false,
                    vec![SourceFileIssue::MissingOnDisk],
                ),
                test_source_file(
                    "/tmp/codex/error.jsonl",
                    true,
                    false,
                    0,
                    0,
                    true,
                    vec![SourceFileIssue::Erroring],
                ),
            ],
            glob_match_count: 4,
            fs_error: None,
            query_error: None,
        };

        let row = build_source_drift_row(&source, Some(&status), None, snapshot);

        assert_eq!(row.status, SourceDriftStatus::Error);
        assert_eq!(row.raw_without_canonical_file_count, 1);
        assert_eq!(row.canonical_without_raw_file_count, 1);
        assert_eq!(row.checkpoint_only_file_count, 1);
        assert_eq!(row.missing_on_disk_file_count, 1);
        assert_eq!(row.ingest_error_count, 1);
        assert!(row
            .findings
            .iter()
            .any(|finding| finding.kind == SourceDriftFindingKind::RawWithoutCanonical));
        assert!(row
            .findings
            .iter()
            .any(|finding| finding.kind == SourceDriftFindingKind::CanonicalWithoutRaw));
        assert!(row
            .findings
            .iter()
            .any(|finding| finding.kind == SourceDriftFindingKind::IngestErrors));
    }

    #[test]
    fn build_source_drift_row_marks_expected_idle() {
        let source = test_source("idle", true);
        let snapshot = SourceFilesSnapshot {
            source_name: "idle".to_string(),
            watch_root: source.watch_root.clone(),
            glob: source.glob.clone(),
            files: Vec::new(),
            glob_match_count: 0,
            fs_error: None,
            query_error: None,
        };

        let row = build_source_drift_row(&source, None, None, snapshot);

        assert_eq!(row.status, SourceDriftStatus::Info);
        assert_eq!(row.findings.len(), 1);
        assert_eq!(row.findings[0].kind, SourceDriftFindingKind::ExpectedIdle);
    }

    #[test]
    fn build_source_file_row_marks_missing_erroring_and_sqlite_sidecars() {
        let mut cfg = moraine_config::AppConfig::default();
        cfg.ingest.reconcile_interval_seconds = 30.0;
        cfg.ingest.heartbeat_interval_seconds = 5.0;

        let root = std::env::temp_dir().join(format!(
            "moraine-source-file-row-{}-{}",
            std::process::id(),
            now_unix_seconds()
        ));
        std::fs::create_dir_all(&root).expect("create temp dir");
        let db_path = root.join("opencode.db");
        std::fs::write(&db_path, b"sqlite").expect("write db");
        std::fs::write(root.join("opencode.db-wal"), b"wal").expect("write wal");
        std::fs::write(root.join("opencode.db-shm"), b"shm").expect("write shm");

        let source = IngestSource {
            name: "opencode".to_string(),
            harness: "opencode".to_string(),
            enabled: true,
            glob: db_path.to_string_lossy().to_string(),
            watch_root: root.to_string_lossy().to_string(),
            format: "opencode_sqlite".to_string(),
        };
        let latest_error = FileLatestErrorRow {
            source_file: db_path.to_string_lossy().to_string(),
            latest_error_at: "2026-04-20 10:20:00".to_string(),
            latest_error_age_seconds: 3,
            error_kind: "schema_drift".to_string(),
            error_text: "missing field".to_string(),
        };

        let row = build_source_file_row(
            &cfg,
            &source,
            &db_path.to_string_lossy(),
            true,
            None,
            None,
            None,
            Some(&latest_error),
            now_unix_seconds(),
        );

        assert!(row.on_disk);
        assert_eq!(row.sqlite_wal_present, Some(true));
        assert_eq!(row.sqlite_shm_present, Some(true));
        assert!(row.issues.contains(&SourceFileIssue::Erroring));
        assert!(row.issues.contains(&SourceFileIssue::SqliteWalPresent));
        assert!(row.issues.contains(&SourceFileIssue::SqliteShmPresent));

        let missing_row = build_source_file_row(
            &cfg,
            &source,
            "/tmp/missing-opencode.db",
            false,
            Some(&FileCheckpointRow {
                source_file: "/tmp/missing-opencode.db".to_string(),
                last_offset: 42,
                last_line_no: 7,
                status: "ok".to_string(),
                updated_at: "2026-04-20 10:19:00".to_string(),
                updated_age_seconds: 10,
            }),
            Some(&FileRawStatsRow {
                source_file: "/tmp/missing-opencode.db".to_string(),
                count: 12,
                latest_raw_event_at: "2026-04-20 10:19:10".to_string(),
                latest_raw_event_age_seconds: 8,
            }),
            Some(&FileCanonicalStatsRow {
                source_file: "/tmp/missing-opencode.db".to_string(),
                count: 24,
            }),
            None,
            now_unix_seconds(),
        );

        assert!(!missing_row.on_disk);
        assert!(missing_row.issues.contains(&SourceFileIssue::MissingOnDisk));
        assert_eq!(missing_row.raw_event_count, 12);
        assert_eq!(missing_row.canonical_event_count, 24);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stale_reason_marks_disk_ahead_of_ingest_progress() {
        let reason = stale_reason(Some(5), Some(10), 1_000, 30);

        assert!(reason.is_some());
        assert!(reason
            .as_deref()
            .unwrap_or_default()
            .contains("disk writes are newer"));
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
            latest_checkpoint_age_seconds: 12,
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
        assert_eq!(row.latest_checkpoint_age_seconds, Some(12));
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
            latest_checkpoint_age_seconds: 0,
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
        assert_eq!(row.latest_checkpoint_age_seconds, Some(0));
        assert!(row.latest_error_at.is_none());
        assert!(row.latest_error_kind.is_none());
        assert!(row.latest_error_text.is_none());
    }

    #[test]
    fn build_source_runtime_snapshot_marks_stale_heartbeat() {
        let mut cfg = moraine_config::AppConfig::default();
        cfg.ingest.heartbeat_interval_seconds = 5.0;
        cfg.ingest.reconcile_interval_seconds = 30.0;

        let runtime = build_source_runtime_snapshot(
            &cfg,
            Some(SourceRuntimeRow {
                latest_heartbeat_at: "2026-04-20 10:15:00".to_string(),
                latest_heartbeat_age_seconds: 45,
                queue_depth: 0,
                files_active: 1,
                files_watched: 3,
                append_to_visible_p50_ms: 20,
                append_to_visible_p95_ms: 120,
                watcher_backend: "native".to_string(),
                watcher_error_count: 0,
                watcher_reset_count: 0,
                watcher_last_reset_at: String::new(),
            }),
        );

        assert_eq!(runtime.lag_indicator, Some(SourceLagIndicator::Stale));
        assert_eq!(runtime.heartbeat_cadence_seconds, 5.0);
        assert_eq!(runtime.reconcile_cadence_seconds, 30.0);
    }

    #[test]
    fn build_source_detail_warnings_distinguishes_file_runtime_and_watcher_state() {
        let source = SourceStatusRow {
            name: "opencode".to_string(),
            harness: "opencode".to_string(),
            format: "opencode_sqlite".to_string(),
            enabled: true,
            glob: "/tmp/opencode.db".to_string(),
            watch_root: "/tmp".to_string(),
            status: SourceHealthStatus::Warning,
            checkpoint_count: 3,
            latest_checkpoint_at: Some("2026-04-20 10:15:00".to_string()),
            latest_checkpoint_age_seconds: Some(18),
            raw_event_count: 42,
            ingest_error_count: 2,
            latest_error_at: Some("2026-04-20 10:20:00".to_string()),
            latest_error_kind: Some("schema_drift".to_string()),
            latest_error_text: Some("missing field".to_string()),
        };
        let runtime = SourceRuntimeSnapshot {
            latest_heartbeat_at: Some("2026-04-20 10:20:30".to_string()),
            latest_heartbeat_age_seconds: Some(16),
            queue_depth: Some(2),
            files_active: Some(1),
            files_watched: Some(4),
            append_to_visible_p50_ms: Some(50),
            append_to_visible_p95_ms: Some(250),
            watcher_backend: Some("mixed".to_string()),
            watcher_error_count: Some(1),
            watcher_reset_count: Some(2),
            watcher_last_reset_at: Some("2026-04-20 10:19:59".to_string()),
            heartbeat_cadence_seconds: 5.0,
            reconcile_cadence_seconds: 30.0,
            lag_indicator: Some(SourceLagIndicator::Delayed),
        };

        let warnings = build_source_detail_warnings(&source, &runtime, None, None);

        assert_eq!(warnings.len(), 3);
        assert_eq!(warnings[0].kind, SourceWarningKind::FileState);
        assert_eq!(warnings[1].kind, SourceWarningKind::IngestHeartbeat);
        assert_eq!(warnings[2].kind, SourceWarningKind::Watcher);
    }
}
