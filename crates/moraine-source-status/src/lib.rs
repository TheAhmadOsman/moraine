use anyhow::Result;
use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

    let mut query_error = None::<String>;
    let checkpoint_rows = match query_source_checkpoint_stats(&ch, &db).await {
        Ok(rows) => rows,
        Err(err) => {
            query_error = Some(format!("checkpoint query failed: {err}"));
            Vec::new()
        }
    };
    let raw_count_rows = match query_source_raw_counts(&ch, &db).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("raw event count query failed: {err}"));
            }
            Vec::new()
        }
    };
    let error_rows = match query_source_error_stats(&ch, &db).await {
        Ok(rows) => rows,
        Err(err) => {
            if query_error.is_none() {
                query_error = Some(format!("ingest error query failed: {err}"));
            }
            Vec::new()
        }
    };

    let checkpoints = checkpoint_rows
        .into_iter()
        .map(|row| (row.source_name.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let raw_counts = raw_count_rows
        .into_iter()
        .map(|row| (row.source_name, row.count))
        .collect::<BTreeMap<_, _>>();
    let errors = error_rows
        .into_iter()
        .map(|row| (row.source_name.clone(), row))
        .collect::<BTreeMap<_, _>>();

    let mut sources = Vec::new();
    for source in &cfg.ingest.sources {
        if !source.enabled && !include_disabled {
            continue;
        }

        let checkpoint = checkpoints.get(&source.name);
        let raw_event_count = raw_counts.get(&source.name).copied().unwrap_or(0);
        let error = errors.get(&source.name);
        let ingest_error_count = error.map(|row| row.ingest_error_count).unwrap_or(0);
        let status = source_health_status(
            source.enabled,
            checkpoint
                .map(|row| row.checkpoint_count)
                .unwrap_or_default(),
            raw_event_count,
            ingest_error_count,
            query_error.as_deref(),
        );

        sources.push(SourceStatusRow {
            name: source.name.clone(),
            harness: source.harness.clone(),
            format: source.format.clone(),
            enabled: source.enabled,
            glob: source.glob.clone(),
            watch_root: source.watch_root.clone(),
            status,
            checkpoint_count: checkpoint
                .map(|row| row.checkpoint_count)
                .unwrap_or_default(),
            latest_checkpoint_at: checkpoint
                .map(|row| row.latest_checkpoint_at.clone())
                .filter(|value| !value.is_empty()),
            raw_event_count,
            ingest_error_count,
            latest_error_at: error
                .map(|row| row.latest_error_at.clone())
                .filter(|value| !value.is_empty()),
            latest_error_kind: error
                .map(|row| row.latest_error_kind.clone())
                .filter(|value| !value.is_empty()),
            latest_error_text: error
                .map(|row| row.latest_error_text.clone())
                .filter(|value| !value.is_empty()),
        });
    }

    Ok(SourceStatusSnapshot {
        sources,
        query_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
