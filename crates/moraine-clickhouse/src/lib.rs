use anyhow::{anyhow, bail, Context, Result};
use moraine_config::ClickHouseConfig;
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Client, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone)]
pub struct ClickHouseClient {
    cfg: ClickHouseConfig,
    http: Client,
}

#[derive(Deserialize)]
struct ClickHouseEnvelope<T> {
    data: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
}

pub const SUPPORTED_CLICKHOUSE_VERSION_LINE: &str = "25.12";
pub const EXPERIMENTAL_CLICKHOUSE_VERSION_LINE: &str = "26.3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClickHouseVersionCompatibility {
    Supported,
    Experimental,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DoctorReport {
    pub clickhouse_healthy: bool,
    pub clickhouse_version: Option<String>,
    pub clickhouse_version_compatibility: ClickHouseVersionCompatibility,
    pub clickhouse_version_line: Option<String>,
    pub database: String,
    pub database_exists: bool,
    pub applied_migrations: Vec<String>,
    pub pending_migrations: Vec<String>,
    pub missing_tables: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorSeverity {
    Ok,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DoctorFinding {
    pub severity: DoctorSeverity,
    pub code: String,
    pub summary: String,
    pub remediation: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DoctorDeepReport {
    #[serde(flatten)]
    pub report: DoctorReport,
    pub findings: Vec<DoctorFinding>,
}

#[derive(Debug, Clone, Copy)]
struct ExpectedSchemaObject {
    name: &'static str,
    engine: &'static str,
}

#[derive(Debug, Deserialize)]
struct SchemaObjectRow {
    name: String,
    engine: String,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    total: u64,
}

impl ClickHouseClient {
    pub fn new(cfg: ClickHouseConfig) -> Result<Self> {
        let timeout = Duration::from_secs_f64(cfg.timeout_seconds.max(1.0));
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to construct reqwest client")?;

        Ok(Self { cfg, http })
    }

    pub fn config(&self) -> &ClickHouseConfig {
        &self.cfg
    }

    fn base_url(&self) -> Result<Url> {
        Url::parse(&self.cfg.url).context("invalid ClickHouse URL")
    }

    pub async fn request_text(
        &self,
        query: &str,
        body: Option<Vec<u8>>,
        database: Option<&str>,
        async_insert: bool,
        default_format: Option<&str>,
    ) -> Result<String> {
        let mut url = self.base_url()?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", query);
            if let Some(database) = database {
                qp.append_pair("database", database);
            }
            if let Some(default_format) = default_format {
                qp.append_pair("default_format", default_format);
            }
            if async_insert && self.cfg.async_insert {
                qp.append_pair("async_insert", "1");
                if self.cfg.wait_for_async_insert {
                    qp.append_pair("wait_for_async_insert", "1");
                }
            }
        }

        // ClickHouse HTTP treats GET as readonly, so use POST for both reads and writes.
        let payload = body.unwrap_or_default();
        let payload_len = payload.len();

        let mut req = self
            .http
            .post(url)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            // Some ClickHouse builds require an explicit Content-Length on POST.
            .header(CONTENT_LENGTH, payload_len)
            .body(payload);

        if !self.cfg.username.is_empty() {
            req = req.basic_auth(self.cfg.username.clone(), Some(self.cfg.password.clone()));
        }

        let response = req.send().await.context("clickhouse request failed")?;
        let status = response.status();
        let text = response.text().await.with_context(|| {
            format!(
                "failed to read clickhouse response body (status {})",
                status
            )
        })?;

        if !status.is_success() {
            return Err(anyhow!("clickhouse returned {}: {}", status, text));
        }

        Ok(text)
    }

    pub async fn ping(&self) -> Result<()> {
        let response = self
            .request_text("SELECT 1", None, Some("system"), false, None)
            .await?;
        if response.trim() == "1" {
            Ok(())
        } else {
            Err(anyhow!("unexpected ping response: {}", response.trim()))
        }
    }

    pub async fn version(&self) -> Result<String> {
        let rows: Vec<Value> = self
            .query_json_data("SELECT version() AS version", Some("system"))
            .await?;
        let version = rows
            .first()
            .and_then(|row| row.get("version"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing version in payload"))?;

        Ok(version.to_string())
    }

    pub async fn query_json_each_row<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text(query, None, database, false, None)
            .await?;
        let mut rows = Vec::new();

        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row = serde_json::from_str::<T>(line)
                .with_context(|| format!("failed to parse JSONEachRow line: {}", line))?;
            rows.push(row);
        }

        Ok(rows)
    }

    pub async fn query_json_data<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        let database = database.or(Some(&self.cfg.database));
        let raw = self
            .request_text(query, None, database, false, Some("JSON"))
            .await?;
        let envelope: ClickHouseEnvelope<T> = serde_json::from_str(&raw)
            .with_context(|| format!("invalid clickhouse JSON response: {}", raw))?;
        Ok(envelope.data)
    }

    pub async fn query_rows<T: DeserializeOwned>(
        &self,
        query: &str,
        database: Option<&str>,
    ) -> Result<Vec<T>> {
        if has_explicit_json_each_row_format(query) {
            return self.query_json_each_row(query, database).await;
        }

        match self.query_json_data(query, database).await {
            Ok(rows) => Ok(rows),
            Err(_) => self.query_json_each_row(query, database).await,
        }
    }

    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::<u8>::new();
        for row in rows {
            let line = serde_json::to_vec(row).context("failed to encode JSON row")?;
            payload.extend_from_slice(&line);
            payload.push(b'\n');
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            escape_identifier(&self.cfg.database),
            escape_identifier(table)
        );
        self.request_text(&query, Some(payload), None, true, None)
            .await?;
        Ok(())
    }

    pub async fn run_migrations(&self) -> Result<Vec<String>> {
        validate_identifier(&self.cfg.database)?;

        self.request_text(
            &format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                escape_identifier(&self.cfg.database)
            ),
            None,
            None,
            false,
            None,
        )
        .await?;

        self.ensure_migration_ledger().await?;
        let applied = self.applied_migration_versions().await?;

        let mut executed = Vec::new();
        for migration in bundled_migrations() {
            if applied.contains(migration.version) {
                continue;
            }

            let sql = materialize_migration_sql(migration.sql, &self.cfg.database)?;
            for statement in split_sql_statements(&sql) {
                self.request_text(&statement, None, Some(&self.cfg.database), false, None)
                    .await
                    .with_context(|| {
                        format!(
                            "failed migration {} statement: {}",
                            migration.name,
                            truncate_for_error(&statement)
                        )
                    })?;
            }

            let log_stmt = format!(
                "INSERT INTO {}.schema_migrations (version, name) VALUES ({}, {})",
                escape_identifier(&self.cfg.database),
                escape_literal(migration.version),
                escape_literal(migration.name)
            );
            self.request_text(&log_stmt, None, Some(&self.cfg.database), false, None)
                .await
                .with_context(|| format!("failed to record migration {}", migration.name))?;

            executed.push(migration.version.to_string());
        }

        Ok(executed)
    }

    pub async fn pending_migration_versions(&self) -> Result<Vec<String>> {
        self.ensure_migration_ledger().await?;
        let applied = self.applied_migration_versions().await?;
        Ok(bundled_migrations()
            .into_iter()
            .filter(|m| !applied.contains(m.version))
            .map(|m| m.version.to_string())
            .collect())
    }

    pub async fn doctor_report(&self) -> Result<DoctorReport> {
        let mut report = DoctorReport {
            clickhouse_healthy: false,
            clickhouse_version: None,
            clickhouse_version_compatibility: ClickHouseVersionCompatibility::Unknown,
            clickhouse_version_line: None,
            database: self.cfg.database.clone(),
            database_exists: false,
            applied_migrations: Vec::new(),
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            errors: Vec::new(),
        };

        match self.ping().await {
            Ok(()) => {
                report.clickhouse_healthy = true;
            }
            Err(err) => {
                report.errors.push(format!("ping failed: {err}"));
                return Ok(report);
            }
        }

        match self.version().await {
            Ok(version) => {
                let (compatibility, line) = classify_clickhouse_version(Some(&version));
                report.clickhouse_version = Some(version);
                report.clickhouse_version_compatibility = compatibility;
                report.clickhouse_version_line = line;
            }
            Err(err) => report.errors.push(format!("version query failed: {err}")),
        }

        #[derive(Deserialize)]
        struct ExistsRow {
            exists: u8,
        }

        let exists_query = format!(
            "SELECT toUInt8(count() > 0) AS exists FROM system.databases WHERE name = {}",
            escape_literal(&self.cfg.database)
        );

        match self
            .query_json_data::<ExistsRow>(&exists_query, Some("system"))
            .await
        {
            Ok(rows) => {
                report.database_exists = rows.first().map(|r| r.exists == 1).unwrap_or(false)
            }
            Err(err) => {
                report
                    .errors
                    .push(format!("database existence query failed: {err}"));
                return Ok(report);
            }
        }

        if !report.database_exists {
            report
                .errors
                .push(format!("database '{}' does not exist", self.cfg.database));
            return Ok(report);
        }

        match self.applied_migration_versions().await {
            Ok(applied) => {
                let mut versions: Vec<String> = applied.into_iter().collect();
                versions.sort();
                report.applied_migrations = versions;
            }
            Err(err) => report
                .errors
                .push(format!("failed to read migration ledger: {err}")),
        }

        let pending = bundled_migrations()
            .into_iter()
            .filter(|m| !report.applied_migrations.iter().any(|v| v == m.version))
            .map(|m| m.version.to_string())
            .collect::<Vec<_>>();
        report.pending_migrations = pending;

        #[derive(Deserialize)]
        struct TableRow {
            name: String,
        }

        let table_query = format!(
            "SELECT name FROM system.tables WHERE database = {}",
            escape_literal(&self.cfg.database)
        );

        let required = [
            "raw_events",
            "events",
            "event_links",
            "tool_io",
            "ingest_errors",
            "ingest_checkpoints",
            "ingest_heartbeats",
            "search_documents",
            "search_postings",
            "search_conversation_terms",
            "search_term_stats",
            "search_corpus_stats",
            "search_query_log",
            "search_hit_log",
            "search_interaction_log",
            "schema_migrations",
        ];

        match self
            .query_json_data::<TableRow>(&table_query, Some("system"))
            .await
        {
            Ok(rows) => {
                let existing = rows.into_iter().map(|r| r.name).collect::<HashSet<_>>();
                report.missing_tables = required
                    .iter()
                    .filter(|name| !existing.contains(**name))
                    .map(|name| (*name).to_string())
                    .collect();
            }
            Err(err) => report.errors.push(format!("table listing failed: {err}")),
        }

        Ok(report)
    }

    pub async fn doctor_deep_report(&self) -> Result<DoctorDeepReport> {
        let report = self.doctor_report().await?;
        let mut findings = doctor_findings_from_report(&report);

        if !report.clickhouse_healthy || !report.database_exists {
            return Ok(DoctorDeepReport { report, findings });
        }

        match self.doctor_schema_objects().await {
            Ok(rows) => findings.push(evaluate_expected_schema_objects(&rows)),
            Err(err) => findings.push(DoctorFinding {
                severity: DoctorSeverity::Warning,
                code: "schema.derived_objects".to_string(),
                summary: format!("Failed to inspect expected views and materialized views: {err}"),
                remediation:
                    "Run `moraine db migrate` and verify ClickHouse system metadata is readable."
                        .to_string(),
            }),
        }

        let has_tables = |required: &[&str]| {
            required
                .iter()
                .all(|name| !report.missing_tables.iter().any(|missing| missing == name))
        };

        if has_tables(&["event_links", "events"]) {
            findings.push(
                self.doctor_count_finding(
                    "integrity.event_links_orphans",
                    &format!(
                        "SELECT toUInt64(count()) AS total FROM {}.event_links FINAL WHERE event_uid NOT IN (SELECT event_uid FROM {}.events FINAL)",
                        escape_identifier(&self.cfg.database),
                        escape_identifier(&self.cfg.database)
                    ),
                    "No orphan event_links rows found.",
                    "orphan event_links rows",
                    "Re-run normalization or repair the broken `events` rows before trusting link traversal."
                        .to_string(),
                    DoctorSeverity::Error,
                )
                .await,
            );
        } else {
            findings.push(skipped_check_finding(
                "integrity.event_links_orphans",
                &missing_prerequisites(&report, &["event_links", "events"]),
                "orphan event_links rows",
            ));
        }

        if has_tables(&["tool_io", "events"]) {
            findings.push(
                self.doctor_count_finding(
                    "integrity.tool_io_orphans",
                    &format!(
                        "SELECT toUInt64(count()) AS total FROM {}.tool_io FINAL WHERE event_uid NOT IN (SELECT event_uid FROM {}.events FINAL)",
                        escape_identifier(&self.cfg.database),
                        escape_identifier(&self.cfg.database)
                    ),
                    "No orphan tool_io rows found.",
                    "orphan tool_io rows",
                    "Re-run normalization or repair the broken `events` rows before using tool I/O provenance."
                        .to_string(),
                    DoctorSeverity::Error,
                )
                .await,
            );
        } else {
            findings.push(skipped_check_finding(
                "integrity.tool_io_orphans",
                &missing_prerequisites(&report, &["tool_io", "events"]),
                "orphan tool_io rows",
            ));
        }

        if has_tables(&["events", "raw_events"]) {
            findings.push(
                self.doctor_count_finding(
                    "integrity.events_missing_raw_events",
                    &events_missing_raw_events_query(&self.cfg.database),
                    "Every normalized event still has a backing raw_events row.",
                    "events without raw_events source-coordinate backing rows",
                    "Re-import or restore the missing raw rows before relying on replay, audits, or provenance checks."
                        .to_string(),
                    DoctorSeverity::Error,
                )
                .await,
            );
        } else {
            findings.push(skipped_check_finding(
                "integrity.events_missing_raw_events",
                &missing_prerequisites(&report, &["events", "raw_events"]),
                "events missing raw_events backing rows",
            ));
        }

        if has_tables(&["events"]) {
            findings.push(
                self.doctor_count_finding(
                    "integrity.session_time_ranges",
                    &session_time_ranges_query(&self.cfg.database),
                    "Session time ranges are internally consistent.",
                    "sessions with session_date ranges outside event_ts ranges",
                    "Rebuild the affected sessions so `session_date` and event timestamps come from the same normalized source."
                        .to_string(),
                    DoctorSeverity::Warning,
                )
                .await,
            );
        } else {
            findings.push(skipped_check_finding(
                "integrity.session_time_ranges",
                &missing_prerequisites(&report, &["events"]),
                "impossible session time ranges",
            ));
        }

        if has_tables(&["events", "search_documents"]) {
            findings.push(
                self.doctor_count_finding(
                    "search.index_freshness",
                    &format!(
                        "SELECT toUInt64(count()) AS total FROM {}.events FINAL WHERE lengthUTF8(replaceRegexpAll(text_content, '\\s+', '')) > 0 AND event_uid NOT IN (SELECT event_uid FROM {}.search_documents FINAL)",
                        escape_identifier(&self.cfg.database),
                        escape_identifier(&self.cfg.database)
                    ),
                    "Search documents are in sync with text-bearing events.",
                    "text-bearing events missing search_documents rows",
                    "Run `moraine db migrate` and rebuild search documents if the materialized views were dropped or stalled."
                        .to_string(),
                    DoctorSeverity::Warning,
                )
                .await,
            );
        } else {
            findings.push(skipped_check_finding(
                "search.index_freshness",
                &missing_prerequisites(&report, &["events", "search_documents"]),
                "search freshness drift",
            ));
        }

        Ok(DoctorDeepReport { report, findings })
    }

    async fn ensure_migration_ledger(&self) -> Result<()> {
        self.request_text(
            &format!(
                "CREATE TABLE IF NOT EXISTS {}.schema_migrations (\
                 version String, \
                 name String, \
                 applied_at DateTime64(3) DEFAULT now64(3)\
                 ) ENGINE = ReplacingMergeTree(applied_at) \
                 ORDER BY (version)",
                escape_identifier(&self.cfg.database)
            ),
            None,
            Some(&self.cfg.database),
            false,
            None,
        )
        .await?;

        Ok(())
    }

    async fn applied_migration_versions(&self) -> Result<HashSet<String>> {
        #[derive(Deserialize)]
        struct Row {
            version: String,
        }

        let query = format!(
            "SELECT version FROM {}.schema_migrations GROUP BY version",
            escape_identifier(&self.cfg.database)
        );

        let rows: Vec<Row> = self
            .query_json_data(&query, Some(&self.cfg.database))
            .await?;
        Ok(rows.into_iter().map(|row| row.version).collect())
    }

    async fn doctor_schema_objects(&self) -> Result<Vec<SchemaObjectRow>> {
        let names = expected_schema_objects()
            .iter()
            .map(|item| escape_literal(item.name))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT name, engine FROM system.tables WHERE database = {} AND name IN ({})",
            escape_literal(&self.cfg.database),
            names
        );

        self.query_json_data(&query, Some("system")).await
    }

    async fn doctor_count_finding(
        &self,
        code: &str,
        query: &str,
        ok_summary: &str,
        issue_noun: &str,
        remediation: String,
        issue_severity: DoctorSeverity,
    ) -> DoctorFinding {
        match self
            .query_json_data::<CountRow>(query, Some(&self.cfg.database))
            .await
        {
            Ok(rows) => {
                let count = rows.first().map(|row| row.total).unwrap_or(0);
                count_finding(
                    code,
                    count,
                    ok_summary,
                    issue_noun,
                    remediation,
                    issue_severity,
                )
            }
            Err(err) => DoctorFinding {
                severity: DoctorSeverity::Warning,
                code: code.to_string(),
                summary: format!("Failed to evaluate {code}: {err}"),
                remediation:
                    "Verify the referenced tables are readable, then rerun `moraine db doctor`."
                        .to_string(),
            },
        }
    }
}

pub fn bundled_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: "001",
            name: "001_schema.sql",
            sql: include_str!("../../../sql/001_schema.sql"),
        },
        Migration {
            version: "002",
            name: "002_views.sql",
            sql: include_str!("../../../sql/002_views.sql"),
        },
        Migration {
            version: "003",
            name: "003_ingest_heartbeats.sql",
            sql: include_str!("../../../sql/003_ingest_heartbeats.sql"),
        },
        Migration {
            version: "004",
            name: "004_search_index.sql",
            sql: include_str!("../../../sql/004_search_index.sql"),
        },
        Migration {
            version: "005",
            name: "005_watcher_heartbeat_metrics.sql",
            sql: include_str!("../../../sql/005_watcher_heartbeat_metrics.sql"),
        },
        Migration {
            version: "006",
            name: "006_search_stats_authoritative_views.sql",
            sql: include_str!("../../../sql/006_search_stats_authoritative_views.sql"),
        },
        Migration {
            version: "007",
            name: "007_event_links_external_id.sql",
            sql: include_str!("../../../sql/007_event_links_external_id.sql"),
        },
        Migration {
            version: "008",
            name: "008_categorical_domain_contracts.sql",
            sql: include_str!("../../../sql/008_categorical_domain_contracts.sql"),
        },
        Migration {
            version: "009",
            name: "009_search_documents_codex_flag.sql",
            sql: include_str!("../../../sql/009_search_documents_codex_flag.sql"),
        },
        Migration {
            version: "010",
            name: "010_search_conversation_terms.sql",
            sql: include_str!("../../../sql/010_search_conversation_terms.sql"),
        },
        Migration {
            version: "011",
            name: "011_rename_provider_to_harness.sql",
            sql: include_str!("../../../sql/011_rename_provider_to_harness.sql"),
        },
        Migration {
            version: "012",
            name: "012_add_inference_provider_and_rename_claude.sql",
            sql: include_str!("../../../sql/012_add_inference_provider_and_rename_claude.sql"),
        },
        Migration {
            version: "013",
            name: "013_privacy_metadata.sql",
            sql: include_str!("../../../sql/013_privacy_metadata.sql"),
        },
    ]
}

fn truncate_for_error(statement: &str) -> String {
    const LIMIT: usize = 240;
    let compact = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= LIMIT {
        compact
    } else {
        let mut boundary = LIMIT;
        while !compact.is_char_boundary(boundary) {
            boundary -= 1;
        }
        format!("{}...", &compact[..boundary])
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        bail!("identifier must not be empty");
    }

    let ok = identifier
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ok {
        bail!("identifier contains unsupported characters: {identifier}");
    }

    Ok(())
}

fn materialize_migration_sql(sql: &str, database: &str) -> Result<String> {
    validate_identifier(database)?;

    let mut text = sql.to_string();
    text = text.replace(
        "CREATE DATABASE IF NOT EXISTS moraine;",
        &format!("CREATE DATABASE IF NOT EXISTS {database};"),
    );
    text = text.replace("moraine.", &format!("{database}."));
    Ok(text)
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut prev = '\0';

    for line in sql.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }

        let chars: Vec<char> = line.chars().collect();
        let mut idx = 0;
        while idx < chars.len() {
            let ch = chars[idx];
            if ch == '\'' {
                if in_single_quote && idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                    current.push(ch);
                    current.push(chars[idx + 1]);
                    prev = chars[idx + 1];
                    idx += 2;
                    continue;
                }
                if prev != '\\' {
                    in_single_quote = !in_single_quote;
                }
            }

            if ch == ';' && !in_single_quote {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
                prev = '\0';
                idx += 1;
                continue;
            }

            current.push(ch);
            prev = ch;
            idx += 1;
        }

        current.push('\n');
    }

    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }

    statements
}

fn escape_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn escape_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn has_explicit_json_each_row_format(query: &str) -> bool {
    let compact = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    compact.contains(" format jsoneachrow")
}

fn expected_schema_objects() -> &'static [ExpectedSchemaObject] {
    &[
        ExpectedSchemaObject {
            name: "v_all_events",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "v_conversation_trace",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "v_turn_summary",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "v_session_summary",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "search_term_stats",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "search_corpus_stats",
            engine: "View",
        },
        ExpectedSchemaObject {
            name: "mv_search_documents_from_events",
            engine: "MaterializedView",
        },
        ExpectedSchemaObject {
            name: "mv_search_postings",
            engine: "MaterializedView",
        },
        ExpectedSchemaObject {
            name: "mv_search_conversation_terms",
            engine: "MaterializedView",
        },
    ]
}

fn events_missing_raw_events_query(database: &str) -> String {
    let db = escape_identifier(database);
    format!(
        "SELECT toUInt64(count()) AS total \
         FROM {db}.events FINAL \
         WHERE (source_name, source_file, source_generation, source_offset, source_line_no) \
         NOT IN (\
             SELECT source_name, source_file, source_generation, source_offset, source_line_no \
             FROM {db}.raw_events\
         )"
    )
}

fn session_time_ranges_query(database: &str) -> String {
    let db = escape_identifier(database);
    format!(
        "SELECT toUInt64(count()) AS total \
         FROM (\
             SELECT \
                 session_id, \
                 countIf(event_ts >= toDateTime64('2000-01-01 00:00:00', 3)) AS real_event_count, \
                 countIf(session_date != toDate('1970-01-01')) AS real_session_date_rows, \
                 minIf(session_date, session_date != toDate('1970-01-01')) AS min_session_date, \
                 maxIf(session_date, session_date != toDate('1970-01-01')) AS max_session_date, \
                 toDate(minIf(event_ts, event_ts >= toDateTime64('2000-01-01 00:00:00', 3))) AS first_event_date, \
                 toDate(maxIf(event_ts, event_ts >= toDateTime64('2000-01-01 00:00:00', 3))) AS last_event_date \
             FROM {db}.events FINAL \
             GROUP BY session_id \
             HAVING real_event_count > 0 \
                 AND real_session_date_rows > 0 \
                 AND (\
                     dateDiff('day', max_session_date, first_event_date) > 2 \
                     OR dateDiff('day', last_event_date, min_session_date) > 2\
                 )\
         )"
    )
}

fn doctor_findings_from_report(report: &DoctorReport) -> Vec<DoctorFinding> {
    let mut findings = vec![
        DoctorFinding {
            severity: if report.clickhouse_healthy {
                DoctorSeverity::Ok
            } else {
                DoctorSeverity::Error
            },
            code: "clickhouse.reachable".to_string(),
            summary: if report.clickhouse_healthy {
                "ClickHouse responded to the health check.".to_string()
            } else {
                "ClickHouse did not respond to the health check.".to_string()
            },
            remediation: "Start ClickHouse and verify the configured HTTP endpoint is reachable."
                .to_string(),
        },
        DoctorFinding {
            severity: if report.database_exists {
                DoctorSeverity::Ok
            } else {
                DoctorSeverity::Error
            },
            code: "clickhouse.database_exists".to_string(),
            summary: if report.database_exists {
                format!("Database `{}` exists.", report.database)
            } else {
                format!("Database `{}` does not exist.", report.database)
            },
            remediation: "Run `moraine db migrate` to create the database and bundled schema."
                .to_string(),
        },
        DoctorFinding {
            severity: match report.clickhouse_version_compatibility {
                ClickHouseVersionCompatibility::Supported => DoctorSeverity::Ok,
                ClickHouseVersionCompatibility::Experimental => DoctorSeverity::Warning,
                ClickHouseVersionCompatibility::Unsupported => DoctorSeverity::Error,
                ClickHouseVersionCompatibility::Unknown => DoctorSeverity::Warning,
            },
            code: "clickhouse.version_compatibility".to_string(),
            summary: clickhouse_version_compatibility_summary(report),
            remediation: clickhouse_version_compatibility_remediation(report).to_string(),
        },
        if report.pending_migrations.is_empty() {
            DoctorFinding {
                severity: DoctorSeverity::Ok,
                code: "schema.pending_migrations".to_string(),
                summary: "All bundled migrations are applied.".to_string(),
                remediation: "No action needed.".to_string(),
            }
        } else {
            DoctorFinding {
                severity: DoctorSeverity::Warning,
                code: "schema.pending_migrations".to_string(),
                summary: format!(
                    "{} pending migration(s): {}.",
                    report.pending_migrations.len(),
                    report.pending_migrations.join(", ")
                ),
                remediation: "Run `moraine db migrate` before relying on deeper integrity checks."
                    .to_string(),
            }
        },
        if report.missing_tables.is_empty() {
            DoctorFinding {
                severity: DoctorSeverity::Ok,
                code: "schema.required_tables".to_string(),
                summary: "All required base and search tables are present.".to_string(),
                remediation: "No action needed.".to_string(),
            }
        } else {
            DoctorFinding {
                severity: DoctorSeverity::Error,
                code: "schema.required_tables".to_string(),
                summary: format!(
                    "Missing required table(s): {}.",
                    report.missing_tables.join(", ")
                ),
                remediation:
                    "Run `moraine db migrate` and restore any missing table data before continuing."
                        .to_string(),
            }
        },
    ];

    if !report.errors.is_empty() {
        findings.push(DoctorFinding {
            severity: DoctorSeverity::Warning,
            code: "doctor.query_errors".to_string(),
            summary: format!(
                "Doctor encountered {} query/runtime error(s): {}.",
                report.errors.len(),
                report.errors.join(" | ")
            ),
            remediation: "Address the ClickHouse errors above, then rerun `moraine db doctor`."
                .to_string(),
        });
    }

    findings
}

fn classify_clickhouse_version(
    version: Option<&str>,
) -> (ClickHouseVersionCompatibility, Option<String>) {
    let Some(version) = version else {
        return (ClickHouseVersionCompatibility::Unknown, None);
    };
    let Some(line) = clickhouse_version_line(version) else {
        return (ClickHouseVersionCompatibility::Unknown, None);
    };

    let compatibility = if line == SUPPORTED_CLICKHOUSE_VERSION_LINE {
        ClickHouseVersionCompatibility::Supported
    } else if line == EXPERIMENTAL_CLICKHOUSE_VERSION_LINE {
        ClickHouseVersionCompatibility::Experimental
    } else {
        ClickHouseVersionCompatibility::Unsupported
    };

    (compatibility, Some(line))
}

fn clickhouse_version_line(version: &str) -> Option<String> {
    let trimmed = version.trim().trim_start_matches('v');
    let mut parts = trimmed.split('.');
    let major = parts
        .next()?
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let minor = parts
        .next()?
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if major.is_empty() || minor.is_empty() {
        return None;
    }
    Some(format!("{major}.{minor}"))
}

fn clickhouse_version_compatibility_summary(report: &DoctorReport) -> String {
    match (
        report.clickhouse_version_compatibility,
        report.clickhouse_version.as_deref(),
        report.clickhouse_version_line.as_deref(),
    ) {
        (ClickHouseVersionCompatibility::Supported, Some(version), Some(line)) => {
            format!("ClickHouse {version} is on the supported {line} line.")
        }
        (ClickHouseVersionCompatibility::Experimental, Some(version), Some(line)) => {
            format!("ClickHouse {version} is on the experimental {line} line.")
        }
        (ClickHouseVersionCompatibility::Unsupported, Some(version), Some(line)) => {
            format!("ClickHouse {version} is on unsupported line {line}.")
        }
        (ClickHouseVersionCompatibility::Unknown, Some(version), None) => {
            format!("ClickHouse {version} could not be mapped to a known compatibility line.")
        }
        _ => "ClickHouse version compatibility is unknown.".to_string(),
    }
}

fn clickhouse_version_compatibility_remediation(report: &DoctorReport) -> &'static str {
    match report.clickhouse_version_compatibility {
        ClickHouseVersionCompatibility::Supported => "No action needed.",
        ClickHouseVersionCompatibility::Experimental => {
            "Use the experimental line only for deliberate validation; the default supported line remains 25.12."
        }
        ClickHouseVersionCompatibility::Unsupported => {
            "Downgrade to the supported 25.12 line or validate and explicitly adopt the experimental 26.3 line before relying on this runtime."
        }
        ClickHouseVersionCompatibility::Unknown => {
            "Verify `SELECT version()` output and compare it against the supported 25.12 line and experimental 26.3 line."
        }
    }
}

fn evaluate_expected_schema_objects(rows: &[SchemaObjectRow]) -> DoctorFinding {
    let mut missing = Vec::new();
    let mut wrong_engine = Vec::new();

    for expected in expected_schema_objects() {
        match rows.iter().find(|row| row.name == expected.name) {
            Some(row) if row.engine == expected.engine => {}
            Some(row) => wrong_engine.push(format!(
                "{} has engine {} (expected {})",
                expected.name, row.engine, expected.engine
            )),
            None => missing.push(expected.name),
        }
    }

    if missing.is_empty() && wrong_engine.is_empty() {
        DoctorFinding {
            severity: DoctorSeverity::Ok,
            code: "schema.derived_objects".to_string(),
            summary:
                "All expected views and materialized views are present with sane engine classes."
                    .to_string(),
            remediation: "No action needed.".to_string(),
        }
    } else {
        let mut issues = Vec::new();
        if !missing.is_empty() {
            issues.push(format!("missing: {}", missing.join(", ")));
        }
        if !wrong_engine.is_empty() {
            issues.push(format!("wrong engine: {}", wrong_engine.join(", ")));
        }

        DoctorFinding {
            severity: DoctorSeverity::Error,
            code: "schema.derived_objects".to_string(),
            summary: format!(
                "Expected views/materialized views are missing or malformed: {}.",
                issues.join("; ")
            ),
            remediation:
                "Run `moraine db migrate` to recreate derived objects, then rebuild search artifacts if drift remains."
                    .to_string(),
        }
    }
}

fn count_finding(
    code: &str,
    count: u64,
    ok_summary: &str,
    issue_noun: &str,
    remediation: String,
    issue_severity: DoctorSeverity,
) -> DoctorFinding {
    if count == 0 {
        DoctorFinding {
            severity: DoctorSeverity::Ok,
            code: code.to_string(),
            summary: ok_summary.to_string(),
            remediation: "No action needed.".to_string(),
        }
    } else {
        DoctorFinding {
            severity: issue_severity,
            code: code.to_string(),
            summary: format!("Found {count} {issue_noun}."),
            remediation,
        }
    }
}

fn missing_prerequisites<'a>(report: &'a DoctorReport, required: &[&'a str]) -> Vec<&'a str> {
    required
        .iter()
        .copied()
        .filter(|name| report.missing_tables.iter().any(|missing| missing == name))
        .collect()
}

fn skipped_check_finding(code: &str, missing_tables: &[&str], description: &str) -> DoctorFinding {
    DoctorFinding {
        severity: DoctorSeverity::Warning,
        code: code.to_string(),
        summary: format!(
            "Skipped check for {description} because required table(s) are missing: {}.",
            missing_tables.join(", ")
        ),
        remediation: "Run `moraine db migrate` and restore the missing tables before rerunning the deep doctor."
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Query,
        http::{HeaderMap, StatusCode},
        routing::get,
        Router,
    };
    use moraine_config::ClickHouseConfig;
    use serde::Deserialize;
    use std::collections::HashMap;

    fn test_clickhouse_config(url: String) -> ClickHouseConfig {
        ClickHouseConfig {
            url,
            database: "moraine".to_string(),
            username: "default".to_string(),
            password: String::new(),
            timeout_seconds: 5.0,
            async_insert: true,
            wait_for_async_insert: true,
        }
    }

    fn test_doctor_report() -> DoctorReport {
        DoctorReport {
            clickhouse_healthy: true,
            clickhouse_version: Some("25.1".to_string()),
            clickhouse_version_compatibility: ClickHouseVersionCompatibility::Unsupported,
            clickhouse_version_line: Some("25.1".to_string()),
            database: "moraine".to_string(),
            database_exists: true,
            applied_migrations: vec!["001".to_string()],
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            errors: Vec::new(),
        }
    }

    async fn spawn_mock_server() -> String {
        async fn handler(
            Query(params): Query<HashMap<String, String>>,
            headers: HeaderMap,
        ) -> (StatusCode, String) {
            if headers.get("content-length").is_none() {
                return (
                    StatusCode::LENGTH_REQUIRED,
                    "missing content-length".to_string(),
                );
            }

            let query = params.get("query").cloned().unwrap_or_default();
            if query.contains("FAIL") {
                return (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_string());
            }

            if params
                .get("default_format")
                .is_some_and(|fmt| fmt == "JSON")
            {
                return (StatusCode::OK, "not-json".to_string());
            }

            (StatusCode::OK, "{\"value\":7}\n".to_string())
        }

        let app = Router::new().route("/", get(handler).post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{}", addr)
    }

    fn spawn_truncated_body_server() -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind raw listener");
        let addr = listener.local_addr().expect("raw listener addr");

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request);

                let response = concat!(
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: text/plain; charset=utf-8\r\n",
                    "Content-Length: 20\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                    "short",
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        format!("http://{}", addr)
    }

    #[test]
    fn sql_split_handles_multiple_statements() {
        let sql = "CREATE TABLE a (x String);\nINSERT INTO a VALUES ('a;b');\n";
        let out = split_sql_statements(sql);
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("CREATE TABLE"));
        assert!(out[1].contains("'a;b'"));
    }

    #[test]
    fn sql_split_handles_sql_standard_escaped_quotes() {
        let sql = "INSERT INTO a VALUES ('it''s;fine');\nSELECT 1;\n";
        let out = split_sql_statements(sql);
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("'it''s;fine'"));
    }

    #[test]
    fn sql_split_handles_escaped_quote_after_backslash() {
        let sql = "INSERT INTO a VALUES ('path\\'';still-string');\nSELECT 1;\n";
        let out = split_sql_statements(sql);
        assert_eq!(
            out,
            vec![
                "INSERT INTO a VALUES ('path\\'';still-string')".to_string(),
                "SELECT 1".to_string()
            ]
        );
    }

    #[test]
    fn sql_materialization_rewrites_database() {
        let sql = "CREATE DATABASE IF NOT EXISTS moraine;\nCREATE TABLE moraine.events (x UInt8);";
        let out = materialize_migration_sql(sql, "custom_db").expect("should rewrite");
        assert!(out.contains("CREATE DATABASE IF NOT EXISTS custom_db;"));
        assert!(out.contains("custom_db.events"));
    }

    #[test]
    fn identifier_validation_rejects_invalid() {
        assert!(validate_identifier("moraine_01").is_ok());
        assert!(validate_identifier("moraine-db").is_err());
    }

    #[test]
    fn format_detection_handles_case_and_whitespace() {
        assert!(has_explicit_json_each_row_format(
            "SELECT 1\nFORMAT JSONEachRow"
        ));
        assert!(has_explicit_json_each_row_format(
            "SELECT 1 format jsoneachrow"
        ));
        assert!(!has_explicit_json_each_row_format("SELECT 1"));
        assert!(!has_explicit_json_each_row_format("SELECT 1 FORMAT JSON"));
    }

    #[test]
    fn doctor_severity_serializes_lowercase() {
        let value = serde_json::to_string(&DoctorSeverity::Warning).expect("serialize severity");
        assert_eq!(value, "\"warning\"");
    }

    #[test]
    fn clickhouse_version_compatibility_serializes_lowercase() {
        let value = serde_json::to_string(&ClickHouseVersionCompatibility::Experimental)
            .expect("serialize compatibility");
        assert_eq!(value, "\"experimental\"");
    }

    #[test]
    fn classify_clickhouse_version_tracks_supported_lines() {
        assert_eq!(
            classify_clickhouse_version(Some("25.12.5.44")),
            (
                ClickHouseVersionCompatibility::Supported,
                Some("25.12".to_string())
            )
        );
        assert_eq!(
            classify_clickhouse_version(Some("v26.3.1.2-stable")),
            (
                ClickHouseVersionCompatibility::Experimental,
                Some("26.3".to_string())
            )
        );
        assert_eq!(
            classify_clickhouse_version(Some("25.8.9.1")),
            (
                ClickHouseVersionCompatibility::Unsupported,
                Some("25.8".to_string())
            )
        );
        assert_eq!(
            classify_clickhouse_version(Some("not-a-version")),
            (ClickHouseVersionCompatibility::Unknown, None)
        );
    }

    #[test]
    fn doctor_findings_from_report_include_pending_migrations_and_errors() {
        let mut report = test_doctor_report();
        report.pending_migrations = vec!["013".to_string()];
        report.errors = vec!["version query failed".to_string()];

        let findings = doctor_findings_from_report(&report);

        assert!(findings.iter().any(|finding| {
            finding.code == "schema.pending_migrations"
                && finding.severity == DoctorSeverity::Warning
                && finding.summary.contains("013")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "clickhouse.version_compatibility"
                && finding.severity == DoctorSeverity::Error
                && finding.summary.contains("unsupported line 25.1")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "doctor.query_errors"
                && finding.severity == DoctorSeverity::Warning
                && finding.summary.contains("version query failed")
        }));
    }

    #[test]
    fn doctor_report_serialization_includes_version_compatibility_fields() {
        let value = serde_json::to_value(test_doctor_report()).expect("serialize report");
        assert_eq!(value["clickhouse_version_compatibility"], "unsupported");
        assert_eq!(value["clickhouse_version_line"], "25.1");
    }

    #[test]
    fn expected_schema_objects_finding_detects_missing_and_wrong_engine() {
        let finding = evaluate_expected_schema_objects(&[
            SchemaObjectRow {
                name: "v_all_events".to_string(),
                engine: "View".to_string(),
            },
            SchemaObjectRow {
                name: "mv_search_documents_from_events".to_string(),
                engine: "View".to_string(),
            },
        ]);

        assert_eq!(finding.code, "schema.derived_objects");
        assert_eq!(finding.severity, DoctorSeverity::Error);
        assert!(finding
            .summary
            .contains("mv_search_documents_from_events has engine View"));
        assert!(finding.summary.contains("v_conversation_trace"));
    }

    #[test]
    fn events_missing_raw_events_query_uses_source_coordinates() {
        let query = events_missing_raw_events_query("moraine");

        assert!(query.contains("source_name, source_file, source_generation"));
        assert!(query.contains("source_offset, source_line_no"));
        assert!(!query.contains("event_uid NOT IN"));
    }

    #[test]
    fn session_time_ranges_query_ignores_synthetic_epoch_and_allows_timezone_skew() {
        let query = session_time_ranges_query("moraine");

        assert!(query.contains("session_date != toDate('1970-01-01')"));
        assert!(query.contains("event_ts >= toDateTime64('2000-01-01 00:00:00', 3)"));
        assert!(query.contains("dateDiff('day', max_session_date, first_event_date) > 2"));
        assert!(query.contains("dateDiff('day', last_event_date, min_session_date) > 2"));
    }

    #[test]
    fn count_finding_reports_issue_counts() {
        let finding = count_finding(
            "integrity.event_links_orphans",
            3,
            "No orphan event_links rows found.",
            "orphan event_links rows",
            "repair".to_string(),
            DoctorSeverity::Error,
        );

        assert_eq!(finding.severity, DoctorSeverity::Error);
        assert_eq!(finding.summary, "Found 3 orphan event_links rows.");
        assert_eq!(finding.remediation, "repair");
    }

    #[test]
    fn skipped_check_finding_lists_missing_tables() {
        let finding = skipped_check_finding(
            "search.index_freshness",
            &["events", "search_documents"],
            "search freshness drift",
        );

        assert_eq!(finding.severity, DoctorSeverity::Warning);
        assert!(finding.summary.contains("events, search_documents"));
    }

    #[test]
    fn missing_prerequisites_returns_only_missing_required_tables() {
        let mut report = test_doctor_report();
        report.missing_tables = vec!["events".to_string(), "tool_io".to_string()];

        let missing = missing_prerequisites(&report, &["events", "raw_events", "tool_io"]);
        assert_eq!(missing, vec!["events", "tool_io"]);
    }

    fn is_migration_filename(name: &str) -> bool {
        // Matches ^\d{3}_.+\.sql$
        let Some(stem) = name.strip_suffix(".sql") else {
            return false;
        };
        if stem.len() < 5 {
            return false;
        }
        let (prefix, rest) = stem.split_at(3);
        prefix.chars().all(|c| c.is_ascii_digit()) && rest.starts_with('_') && rest.len() > 1
    }

    #[test]
    fn bundled_migrations_matches_sql_directory() {
        use std::path::PathBuf;

        let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("sql");

        let mut discovered: Vec<String> = std::fs::read_dir(&sql_dir)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", sql_dir.display()))
            .filter_map(|entry| {
                let entry = entry.ok()?;
                if !entry.file_type().ok()?.is_file() {
                    return None;
                }
                let name = entry.file_name().to_str()?.to_string();
                is_migration_filename(&name).then_some(name)
            })
            .collect();
        discovered.sort();

        assert!(
            !discovered.is_empty(),
            "no migration files found under {}",
            sql_dir.display()
        );

        let migrations = bundled_migrations();
        let bundled_names: Vec<String> = migrations.iter().map(|m| m.name.to_string()).collect();

        assert_eq!(
            bundled_names, discovered,
            "bundled_migrations() is out of sync with sql/*.sql — \
             new migration files must be registered with a matching include_str! entry"
        );

        // bundled_migrations() must be sorted ascending by version.
        let versions: Vec<&str> = migrations.iter().map(|m| m.version).collect();
        let mut sorted = versions.clone();
        sorted.sort();
        assert_eq!(
            versions, sorted,
            "bundled_migrations() must be ordered ascending by version"
        );

        // Each entry's version must match its filename's numeric prefix.
        for m in &migrations {
            assert!(
                m.name.starts_with(&format!("{}_", m.version)),
                "migration name {} does not begin with {}_ prefix",
                m.name,
                m.version
            );
            assert!(
                !m.sql.is_empty(),
                "migration {} has empty bundled sql — include_str! target may be missing",
                m.name
            );
        }
    }

    #[test]
    fn migration_filename_matcher_rejects_non_conforming_names() {
        assert!(is_migration_filename("001_schema.sql"));
        assert!(is_migration_filename("012_add_inference_provider.sql"));
        assert!(!is_migration_filename("001_schema.txt"));
        assert!(!is_migration_filename("schema.sql"));
        assert!(!is_migration_filename("01_schema.sql"));
        assert!(!is_migration_filename("0001_schema.sql"));
        assert!(!is_migration_filename("001schema.sql"));
        assert!(!is_migration_filename("001_.sql"));
        assert!(!is_migration_filename("README.md"));
    }

    #[test]
    fn truncate_for_error_handles_multibyte_utf8_boundaries() {
        let statement = format!("{}é{}", "a".repeat(239), "b".repeat(10));
        let truncated = truncate_for_error(&statement);
        assert_eq!(truncated, format!("{}...", "a".repeat(239)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn query_rows_falls_back_to_json_each_row() {
        #[derive(Deserialize)]
        struct Row {
            value: u8,
        }

        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let rows: Vec<Row> = client
            .query_rows("SELECT 7 AS value", None)
            .await
            .expect("fallback query_rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_includes_status_and_body_on_http_failure() {
        let base_url = spawn_mock_server().await;
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .request_text("SELECT FAIL", None, None, false, None)
            .await
            .expect_err("expected HTTP failure");

        let msg = err.to_string();
        assert!(msg.contains("clickhouse returned"));
        assert!(msg.contains("500"));
        assert!(msg.contains("boom"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn request_text_propagates_response_body_read_errors() {
        let base_url = spawn_truncated_body_server();
        let client = ClickHouseClient::new(test_clickhouse_config(base_url)).expect("new client");

        let err = client
            .request_text("SELECT 1", None, None, false, None)
            .await
            .expect_err("expected response body read failure");

        let msg = err.to_string();
        assert!(msg.contains("failed to read clickhouse response body"));
    }
}
