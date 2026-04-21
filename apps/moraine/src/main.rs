use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use moraine_clickhouse::{
    bundled_migrations, ClickHouseClient, ClickHouseVersionCompatibility, DoctorDeepReport,
    DoctorFinding, DoctorReport, DoctorSeverity,
};
use moraine_config::AppConfig;
use moraine_source_status::{
    build_source_errors_snapshot, build_source_files_snapshot, build_source_status_snapshot,
    SourceErrorsSnapshot, SourceFilesSnapshot, SourceStatusSnapshot,
};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table, Widget, Wrap};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

const CLICKHOUSE_TEMPLATE: &str = include_str!("../../../config/clickhouse.xml");
const USERS_TEMPLATE: &str = include_str!("../../../config/users.xml");
const SEARCH_INDEX_SQL: &str = include_str!("../../../sql/004_search_index.sql");
const SEARCH_CONVERSATION_TERMS_SQL: &str =
    include_str!("../../../sql/010_search_conversation_terms.sql");
const PRIVACY_METADATA_SQL: &str = include_str!("../../../sql/013_privacy_metadata.sql");
const BACKUP_MANIFEST_FILE: &str = "manifest.json";
const BACKUP_TABLES_DIR: &str = "tables";
const BACKUP_MANIFEST_VERSION: u32 = 1;
const DESTRUCTIVE_BACKUP_MAX_AGE_SECONDS: u64 = 24 * 60 * 60;
const DESTRUCTIVE_BACKUP_MAX_AGE_LABEL: &str = "24h";

const DEFAULT_CLICKHOUSE_TAG: &str = "v25.12.5.44-stable";
const CH_URL_MACOS_X86_64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-macos";
const CH_SHA_MACOS_X86_64: &str =
    "8035b4b7905147156192216cc6937a29d0cd2775d481b5f297cdc11058cb68c4";
const CH_URL_MACOS_AARCH64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-macos-aarch64";
const CH_SHA_MACOS_AARCH64: &str =
    "1a0edc37c6e5aa6c06a7cb00c8f8edd83a0df02f643e29185a8b3934eb860ac4";
const CH_URL_LINUX_X86_64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-common-static-25.12.5.44-amd64.tgz";
const CH_SHA_LINUX_X86_64: &str =
    "3756d8b061f97abd79621df1a586f6ba777e8787696f21d82bc488ce5dbca2d7";
const CH_URL_LINUX_AARCH64: &str = "https://github.com/ClickHouse/ClickHouse/releases/download/v25.12.5.44-stable/clickhouse-common-static-25.12.5.44-arm64.tgz";
const CH_SHA_LINUX_AARCH64: &str =
    "3d227e50109b0dab330ee2230f46d76f0360f1a61956443c37de5b7651fb488b";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Auto,
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Rich,
    Plain,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "moraine",
    about = "Unified runtime control plane for Moraine services",
    version = env!("CARGO_PKG_VERSION")
)]
struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Auto)]
    output: OutputFormat,
    #[arg(long, global = true, default_value_t = false)]
    verbose: bool,
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Up(UpArgs),
    Down,
    Status,
    Logs(LogsArgs),
    Db(DbArgs),
    Clickhouse(ClickhouseArgs),
    Config(ConfigArgs),
    Sources(SourcesArgs),
    Import(ImportArgs),
    Archive(ArchiveArgs),
    Backup(BackupArgs),
    Restore(RestoreArgs),
    Reindex(ReindexArgs),
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct UpArgs {
    #[arg(long)]
    no_ingest: bool,
    #[arg(long)]
    monitor: bool,
    #[arg(long)]
    mcp: bool,
    #[arg(long, default_value_t = false)]
    no_backup_check: bool,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(value_enum)]
    service: Option<Service>,
    #[arg(long, default_value_t = 200)]
    lines: usize,
}

#[derive(Debug, Args)]
struct DbArgs {
    #[command(subcommand)]
    command: DbCommand,
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    Migrate(DbMigrateArgs),
    Doctor(DbDoctorArgs),
}

#[derive(Debug, Args)]
struct DbMigrateArgs {
    #[arg(long, default_value_t = false)]
    no_backup_check: bool,
}

#[derive(Debug, Args)]
struct DbDoctorArgs {
    #[arg(long, default_value_t = false)]
    deep: bool,
}

#[derive(Debug, Args)]
struct ClickhouseArgs {
    #[command(subcommand)]
    command: ClickhouseCommand,
}

#[derive(Debug, Subcommand)]
enum ClickhouseCommand {
    Install(ClickhouseInstallArgs),
    Status,
    Uninstall,
}

#[derive(Debug, Args)]
struct ClickhouseInstallArgs {
    #[arg(long)]
    force: bool,
    #[arg(long)]
    version: Option<String>,
}

#[derive(Debug, Args)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    Get(ConfigGetArgs),
    Wizard,
    Detect(ConfigDetectArgs),
    Validate,
}

#[derive(Debug, Args)]
struct SourcesArgs {
    #[command(subcommand)]
    command: SourcesCommand,
}

#[derive(Debug, Subcommand)]
enum SourcesCommand {
    Status(SourcesStatusArgs),
    Files(SourcesFilesArgs),
    Errors(SourcesErrorsArgs),
}

#[derive(Debug, Args)]
struct SourcesStatusArgs {
    #[arg(long, default_value_t = false)]
    include_disabled: bool,
}

#[derive(Debug, Args)]
struct SourcesFilesArgs {
    #[arg(value_name = "SOURCE")]
    source: String,
}

#[derive(Debug, Args)]
struct SourcesErrorsArgs {
    #[arg(value_name = "SOURCE")]
    source: String,
    #[arg(long, default_value_t = 50)]
    limit: u32,
}

#[derive(Debug, Args)]
struct ConfigGetArgs {
    #[arg(value_name = "KEY")]
    key: String,
}

#[derive(Debug, Args)]
struct ConfigDetectArgs {
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Debug, Args)]
struct ImportArgs {
    #[command(subcommand)]
    command: ImportCommand,
}

#[derive(Debug, Subcommand)]
enum ImportCommand {
    Sync(ImportSyncArgs),
    Status,
}

#[derive(Debug, Args)]
struct ImportSyncArgs {
    #[arg(value_name = "NAME")]
    name: String,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    execute: bool,
}

#[derive(Debug, Args)]
struct ArchiveArgs {
    #[command(subcommand)]
    command: ArchiveCommand,
}

#[derive(Debug, Subcommand)]
enum ArchiveCommand {
    Export(ArchiveExportArgs),
    Import(ArchiveImportArgs),
    Verify(ArchiveVerifyArgs),
}

#[derive(Debug, Args)]
struct ArchiveExportArgs {
    #[arg(short, long, value_name = "DIR")]
    out_dir: PathBuf,
    #[arg(long, value_name = "IDS", value_delimiter = ',')]
    session_ids: Option<Vec<String>>,
    #[arg(long, value_name = "DURATION")]
    since: Option<String>,
    #[arg(long, default_value_t = false)]
    raw: bool,
    #[arg(long, default_value_t = false)]
    manifest_only: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    execute: bool,
}

#[derive(Debug, Args)]
struct ArchiveImportArgs {
    #[arg(short, long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    execute: bool,
}

#[derive(Debug, Args)]
struct ArchiveVerifyArgs {
    #[arg(value_name = "DIR")]
    path: PathBuf,
}

#[derive(Debug, Args)]
struct BackupArgs {
    #[command(subcommand)]
    command: BackupCommand,
}

#[derive(Debug, Subcommand)]
enum BackupCommand {
    Create(BackupCreateArgs),
    List(BackupListArgs),
    Verify(BackupVerifyArgs),
}

#[derive(Debug, Args)]
struct BackupCreateArgs {
    #[arg(long, value_name = "DIR")]
    out_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    include_derived: bool,
}

#[derive(Debug, Args)]
struct BackupListArgs {
    #[arg(long, value_name = "DIR")]
    root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct BackupVerifyArgs {
    #[arg(value_name = "DIR")]
    path: PathBuf,
}

#[derive(Debug, Args)]
struct RestoreArgs {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    execute: bool,
    #[arg(long, value_name = "DB")]
    target_database: Option<String>,
}

#[derive(Debug, Args)]
struct ReindexArgs {
    #[arg(long, default_value_t = false)]
    search_only: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
    execute: bool,
    #[arg(long, default_value_t = false)]
    no_backup_check: bool,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(value_enum)]
    service: Service,
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 0..
    )]
    args: Vec<String>,
}

#[derive(Clone)]
struct RuntimePaths {
    root: PathBuf,
    logs_dir: PathBuf,
    pids_dir: PathBuf,
    clickhouse_root: PathBuf,
    clickhouse_config: PathBuf,
    clickhouse_users: PathBuf,
    service_bin_dir: PathBuf,
    managed_clickhouse_dir: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum Service {
    #[value(name = "clickhouse")]
    ClickHouse,
    #[value(name = "ingest")]
    Ingest,
    #[value(name = "monitor")]
    Monitor,
    #[value(name = "mcp")]
    Mcp,
}

impl Service {
    fn name(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Ingest => "ingest",
            Self::Monitor => "monitor",
            Self::Mcp => "mcp",
        }
    }

    fn pid_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.pid",
            Self::Ingest => "ingest.pid",
            Self::Monitor => "monitor.pid",
            Self::Mcp => "mcp.pid",
        }
    }

    fn log_file(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse.log",
            Self::Ingest => "ingest.log",
            Self::Monitor => "monitor.log",
            Self::Mcp => "mcp.log",
        }
    }

    fn binary_name(self) -> Option<&'static str> {
        match self {
            Self::ClickHouse => None,
            Self::Ingest => Some("moraine-ingest"),
            Self::Monitor => Some("moraine-monitor"),
            Self::Mcp => Some("moraine-mcp"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
    #[serde(default)]
    watcher_backend: String,
    #[serde(default)]
    watcher_error_count: u64,
    #[serde(default)]
    watcher_reset_count: u64,
    #[serde(default)]
    watcher_last_reset_unix_ms: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyHeartbeatRow {
    latest: String,
    queue_depth: u64,
    files_active: u64,
}

#[derive(Clone, Copy, Debug)]
struct ClickHouseAsset {
    url: &'static str,
    sha256: &'static str,
    is_archive: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceRuntimeStatus {
    service: Service,
    pid: Option<u32>,
    supervisor: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum HeartbeatSnapshot {
    Available {
        latest: String,
        queue_depth: u64,
        files_active: u64,
        watcher_backend: String,
        watcher_error_count: u64,
        watcher_reset_count: u64,
        watcher_last_reset_unix_ms: u64,
    },
    Unavailable,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
struct StatusSnapshot {
    services: Vec<ServiceRuntimeStatus>,
    monitor_url: Option<String>,
    managed_clickhouse_installed: bool,
    managed_clickhouse_path: String,
    managed_clickhouse_version: Option<String>,
    clickhouse_active_source: String,
    clickhouse_active_source_path: Option<String>,
    managed_clickhouse_checksum: String,
    clickhouse_health_url: String,
    status_notes: Vec<String>,
    doctor: DoctorReport,
    heartbeat: HeartbeatSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MigrationOutcome {
    applied: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
enum DoctorSnapshot {
    Basic(DoctorReport),
    Deep(DoctorDeepReport),
}

#[derive(Debug, Clone, serde::Serialize)]
struct ServiceLogSection {
    service: Service,
    path: String,
    exists: bool,
    lines: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LogsSnapshot {
    requested_lines: usize,
    sections: Vec<ServiceLogSection>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClickhouseStatusSnapshot {
    managed_root: String,
    clickhouse_exists: bool,
    clickhouse_server_exists: bool,
    clickhouse_client_exists: bool,
    expected_version: String,
    active_source: String,
    active_source_path: Option<String>,
    checksum_state: String,
    installed_version: Option<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StartState {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, serde::Serialize)]
struct StartOutcome {
    service: Service,
    state: StartState,
    pid: u32,
    log_path: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct UpSnapshot {
    clickhouse: StartOutcome,
    migrations: MigrationOutcome,
    services: Vec<StartOutcome>,
    status: StatusSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
struct DownSnapshot {
    stopped: Vec<Service>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncManifest {
    profile_name: String,
    synced_at: String,
    source_host: String,
    source_paths: Vec<String>,
    local_mirror: String,
    files_copied: u64,
    bytes_copied: u64,
    files_skipped: u64,
    duration_ms: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncResult {
    profile_name: String,
    success: bool,
    manifest: SyncManifest,
}

#[derive(Debug, Default)]
struct RsyncStats {
    files_copied: u64,
    bytes_copied: u64,
    files_skipped: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ImportStatusSnapshot {
    profiles: Vec<ImportProfileStatus>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ImportProfileStatus {
    name: String,
    configured: bool,
    host: String,
    local_mirror: String,
    cadence: String,
    last_sync: Option<SyncManifest>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArchiveManifest {
    schema_version: String,
    moraine_version: String,
    exported_at: String,
    tables: Vec<ArchiveTableManifest>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArchiveTableManifest {
    name: String,
    rows: u64,
    file: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArchiveExportSnapshot {
    output_dir: String,
    manifest: ArchiveManifest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArchiveImportSnapshot {
    input_dir: String,
    dry_run: bool,
    imported_tables: Vec<ArchiveTableImport>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArchiveTableImport {
    name: String,
    rows: u64,
    file: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupManifest {
    manifest_version: u32,
    backup_id: String,
    created_unix_seconds: u64,
    moraine_version: String,
    clickhouse_database: String,
    clickhouse_version: Option<String>,
    include_derived: bool,
    bundled_migrations: Vec<String>,
    applied_migrations: Vec<String>,
    privacy_key_material: String,
    sources: Vec<BackupSourceInventory>,
    tables: Vec<BackupTableManifest>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupSourceInventory {
    name: String,
    harness: String,
    enabled: bool,
    glob: String,
    watch_root: String,
    format: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupTableManifest {
    name: String,
    kind: String,
    file: String,
    row_count: u64,
    sha256: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupCreateSnapshot {
    backup_dir: String,
    manifest: BackupManifest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupListSnapshot {
    root: String,
    backups: Vec<BackupListEntry>,
    skipped: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupListEntry {
    backup_id: String,
    path: String,
    created_unix_seconds: u64,
    table_count: usize,
    total_rows: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BackupVerifySnapshot {
    path: String,
    ok: bool,
    errors: Vec<String>,
    manifest: Option<BackupManifest>,
}

#[derive(Debug, Clone)]
struct VerifiedBackupSummary {
    backup_id: String,
    path: PathBuf,
    created_unix_seconds: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RestorePlanSnapshot {
    input_dir: String,
    target_database: String,
    dry_run: bool,
    can_restore: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
    table_count: usize,
    total_rows: u64,
}

#[derive(Debug, Clone, Copy)]
struct BackupTableSpec {
    name: &'static str,
    kind: &'static str,
    derived: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ConfigDetectSnapshot {
    sources: Vec<moraine_config::DiscoveredSource>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ConfigValidateSnapshot {
    ok: bool,
    issues: Vec<moraine_config::SourceValidationIssue>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ReindexPreviewCounts {
    events: u64,
    search_documents: u64,
    search_postings: u64,
    search_conversation_terms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ReindexSnapshot {
    mode: String,
    target: String,
    database: String,
    current: ReindexPreviewCounts,
    projected: ReindexPreviewCounts,
    notes: Vec<String>,
}

struct CliOutput {
    mode: OutputMode,
    verbose: bool,
    unicode: bool,
    width: u16,
}

impl CliOutput {
    fn from_cli(cli: &Cli) -> Self {
        let mode = match cli.output {
            OutputFormat::Auto => {
                if std::io::stdout().is_terminal() {
                    OutputMode::Rich
                } else {
                    OutputMode::Plain
                }
            }
            OutputFormat::Rich => OutputMode::Rich,
            OutputFormat::Plain => OutputMode::Plain,
            OutputFormat::Json => OutputMode::Json,
        };
        let unicode = std::env::var("LC_ALL")
            .ok()
            .or_else(|| std::env::var("LANG").ok())
            .map(|v| !v.to_ascii_uppercase().contains("C"))
            .unwrap_or(true);
        let width = std::env::var("COLUMNS")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .map(|v| v.clamp(72, 140))
            .unwrap_or(100);

        Self {
            mode,
            verbose: cli.verbose,
            unicode,
            width,
        }
    }

    fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    fn section(&self, title: &str, lines: &[String]) {
        match self.mode {
            OutputMode::Plain => {
                println!("{title}");
                for line in lines {
                    println!("  {line}");
                }
            }
            OutputMode::Rich => {
                let panel = render_panel(title, lines, self.width, self.unicode);
                println!("{panel}");
            }
            OutputMode::Json => {}
        }
    }

    fn table(&self, title: &str, headers: &[&str], rows: &[Vec<String>]) {
        match self.mode {
            OutputMode::Plain => print_plain_table(title, headers, rows),
            OutputMode::Rich => {
                let table = render_table(title, headers, rows, self.width, self.unicode);
                println!("{table}");
            }
            OutputMode::Json => {}
        }
    }

    fn line(&self, text: &str) {
        if self.mode != OutputMode::Json {
            println!("{text}");
        }
    }
}

fn render_panel(title: &str, lines: &[String], width: u16, unicode: bool) -> String {
    let area = Rect::new(0, 0, width, (lines.len().max(1) as u16).saturating_add(2));
    let mut buffer = Buffer::empty(area);
    let mut block = Block::default()
        .title(Line::from(title.to_string()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));
    if !unicode {
        block = block.border_set(ratatui::symbols::border::PLAIN);
    }
    let paragraph = Paragraph::new(lines.join("\n"))
        .block(block)
        .wrap(Wrap { trim: false })
        .style(Style::default().fg(Color::White));
    paragraph.render(area, &mut buffer);
    buffer_to_string(&buffer)
}

fn render_table(
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    width: u16,
    unicode: bool,
) -> String {
    let area = Rect::new(
        0,
        0,
        width,
        (rows.len().saturating_add(1) as u16).saturating_add(2),
    );
    let mut buffer = Buffer::empty(area);
    let mut block = Block::default()
        .title(Line::from(title.to_string()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));
    if !unicode {
        block = block.border_set(ratatui::symbols::border::PLAIN);
    }

    let header = Row::new(
        headers
            .iter()
            .map(|h| Cell::from((*h).to_string()).style(Style::default().fg(Color::Yellow))),
    )
    .style(Style::default().add_modifier(Modifier::BOLD));
    let data_rows = rows.iter().map(|row| Row::new(row.clone()));
    let widths = headers
        .iter()
        .map(|_| Constraint::Percentage((100 / headers.len().max(1)) as u16))
        .collect::<Vec<_>>();
    let table = Table::new(data_rows, widths).header(header).block(block);
    table.render(area, &mut buffer);
    buffer_to_string(&buffer)
}

fn buffer_to_string(buffer: &Buffer) -> String {
    let mut lines = Vec::new();
    for y in 0..buffer.area.height {
        let mut line = String::new();
        for x in 0..buffer.area.width {
            line.push_str(buffer[(x, y)].symbol());
        }
        while line.ends_with(' ') {
            line.pop();
        }
        lines.push(line);
    }
    while lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    lines.join("\n")
}

fn print_plain_table(title: &str, headers: &[&str], rows: &[Vec<String>]) {
    println!("{title}");
    println!("{}", headers.join(" | "));
    let divider = headers.iter().map(|_| "---").collect::<Vec<_>>().join("+");
    println!("{divider}");
    for row in rows {
        println!("{}", row.join(" | "));
    }
}

fn runtime_paths(cfg: &AppConfig) -> RuntimePaths {
    let root = PathBuf::from(&cfg.runtime.root_dir);
    let clickhouse_root = root.join("clickhouse");

    RuntimePaths {
        root,
        logs_dir: PathBuf::from(&cfg.runtime.logs_dir),
        pids_dir: PathBuf::from(&cfg.runtime.pids_dir),
        clickhouse_config: clickhouse_root.join("config.xml"),
        clickhouse_users: clickhouse_root.join("users.xml"),
        clickhouse_root,
        service_bin_dir: PathBuf::from(&cfg.runtime.service_bin_dir),
        managed_clickhouse_dir: PathBuf::from(&cfg.runtime.managed_clickhouse_dir),
    }
}

fn ensure_runtime_dirs(paths: &RuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.root)
        .with_context(|| format!("failed to create {}", paths.root.display()))?;
    fs::create_dir_all(&paths.logs_dir)
        .with_context(|| format!("failed to create {}", paths.logs_dir.display()))?;
    fs::create_dir_all(&paths.pids_dir)
        .with_context(|| format!("failed to create {}", paths.pids_dir.display()))?;

    fs::create_dir_all(paths.clickhouse_root.join("data"))?;
    fs::create_dir_all(paths.clickhouse_root.join("tmp"))?;
    fs::create_dir_all(paths.clickhouse_root.join("log"))?;
    fs::create_dir_all(paths.clickhouse_root.join("user_files"))?;
    fs::create_dir_all(paths.clickhouse_root.join("format_schemas"))?;

    Ok(())
}

fn pid_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    paths.pids_dir.join(service.pid_file())
}

fn clickhouse_internal_log_path(paths: &RuntimePaths) -> PathBuf {
    paths
        .clickhouse_root
        .join("log")
        .join("clickhouse-server.log")
}

fn legacy_clickhouse_pipe_log_path(paths: &RuntimePaths) -> PathBuf {
    paths.logs_dir.join(Service::ClickHouse.log_file())
}

fn cleanup_legacy_clickhouse_pipe_log(paths: &RuntimePaths) {
    let legacy_log = legacy_clickhouse_pipe_log_path(paths);
    let should_remove = fs::metadata(&legacy_log)
        .map(|metadata| metadata.is_file())
        .unwrap_or(false);
    if should_remove {
        let _ = fs::remove_file(legacy_log);
    }
}

fn log_path(paths: &RuntimePaths, service: Service) -> PathBuf {
    match service {
        Service::ClickHouse => clickhouse_internal_log_path(paths),
        Service::Ingest | Service::Monitor | Service::Mcp => {
            paths.logs_dir.join(service.log_file())
        }
    }
}

fn read_pid(path: &Path) -> Option<u32> {
    let text = fs::read_to_string(path).ok()?;
    text.trim().parse::<u32>().ok()
}

fn write_pid(path: &Path, pid: u32) -> Result<()> {
    fs::write(path, format!("{}\n", pid))
        .with_context(|| format!("failed to write pid file {}", path.display()))
}

fn is_pid_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_pid_fresh(path: &Path) {
    if let Some(pid) = read_pid(path) {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(path);
        }
    }
}

fn service_running(paths: &RuntimePaths, service: Service) -> Option<u32> {
    let path = pid_path(paths, service);
    ensure_pid_fresh(&path);
    let pid = read_pid(&path)?;
    if is_pid_running(pid) {
        Some(pid)
    } else {
        None
    }
}

fn pid_file_runtime_status(paths: &RuntimePaths, service: Service) -> Option<ServiceRuntimeStatus> {
    service_running(paths, service).map(|pid| ServiceRuntimeStatus {
        service,
        pid: Some(pid),
        supervisor: "pid_file".to_string(),
    })
}

fn parse_launchctl_pid(output: &str) -> Option<u32> {
    output.lines().find_map(|line| {
        let value = line.trim().strip_prefix("pid = ")?;
        value.trim().parse::<u32>().ok()
    })
}

#[cfg(target_os = "macos")]
fn launchd_runtime_status(service: Service) -> Option<ServiceRuntimeStatus> {
    let uid = Command::new("id").arg("-u").output().ok()?;
    if !uid.status.success() {
        return None;
    }
    let uid = String::from_utf8_lossy(&uid.stdout).trim().to_string();
    if uid.is_empty() {
        return None;
    }

    let label = format!("local.moraine.{}", service.name());
    let target = format!("gui/{uid}/{label}");
    let output = Command::new("launchctl")
        .arg("print")
        .arg(&target)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    parse_launchctl_pid(&text).and_then(|pid| {
        is_pid_running(pid).then(|| ServiceRuntimeStatus {
            service,
            pid: Some(pid),
            supervisor: format!("launchd:{label}"),
        })
    })
}

#[cfg(not(target_os = "macos"))]
fn launchd_runtime_status(_service: Service) -> Option<ServiceRuntimeStatus> {
    None
}

fn service_runtime_status(paths: &RuntimePaths, service: Service) -> ServiceRuntimeStatus {
    pid_file_runtime_status(paths, service)
        .or_else(|| launchd_runtime_status(service))
        .unwrap_or(ServiceRuntimeStatus {
            service,
            pid: None,
            supervisor: "none".to_string(),
        })
}

fn stop_service(paths: &RuntimePaths, service: Service) -> Result<bool> {
    let path = pid_path(paths, service);
    let Some(pid) = read_pid(&path) else {
        return Ok(false);
    };

    if !is_pid_running(pid) {
        let _ = fs::remove_file(path);
        return Ok(false);
    }

    let _ = Command::new("kill")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    for _ in 0..20 {
        if !is_pid_running(pid) {
            let _ = fs::remove_file(&path);
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = fs::remove_file(path);
    Ok(true)
}

fn parse_config_flag(args: &[String]) -> Result<(Option<PathBuf>, Vec<String>)> {
    let mut raw_config = None;
    let mut rest = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 >= args.len() {
                bail!("--config requires a path");
            }
            raw_config = Some(PathBuf::from(args[i + 1].clone()));
            i += 2;
            continue;
        }

        rest.push(args[i].clone());
        i += 1;
    }

    Ok((raw_config, rest))
}

fn load_cfg(raw_config: Option<PathBuf>) -> Result<(PathBuf, AppConfig)> {
    let config_path = moraine_config::resolve_config_path(raw_config);
    let cfg = moraine_config::load_config(&config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;
    Ok((config_path, cfg))
}

fn clickhouse_ports_from_url(cfg: &AppConfig) -> Result<(u16, u16, u16)> {
    let parsed = reqwest::Url::parse(&cfg.clickhouse.url)
        .with_context(|| format!("invalid clickhouse.url '{}'", cfg.clickhouse.url))?;
    let http_port = parsed.port_or_known_default().ok_or_else(|| {
        anyhow!(
            "clickhouse.url '{}' must include a known port",
            cfg.clickhouse.url
        )
    })?;
    let tcp_port = http_port
        .checked_add(877)
        .ok_or_else(|| anyhow!("derived clickhouse tcp port overflow from {}", http_port))?;
    let interserver_http_port = http_port.checked_add(886).ok_or_else(|| {
        anyhow!(
            "derived clickhouse interserver port overflow from {}",
            http_port
        )
    })?;
    Ok((http_port, tcp_port, interserver_http_port))
}

fn materialize_clickhouse_config(cfg: &AppConfig, paths: &RuntimePaths) -> Result<()> {
    let (http_port, tcp_port, interserver_http_port) = clickhouse_ports_from_url(cfg)?;
    let rendered_clickhouse = CLICKHOUSE_TEMPLATE
        .replace("__MORAINE_HOME__", &cfg.runtime.root_dir)
        .replace("__CLICKHOUSE_HTTP_PORT__", &http_port.to_string())
        .replace("__CLICKHOUSE_TCP_PORT__", &tcp_port.to_string())
        .replace(
            "__CLICKHOUSE_INTERSERVER_HTTP_PORT__",
            &interserver_http_port.to_string(),
        );
    let rendered_users = USERS_TEMPLATE.replace("__MORAINE_HOME__", &cfg.runtime.root_dir);

    fs::write(&paths.clickhouse_config, rendered_clickhouse).with_context(|| {
        format!(
            "failed writing clickhouse config {}",
            paths.clickhouse_config.display()
        )
    })?;
    fs::write(&paths.clickhouse_users, rendered_users).with_context(|| {
        format!(
            "failed writing users config {}",
            paths.clickhouse_users.display()
        )
    })?;

    Ok(())
}

fn managed_clickhouse_bin(paths: &RuntimePaths, binary: &str) -> PathBuf {
    paths.managed_clickhouse_dir.join("bin").join(binary)
}

fn managed_clickhouse_checksum_file(paths: &RuntimePaths) -> PathBuf {
    paths.managed_clickhouse_dir.join("SHA256")
}

fn detect_host_target() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        (os, arch) => bail!(
            "unsupported platform for managed ClickHouse: {} {}",
            os,
            arch
        ),
    }
}

fn clickhouse_asset_for_target(version: &str, target: &str) -> Result<ClickHouseAsset> {
    if version != DEFAULT_CLICKHOUSE_TAG {
        bail!(
            "unsupported ClickHouse version {}; this build supports {}",
            version,
            DEFAULT_CLICKHOUSE_TAG
        );
    }

    match target {
        "x86_64-apple-darwin" => Ok(ClickHouseAsset {
            url: CH_URL_MACOS_X86_64,
            sha256: CH_SHA_MACOS_X86_64,
            is_archive: false,
        }),
        "aarch64-apple-darwin" => Ok(ClickHouseAsset {
            url: CH_URL_MACOS_AARCH64,
            sha256: CH_SHA_MACOS_AARCH64,
            is_archive: false,
        }),
        "x86_64-unknown-linux-gnu" => Ok(ClickHouseAsset {
            url: CH_URL_LINUX_X86_64,
            sha256: CH_SHA_LINUX_X86_64,
            is_archive: true,
        }),
        "aarch64-unknown-linux-gnu" => Ok(ClickHouseAsset {
            url: CH_URL_LINUX_AARCH64,
            sha256: CH_SHA_LINUX_AARCH64,
            is_archive: true,
        }),
        other => bail!("unsupported ClickHouse target: {}", other),
    }
}

fn clickhouse_asset_for_host(version: &str) -> Result<ClickHouseAsset> {
    clickhouse_asset_for_target(version, detect_host_target()?)
}

async fn download_to_path(url: &str, dest: &Path, label: &str) -> Result<()> {
    let client = Client::new();
    let mut response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download {}", url))?
        .error_for_status()
        .with_context(|| format!("download failed for {}", url))?;

    let total = response.content_length();
    let show_progress = std::io::stderr().is_terminal();

    let mut file = std::fs::File::create(dest)
        .with_context(|| format!("failed writing {}", dest.display()))?;
    let mut downloaded: u64 = 0;
    let mut last_render = Instant::now();

    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("failed reading response body for {}", url))?
    {
        file.write_all(&chunk)
            .with_context(|| format!("failed writing {}", dest.display()))?;
        downloaded += chunk.len() as u64;

        if show_progress && last_render.elapsed() >= Duration::from_millis(150) {
            render_download_progress(label, downloaded, total, false);
            last_render = Instant::now();
        }
    }

    if show_progress {
        render_download_progress(label, downloaded, total, true);
    }

    Ok(())
}

fn render_download_progress(label: &str, done: u64, total: Option<u64>, done_flag: bool) {
    const MIB: f64 = 1024.0 * 1024.0;
    let done_mb = done as f64 / MIB;
    match total {
        Some(t) if t > 0 => {
            let total_mb = t as f64 / MIB;
            let pct = ((done as f64 / t as f64) * 100.0).min(100.0);
            eprint!("\r  {label}: {done_mb:>6.1} / {total_mb:>6.1} MiB  ({pct:>5.1}%)");
        }
        _ => {
            eprint!("\r  {label}: {done_mb:>6.1} MiB");
        }
    }
    if done_flag {
        eprintln!();
    } else {
        std::io::stderr().flush().ok();
    }
}

fn sha256_hex(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("failed opening {} for checksum", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{:02x}", b)).collect())
}

fn path_ends_with_components(path: &Path, suffix: &[&str]) -> bool {
    let mut components = path.components().rev();
    for expected in suffix.iter().rev() {
        match components
            .next()
            .and_then(|component| component.as_os_str().to_str())
        {
            Some(component) if component == *expected => {}
            _ => return false,
        }
    }

    true
}

fn find_file_ending_with(root: &Path, suffix: &[&str]) -> Result<Option<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path_ends_with_components(&path, suffix) {
                return Ok(Some(path));
            }
        }
    }

    Ok(None)
}

fn find_file_named(root: &Path, name: &str) -> Result<Option<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s == name)
            {
                return Ok(Some(path));
            }
        }
    }

    Ok(None)
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_symlink(target: &Path, link: &Path) -> Result<()> {
    if link.exists() {
        let _ = fs::remove_file(link);
    }
    symlink(target, link).with_context(|| {
        format!(
            "failed creating symlink {} -> {}",
            link.display(),
            target.display()
        )
    })
}

#[cfg(not(unix))]
fn ensure_symlink(target: &Path, link: &Path) -> Result<()> {
    fs::copy(target, link)?;
    Ok(())
}

async fn install_managed_clickhouse(
    paths: &RuntimePaths,
    version: &str,
    force: bool,
) -> Result<PathBuf> {
    let asset = clickhouse_asset_for_host(version)?;

    let bin_dir = paths.managed_clickhouse_dir.join("bin");
    let clickhouse = bin_dir.join("clickhouse");
    let clickhouse_server = bin_dir.join("clickhouse-server");

    if clickhouse_server.exists() && !force {
        return Ok(clickhouse_server);
    }

    fs::create_dir_all(paths.root.join("tmp"))
        .with_context(|| format!("failed to create {}", paths.root.join("tmp").display()))?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let download = paths
        .root
        .join("tmp")
        .join(format!("clickhouse-download-{}", stamp));
    let extract_dir = paths
        .root
        .join("tmp")
        .join(format!("clickhouse-extract-{}", stamp));

    download_to_path(asset.url, &download, &format!("ClickHouse {version}")).await?;

    let digest = sha256_hex(&download)?;
    if digest != asset.sha256 {
        bail!(
            "managed ClickHouse checksum mismatch: expected {}, got {}",
            asset.sha256,
            digest
        );
    }

    fs::create_dir_all(&extract_dir)?;

    let staged_binary = if asset.is_archive {
        let status = Command::new("tar")
            .env("LC_ALL", "C")
            .arg("-xzf")
            .arg(&download)
            .arg("-C")
            .arg(&extract_dir)
            .status()
            .context("failed to run tar while installing ClickHouse")?;
        if !status.success() {
            bail!("failed to extract ClickHouse archive");
        }

        find_file_ending_with(&extract_dir, &["usr", "bin", "clickhouse"])?
            .or(find_file_named(&extract_dir, "clickhouse")?)
            .ok_or_else(|| anyhow!("extracted ClickHouse archive missing clickhouse binary"))?
    } else {
        download.clone()
    };

    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("failed creating {}", bin_dir.display()))?;

    fs::copy(&staged_binary, &clickhouse)
        .with_context(|| format!("failed writing {}", clickhouse.display()))?;
    make_executable(&clickhouse)?;

    ensure_symlink(&clickhouse, &clickhouse_server)?;
    ensure_symlink(&clickhouse, &bin_dir.join("clickhouse-client"))?;

    fs::write(
        paths.managed_clickhouse_dir.join("VERSION"),
        format!("{}\n", version),
    )?;
    fs::write(
        managed_clickhouse_checksum_file(paths),
        format!("{}\n", digest),
    )?;

    let _ = fs::remove_file(download);
    let _ = fs::remove_dir_all(extract_dir);

    Ok(clickhouse_server)
}

fn clickhouse_from_path_available() -> bool {
    Command::new("clickhouse-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn resolve_clickhouse_server_command(
    cfg: &AppConfig,
    paths: &RuntimePaths,
) -> Result<PathBuf> {
    let managed = managed_clickhouse_bin(paths, "clickhouse-server");
    if managed.exists() {
        return Ok(managed);
    }

    if clickhouse_from_path_available() {
        return Ok(PathBuf::from("clickhouse-server"));
    }

    let version = &cfg.runtime.clickhouse_version;
    let should_install = if cfg.runtime.clickhouse_auto_install {
        eprintln!(
            "managed ClickHouse not found; auto-installing {version}.\n\
             one-time ~175 MiB download + extract (progress shown below).\n\
             set runtime.clickhouse_auto_install = false in your config to disable.",
        );
        true
    } else {
        prompt_install_clickhouse(version)?
    };

    if should_install {
        install_managed_clickhouse(paths, version, false).await?;
        let managed = managed_clickhouse_bin(paths, "clickhouse-server");
        if managed.exists() {
            return Ok(managed);
        }
    }

    bail!(
        "clickhouse-server is not installed or not on PATH (managed install dir: {})",
        paths.managed_clickhouse_dir.display()
    )
}

fn prompt_install_clickhouse(version: &str) -> Result<bool> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() || !std::io::stderr().is_terminal() {
        bail!(
            "managed ClickHouse is not installed and runtime.clickhouse_auto_install = false.\n\
             cannot prompt in non-interactive mode.\n\
             remediation:\n\
             - run `moraine clickhouse install` explicitly, or\n\
             - set runtime.clickhouse_auto_install = true in your config"
        );
    }

    loop {
        eprint!("managed ClickHouse {version} is not installed. install now? [Y/n] ");
        std::io::stderr().flush().ok();

        let mut input = String::new();
        stdin
            .read_line(&mut input)
            .context("failed to read confirmation from stdin")?;

        match input.trim().to_ascii_lowercase().as_str() {
            "" | "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("please answer 'y' or 'n'."),
        }
    }
}

async fn wait_for_clickhouse(cfg: &AppConfig) -> Result<()> {
    let client = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let timeout = Duration::from_secs_f64(cfg.runtime.clickhouse_start_timeout_seconds.max(1.0));
    let interval = Duration::from_millis(cfg.runtime.healthcheck_interval_ms.max(100));
    let start = Instant::now();

    loop {
        if client.ping().await.is_ok() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "clickhouse did not become healthy within {:.1}s",
                timeout.as_secs_f64()
            );
        }

        sleep(interval).await;
    }
}

async fn start_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<StartOutcome> {
    if let Some(pid) = service_running(paths, Service::ClickHouse) {
        return Ok(StartOutcome {
            service: Service::ClickHouse,
            state: StartState::AlreadyRunning,
            pid,
            log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
        });
    }

    cleanup_legacy_clickhouse_pipe_log(paths);

    let server_bin = resolve_clickhouse_server_command(cfg, paths).await?;

    materialize_clickhouse_config(cfg, paths)?;

    let child = Command::new(&server_bin)
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to start {}", server_bin.display()))?;

    write_pid(&pid_path(paths, Service::ClickHouse), child.id())?;

    wait_for_clickhouse(cfg).await?;
    Ok(StartOutcome {
        service: Service::ClickHouse,
        state: StartState::Started,
        pid: child.id(),
        log_path: Some(log_path(paths, Service::ClickHouse).display().to_string()),
    })
}

async fn run_foreground_clickhouse(cfg: &AppConfig, paths: &RuntimePaths) -> Result<ExitCode> {
    ensure_runtime_dirs(paths)?;
    let server_bin = resolve_clickhouse_server_command(cfg, paths).await?;
    materialize_clickhouse_config(cfg, paths)?;

    let status = Command::new(server_bin)
        .arg("--config-file")
        .arg(&paths.clickhouse_config)
        .status()
        .context("failed to run clickhouse-server")?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

fn env_flag_enabled(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn source_tree_mode_enabled() -> bool {
    env_flag_enabled("MORAINE_SOURCE_TREE_MODE")
}

#[derive(Debug, Clone)]
struct ServiceBinaryProbe {
    source: &'static str,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct ServiceBinaryResolution {
    binary_name: String,
    resolved_path: Option<PathBuf>,
    checked_paths: Vec<ServiceBinaryProbe>,
}

fn resolve_service_binary(service: Service, paths: &RuntimePaths) -> ServiceBinaryResolution {
    let name = service.binary_name().unwrap_or(service.name()).to_string();
    let mut checked_paths = Vec::new();

    let mut check = |source: &'static str, path: PathBuf| {
        if path.exists() {
            Some(path)
        } else {
            checked_paths.push(ServiceBinaryProbe { source, path });
            None
        }
    };

    if let Ok(dir) = std::env::var("MORAINE_SERVICE_BIN_DIR") {
        if let Some(path) = check("MORAINE_SERVICE_BIN_DIR", PathBuf::from(dir).join(&name)) {
            return ServiceBinaryResolution {
                binary_name: name,
                resolved_path: Some(path),
                checked_paths,
            };
        }
    }

    if let Some(path) = check("runtime.service_bin_dir", paths.service_bin_dir.join(&name)) {
        return ServiceBinaryResolution {
            binary_name: name,
            resolved_path: Some(path),
            checked_paths,
        };
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if let Some(path) = check("moraine sibling", dir.join(&name)) {
                return ServiceBinaryResolution {
                    binary_name: name,
                    resolved_path: Some(path),
                    checked_paths,
                };
            }
        }
    }

    if source_tree_mode_enabled() {
        if let Some(project_bin) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("target").join("debug").join(&name))
        {
            if let Some(path) = check("source-tree mode target/debug", project_bin) {
                return ServiceBinaryResolution {
                    binary_name: name,
                    resolved_path: Some(path),
                    checked_paths,
                };
            }
        }
    }

    ServiceBinaryResolution {
        binary_name: name,
        resolved_path: None,
        checked_paths,
    }
}

fn require_service_binary(service: Service, paths: &RuntimePaths) -> Result<PathBuf> {
    let resolution = resolve_service_binary(service, paths);
    if let Some(path) = resolution.resolved_path {
        return Ok(path);
    }

    let checked = if resolution.checked_paths.is_empty() {
        "- (no probe paths)".to_string()
    } else {
        resolution
            .checked_paths
            .iter()
            .map(|probe| format!("- {} ({})", probe.path.display(), probe.source))
            .collect::<Vec<_>>()
            .join("\n")
    };

    bail!(
        "required service binary `{}` for `{}` was not found.\nchecked:\n{}\nremediation:\n- install Moraine service binaries so `{}` exists under `runtime.service_bin_dir` (`{}`)\n- or set `MORAINE_SERVICE_BIN_DIR` to a directory containing `{}`\n- for source builds run `cargo build --workspace --locked` and set `MORAINE_SOURCE_TREE_MODE=1`\n`moraine` does not fall back to PATH for service binaries.",
        resolution.binary_name,
        service.name(),
        checked,
        resolution.binary_name,
        paths.service_bin_dir.display(),
        resolution.binary_name
    );
}

fn preflight_required_service_binaries(services: &[Service], paths: &RuntimePaths) -> Result<()> {
    for service in services {
        if *service == Service::ClickHouse {
            continue;
        }
        require_service_binary(*service, paths)?;
    }
    Ok(())
}

fn contains_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn monitor_dist_candidate(root: &Path) -> PathBuf {
    root.join("web").join("monitor").join("dist")
}

const MONITOR_DIST_ENV_KEYS: &[&str] = &["MORAINE_MONITOR_DIST", "MORAINE_MONITOR_STATIC_DIR"];

fn resolve_monitor_static_dir(paths: &RuntimePaths) -> Option<PathBuf> {
    for key in MONITOR_DIST_ENV_KEYS {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let path = PathBuf::from(trimmed);
            if path.exists() {
                return Some(path);
            }
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(bin_dir) = exe.parent() {
            if let Some(bundle_root) = bin_dir.parent() {
                let candidate = monitor_dist_candidate(bundle_root);
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
    }

    if let Some(bundle_root) = paths.service_bin_dir.parent() {
        let candidate = monitor_dist_candidate(bundle_root);
        if candidate.exists() {
            return Some(candidate);
        }
    }

    if source_tree_mode_enabled() {
        if let Some(dev_path) = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
        {
            let candidate = monitor_dist_candidate(&dev_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    None
}

fn service_args_with_defaults(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    passthrough: &[String],
) -> Vec<String> {
    let mut args = Vec::new();

    if !contains_flag(passthrough, "--config") {
        args.push("--config".to_string());
        args.push(cfg_path.to_string_lossy().to_string());
    }

    if service == Service::Monitor {
        if !contains_flag(passthrough, "--host") {
            args.push("--host".to_string());
            args.push(cfg.monitor.host.clone());
        }
        if !contains_flag(passthrough, "--port") {
            args.push("--port".to_string());
            args.push(cfg.monitor.port.to_string());
        }
        if !contains_flag(passthrough, "--static-dir") {
            if let Some(static_dir) = resolve_monitor_static_dir(paths) {
                args.push("--static-dir".to_string());
                args.push(static_dir.to_string_lossy().to_string());
            }
        }
    }

    args.extend(passthrough.iter().cloned());
    args
}

fn start_background_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    extra_args: &[String],
) -> Result<StartOutcome> {
    if service == Service::ClickHouse {
        bail!("clickhouse is not managed by service launcher; use `moraine up`");
    }

    if let Some(pid) = service_running(paths, service) {
        return Ok(StartOutcome {
            service,
            state: StartState::AlreadyRunning,
            pid,
            log_path: Some(log_path(paths, service).display().to_string()),
        });
    }

    let binary = require_service_binary(service, paths)?;

    let logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(paths, service))
        .with_context(|| format!("failed to open {} log", service.name()))?;
    let logfile_err = logfile
        .try_clone()
        .with_context(|| format!("failed to clone {} log", service.name()))?;

    let args = service_args_with_defaults(service, cfg_path, cfg, paths, extra_args);

    let child = Command::new(&binary)
        .args(args)
        .stdout(Stdio::from(logfile))
        .stderr(Stdio::from(logfile_err))
        .spawn()
        .with_context(|| format!("failed to start {}", service.name()))?;

    write_pid(&pid_path(paths, service), child.id())?;
    Ok(StartOutcome {
        service,
        state: StartState::Started,
        pid: child.id(),
        log_path: Some(log_path(paths, service).display().to_string()),
    })
}

async fn run_foreground_service(
    service: Service,
    cfg_path: &Path,
    cfg: &AppConfig,
    paths: &RuntimePaths,
    passthrough_args: &[String],
) -> Result<ExitCode> {
    if service == Service::ClickHouse {
        return run_foreground_clickhouse(cfg, paths).await;
    }

    let binary = require_service_binary(service, paths)?;
    let args = service_args_with_defaults(service, cfg_path, cfg, paths, passthrough_args);

    let status = Command::new(binary)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {}", service.name()))?;

    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

async fn cmd_db_migrate(cfg: &AppConfig) -> Result<MigrationOutcome> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let applied = ch.run_migrations().await?;
    Ok(MigrationOutcome { applied })
}

async fn cmd_db_doctor(cfg: &AppConfig, deep: bool) -> Result<DoctorSnapshot> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    if deep {
        Ok(DoctorSnapshot::Deep(ch.doctor_deep_report().await?))
    } else {
        Ok(DoctorSnapshot::Basic(ch.doctor_report().await?))
    }
}

async fn query_heartbeat(cfg: &AppConfig) -> Result<Option<HeartbeatRow>> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let db = quote_identifier(&cfg.clickhouse.database);
    let query = format!(
        "SELECT \
            toString(max(ts)) AS latest, \
            toUInt64(argMax(queue_depth, ts)) AS queue_depth, \
            toUInt64(argMax(files_active, ts)) AS files_active, \
            toString(argMax(watcher_backend, ts)) AS watcher_backend, \
            toUInt64(argMax(watcher_error_count, ts)) AS watcher_error_count, \
            toUInt64(argMax(watcher_reset_count, ts)) AS watcher_reset_count, \
            toUInt64(argMax(watcher_last_reset_unix_ms, ts)) AS watcher_last_reset_unix_ms \
         FROM {db}.ingest_heartbeats"
    );

    match ch.query_json_data::<HeartbeatRow>(&query, None).await {
        Ok(rows) => Ok(rows.into_iter().next()),
        Err(_) => {
            let legacy_query = format!(
                "SELECT toString(max(ts)) AS latest, toUInt64(argMax(queue_depth, ts)) AS queue_depth, toUInt64(argMax(files_active, ts)) AS files_active FROM {db}.ingest_heartbeats"
            );
            let rows: Vec<LegacyHeartbeatRow> = ch.query_json_data(&legacy_query, None).await?;
            Ok(rows.into_iter().next().map(|row| HeartbeatRow {
                latest: row.latest,
                queue_depth: row.queue_depth,
                files_active: row.files_active,
                watcher_backend: "unknown".to_string(),
                watcher_error_count: 0,
                watcher_reset_count: 0,
                watcher_last_reset_unix_ms: 0,
            }))
        }
    }
}

async fn cmd_sources_status(
    cfg: &AppConfig,
    include_disabled: bool,
) -> Result<SourceStatusSnapshot> {
    build_source_status_snapshot(cfg, include_disabled).await
}

async fn cmd_sources_files(cfg: &AppConfig, source: &str) -> Result<SourceFilesSnapshot> {
    build_source_files_snapshot(cfg, source).await
}

async fn cmd_sources_errors(
    cfg: &AppConfig,
    source: &str,
    limit: u32,
) -> Result<SourceErrorsSnapshot> {
    build_source_errors_snapshot(cfg, source, limit).await
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn managed_clickhouse_version(paths: &RuntimePaths) -> Option<String> {
    fs::read_to_string(paths.managed_clickhouse_dir.join("VERSION"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn managed_clickhouse_checksum(paths: &RuntimePaths) -> Option<String> {
    fs::read_to_string(managed_clickhouse_checksum_file(paths))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn managed_clickhouse_checksum_state(cfg: &AppConfig, paths: &RuntimePaths) -> String {
    let Some(stored) = managed_clickhouse_checksum(paths) else {
        return "unknown (missing checksum metadata)".to_string();
    };

    let expected = match clickhouse_asset_for_host(&cfg.runtime.clickhouse_version) {
        Ok(asset) => asset.sha256,
        Err(exc) => return format!("unknown ({})", exc),
    };

    if stored == expected {
        "verified".to_string()
    } else {
        format!("mismatch (expected {}, got {})", expected, stored)
    }
}

fn active_clickhouse_source(paths: &RuntimePaths) -> (&'static str, Option<PathBuf>) {
    let managed = managed_clickhouse_bin(paths, "clickhouse-server");
    if managed.exists() {
        return ("managed", Some(managed));
    }
    if clickhouse_from_path_available() {
        return ("path", Some(PathBuf::from("clickhouse-server")));
    }
    ("missing", None)
}

fn service_runtime_running(services: &[ServiceRuntimeStatus], service: Service) -> bool {
    services
        .iter()
        .find(|row| row.service == service)
        .and_then(|row| row.pid)
        .is_some()
}

fn clickhouse_runtime_running(services: &[ServiceRuntimeStatus]) -> bool {
    service_runtime_running(services, Service::ClickHouse)
}

fn monitor_runtime_running(services: &[ServiceRuntimeStatus]) -> bool {
    service_runtime_running(services, Service::Monitor)
}

fn format_http_url(host: &str, port: u16) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

fn monitor_runtime_url(cfg: &AppConfig) -> String {
    format_http_url(&cfg.monitor.host, cfg.monitor.port)
}

fn build_status_notes(
    services: &[ServiceRuntimeStatus],
    report: &DoctorReport,
    clickhouse_url: &str,
) -> Vec<String> {
    let clickhouse_running = clickhouse_runtime_running(services);
    let mut notes = Vec::new();

    if report.clickhouse_healthy && !clickhouse_running {
        notes.push(format!(
            "database health checks query clickhouse.url ({clickhouse_url}); endpoint is healthy while managed clickhouse runtime is stopped"
        ));
    }

    if !report.clickhouse_healthy && clickhouse_running {
        notes.push(format!(
            "managed clickhouse runtime is running, but health checks against clickhouse.url ({clickhouse_url}) are failing"
        ));
    }

    notes
}

async fn cmd_status(paths: &RuntimePaths, cfg: &AppConfig) -> Result<StatusSnapshot> {
    let services = [
        Service::ClickHouse,
        Service::Ingest,
        Service::Monitor,
        Service::Mcp,
    ]
    .iter()
    .copied()
    .map(|service| service_runtime_status(paths, service))
    .collect::<Vec<_>>();
    let managed_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let (source, source_path) = active_clickhouse_source(paths);
    let report = match cmd_db_doctor(cfg, false).await? {
        DoctorSnapshot::Basic(report) => report,
        DoctorSnapshot::Deep(report) => report.report,
    };
    let clickhouse_health_url = cfg.clickhouse.url.clone();
    let status_notes = build_status_notes(&services, &report, &clickhouse_health_url);
    let monitor_url = monitor_runtime_running(&services).then(|| monitor_runtime_url(cfg));
    let heartbeat = match query_heartbeat(cfg).await {
        Ok(Some(row)) => HeartbeatSnapshot::Available {
            latest: row.latest,
            queue_depth: row.queue_depth,
            files_active: row.files_active,
            watcher_backend: row.watcher_backend,
            watcher_error_count: row.watcher_error_count,
            watcher_reset_count: row.watcher_reset_count,
            watcher_last_reset_unix_ms: row.watcher_last_reset_unix_ms,
        },
        Ok(None) => HeartbeatSnapshot::Unavailable,
        Err(err) => HeartbeatSnapshot::Error {
            message: err.to_string(),
        },
    };

    Ok(StatusSnapshot {
        services,
        monitor_url,
        managed_clickhouse_installed: managed_server.exists(),
        managed_clickhouse_path: managed_server.display().to_string(),
        managed_clickhouse_version: managed_clickhouse_version(paths),
        clickhouse_active_source: source.to_string(),
        clickhouse_active_source_path: source_path.map(|path| path.display().to_string()),
        managed_clickhouse_checksum: managed_clickhouse_checksum_state(cfg, paths),
        clickhouse_health_url,
        status_notes,
        doctor: report,
        heartbeat,
    })
}

fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    const TAIL_READ_CHUNK_BYTES: usize = 8 * 1024;

    if lines == 0 {
        return Ok(Vec::new());
    }

    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to read log file {}", path.display()))?;
    let mut position = file
        .metadata()
        .with_context(|| format!("failed to read log file {}", path.display()))?
        .len();

    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut scratch = vec![0_u8; TAIL_READ_CHUNK_BYTES];
    let mut newline_count = 0usize;
    while position > 0 {
        let read_len = (position as usize).min(TAIL_READ_CHUNK_BYTES);
        position -= read_len as u64;
        file.seek(SeekFrom::Start(position))
            .with_context(|| format!("failed to read log file {}", path.display()))?;
        file.read_exact(&mut scratch[..read_len])
            .with_context(|| format!("failed to read log file {}", path.display()))?;
        newline_count += scratch[..read_len]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count();
        chunks.push(scratch[..read_len].to_vec());
        if newline_count > lines {
            break;
        }
    }

    let total_len = chunks.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks.iter().rev() {
        bytes.extend_from_slice(chunk);
    }

    let start = if position > 0 {
        bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |idx| idx + 1)
    } else {
        0
    };
    let content = std::str::from_utf8(&bytes[start..])
        .with_context(|| format!("failed to decode log file {} as utf-8", path.display()))?;
    let mut collected = content
        .lines()
        .rev()
        .take(lines)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    collected.reverse();
    Ok(collected)
}

fn collect_logs(
    paths: &RuntimePaths,
    service: Option<Service>,
    lines: usize,
) -> Result<LogsSnapshot> {
    let targets = match service {
        Some(svc) => vec![svc],
        None => vec![
            Service::ClickHouse,
            Service::Ingest,
            Service::Monitor,
            Service::Mcp,
        ],
    };

    let mut sections = Vec::new();
    for svc in targets {
        let path = log_path(paths, svc);
        let path_string = path.display().to_string();
        if !path.exists() {
            sections.push(ServiceLogSection {
                service: svc,
                path: path_string,
                exists: false,
                lines: Vec::new(),
            });
            continue;
        }
        sections.push(ServiceLogSection {
            service: svc,
            path: path_string,
            exists: true,
            lines: tail_lines(&path, lines)?,
        });
    }

    Ok(LogsSnapshot {
        requested_lines: lines,
        sections,
    })
}

fn selected_up_services(args: &UpArgs, cfg: &AppConfig) -> Vec<Service> {
    let mut services = Vec::new();
    if !args.no_ingest {
        services.push(Service::Ingest);
    }
    if args.monitor || cfg.runtime.start_monitor_on_up {
        services.push(Service::Monitor);
    }
    if args.mcp || cfg.runtime.start_mcp_on_up {
        services.push(Service::Mcp);
    }
    services
}

async fn cmd_clickhouse_install(
    paths: &RuntimePaths,
    version: &str,
    force: bool,
) -> Result<PathBuf> {
    ensure_runtime_dirs(paths)?;
    let installed = install_managed_clickhouse(paths, version, force).await?;
    Ok(installed)
}

fn cmd_clickhouse_status(cfg: &AppConfig, paths: &RuntimePaths) -> ClickhouseStatusSnapshot {
    let clickhouse = managed_clickhouse_bin(paths, "clickhouse");
    let clickhouse_server = managed_clickhouse_bin(paths, "clickhouse-server");
    let clickhouse_client = managed_clickhouse_bin(paths, "clickhouse-client");
    let (active_source, active_source_path) = active_clickhouse_source(paths);

    ClickhouseStatusSnapshot {
        managed_root: paths.managed_clickhouse_dir.display().to_string(),
        clickhouse_exists: clickhouse.exists(),
        clickhouse_server_exists: clickhouse_server.exists(),
        clickhouse_client_exists: clickhouse_client.exists(),
        expected_version: cfg.runtime.clickhouse_version.clone(),
        active_source: active_source.to_string(),
        active_source_path: active_source_path.map(|path| path.display().to_string()),
        checksum_state: managed_clickhouse_checksum_state(cfg, paths),
        installed_version: managed_clickhouse_version(paths),
    }
}

fn cmd_clickhouse_uninstall(paths: &RuntimePaths) -> Result<String> {
    if paths.managed_clickhouse_dir.exists() {
        fs::remove_dir_all(&paths.managed_clickhouse_dir).with_context(|| {
            format!("failed removing {}", paths.managed_clickhouse_dir.display())
        })?;
    }

    Ok(paths.managed_clickhouse_dir.display().to_string())
}

fn cmd_config_get(cfg: &AppConfig, key: &str) -> Result<String> {
    match key {
        "clickhouse.url" => Ok(cfg.clickhouse.url.clone()),
        "clickhouse.database" => Ok(cfg.clickhouse.database.clone()),
        _ => bail!(
            "unsupported config key '{}'; supported keys: clickhouse.url, clickhouse.database",
            key
        ),
    }
}

fn health_label(value: bool) -> &'static str {
    if value {
        "healthy"
    } else {
        "unhealthy"
    }
}

fn state_label(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn stoplight(running: bool) -> &'static str {
    if running {
        "\u{1F7E2}" // 🟢
    } else {
        "\u{1F534}" // 🔴
    }
}

fn service_endpoint(service: Service, snapshot: &StatusSnapshot) -> Option<String> {
    match service {
        Service::ClickHouse => Some(snapshot.clickhouse_health_url.clone()),
        Service::Monitor => snapshot.monitor_url.clone(),
        _ => None,
    }
}

fn format_start_state(outcome: &StartOutcome) -> String {
    match outcome.state {
        StartState::Started => "started".to_string(),
        StartState::AlreadyRunning => "already running".to_string(),
    }
}

fn render_status(output: &CliOutput, snapshot: &StatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    // -- Services with stoplight indicators and endpoints --
    let service_rows: Vec<Vec<String>> = snapshot
        .services
        .iter()
        .map(|row| {
            let running = row.pid.is_some();
            let mut cols = vec![
                format!("{} {}", stoplight(running), row.service.name()),
                if running {
                    "running".to_string()
                } else {
                    "stopped".to_string()
                },
                service_endpoint(row.service, snapshot).unwrap_or_default(),
            ];
            if output.verbose {
                cols.push(
                    row.pid
                        .map(|pid| pid.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                cols.push(row.supervisor.clone());
            }
            cols
        })
        .collect();

    if output.verbose {
        output.table(
            "Services",
            &["", "state", "endpoint", "pid", "supervisor"],
            &service_rows,
        );
    } else {
        output.table("Services", &["", "state", "endpoint"], &service_rows);
    }

    // -- Database Health (concise) --
    let db_healthy = snapshot.doctor.clickhouse_healthy && snapshot.doctor.database_exists;
    let mut doctor_lines = vec![format!(
        "{} {}",
        stoplight(db_healthy),
        if db_healthy {
            "database healthy".to_string()
        } else {
            format!(
                "clickhouse {} / db {}",
                health_label(snapshot.doctor.clickhouse_healthy),
                if snapshot.doctor.database_exists {
                    "exists"
                } else {
                    "missing"
                }
            )
        }
    )];
    if let Some(version) = &snapshot.doctor.clickhouse_version {
        doctor_lines[0].push_str(&format!("  (v{version})"));
    }
    doctor_lines.push(format!(
        "  clickhouse compatibility: {}",
        clickhouse_compatibility_detail(&snapshot.doctor)
    ));
    if !snapshot.doctor.pending_migrations.is_empty() {
        doctor_lines.push(format!(
            "  pending migrations: {}",
            snapshot.doctor.pending_migrations.join(", ")
        ));
    }
    if !snapshot.doctor.missing_tables.is_empty() {
        doctor_lines.push(format!(
            "  missing tables: {}",
            snapshot.doctor.missing_tables.join(", ")
        ));
    }
    if output.verbose && !snapshot.doctor.errors.is_empty() {
        doctor_lines.push(format!("  errors: {}", snapshot.doctor.errors.join(" | ")));
    }
    output.section("Database", &doctor_lines);

    // -- Ingest activity (only show when there is something to report) --
    match &snapshot.heartbeat {
        HeartbeatSnapshot::Available {
            latest,
            queue_depth,
            files_active,
            watcher_backend,
            watcher_error_count,
            watcher_reset_count,
            watcher_last_reset_unix_ms,
        } => {
            let mut lines = vec![
                format!("last event: {latest}"),
                format!("queue: {queue_depth}  |  active files: {files_active}"),
            ];
            if *watcher_error_count > 0 || *watcher_reset_count > 0 {
                lines.push(format!(
                    "watcher: {watcher_backend}  (errors: {watcher_error_count}, resets: {watcher_reset_count})"
                ));
            } else if output.verbose {
                lines.push(format!("watcher: {watcher_backend}"));
            }
            if output.verbose {
                lines.push(format!(
                    "watcher last reset unix ms: {watcher_last_reset_unix_ms}"
                ));
            }
            output.section("Ingest", &lines);
        }
        HeartbeatSnapshot::Unavailable => {
            if output.verbose {
                output.section("Ingest", &["no heartbeat data".to_string()]);
            }
        }
        HeartbeatSnapshot::Error { message } => {
            output.section("Ingest", &[format!("heartbeat error: {message}")]);
        }
    }

    // -- ClickHouse runtime details (verbose only) --
    if output.verbose {
        let mut ch_lines = vec![
            format!(
                "managed install: {}",
                if snapshot.managed_clickhouse_installed {
                    "present"
                } else {
                    "missing"
                }
            ),
            format!("binary: {}", snapshot.managed_clickhouse_path),
            format!(
                "source: {}{}",
                snapshot.clickhouse_active_source,
                snapshot
                    .clickhouse_active_source_path
                    .as_ref()
                    .map(|p| format!(" ({p})"))
                    .unwrap_or_default()
            ),
            format!("checksum: {}", snapshot.managed_clickhouse_checksum),
        ];
        if let Some(version) = &snapshot.managed_clickhouse_version {
            ch_lines.push(format!("managed version: {version}"));
        }
        output.section("ClickHouse Runtime", &ch_lines);
    }

    // -- Status notes (warnings) --
    if !snapshot.status_notes.is_empty() {
        output.section("Warnings", &snapshot.status_notes);
    }
    Ok(())
}

fn render_sources_status(output: &CliOutput, snapshot: &SourceStatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    if let Some(error) = &snapshot.query_error {
        output.section(
            "Source Status Query",
            &[format!("status: partial ({error})")],
        );
    }

    if snapshot.sources.is_empty() {
        output.section(
            "Sources",
            &["no matching ingest sources configured".to_string()],
        );
        return Ok(());
    }

    let rows = snapshot
        .sources
        .iter()
        .map(|source| {
            let format = if source.format.trim().is_empty() {
                "infer"
            } else {
                source.format.as_str()
            };
            let latest_error = source
                .latest_error_kind
                .as_ref()
                .map(|kind| {
                    if output.verbose {
                        let text = source.latest_error_text.as_deref().unwrap_or_default();
                        if text.is_empty() {
                            kind.clone()
                        } else {
                            format!("{kind}: {text}")
                        }
                    } else {
                        kind.clone()
                    }
                })
                .unwrap_or_else(|| "-".to_string());

            let mut row = vec![
                source.name.clone(),
                source.status.as_str().to_string(),
                source.harness.clone(),
                format.to_string(),
                source.raw_event_count.to_string(),
                source.checkpoint_count.to_string(),
                source.ingest_error_count.to_string(),
                latest_error,
            ];

            if output.verbose {
                row.push(
                    source
                        .latest_checkpoint_at
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                );
                row.push(
                    source
                        .latest_error_at
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                );
                row.push(source.watch_root.clone());
                row.push(source.glob.clone());
            }

            row
        })
        .collect::<Vec<_>>();

    if output.verbose {
        output.table(
            "Sources",
            &[
                "source",
                "status",
                "harness",
                "format",
                "raw",
                "checkpoints",
                "errors",
                "latest error",
                "latest checkpoint",
                "latest error at",
                "watch root",
                "glob",
            ],
            &rows,
        );
    } else {
        output.table(
            "Sources",
            &[
                "source",
                "status",
                "harness",
                "format",
                "raw",
                "checkpoints",
                "errors",
                "latest error",
            ],
            &rows,
        );
    }

    Ok(())
}

fn render_sources_files(output: &CliOutput, snapshot: &SourceFilesSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    if let Some(error) = &snapshot.fs_error {
        output.section("Filesystem", &[format!("warning: {error}")]);
    }
    if let Some(error) = &snapshot.query_error {
        output.section("Query", &[format!("warning: {error}")]);
    }

    if snapshot.files.is_empty() {
        output.section(
            &format!("Source Files: {}", snapshot.source_name),
            &["no files matched".to_string()],
        );
        return Ok(());
    }

    let rows = snapshot
        .files
        .iter()
        .map(|file| {
            let mut row = vec![
                file.path.clone(),
                file.size_bytes.to_string(),
                file.modified_at.clone().unwrap_or_else(|| "-".to_string()),
                file.raw_event_count.to_string(),
                file.checkpoint_offset
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                file.checkpoint_status
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ];
            if output.verbose {
                row.push(
                    file.checkpoint_line_no
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                row.push(
                    file.checkpoint_updated_at
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                );
                row.push(
                    file.latest_error_kind
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                );
            }
            row
        })
        .collect::<Vec<_>>();

    if output.verbose {
        output.table(
            &format!(
                "Source Files: {} ({} matches)",
                snapshot.source_name, snapshot.glob_match_count
            ),
            &[
                "path",
                "size",
                "modified",
                "raw",
                "checkpoint",
                "status",
                "line",
                "updated",
                "latest error",
            ],
            &rows,
        );
    } else {
        output.table(
            &format!(
                "Source Files: {} ({} matches)",
                snapshot.source_name, snapshot.glob_match_count
            ),
            &["path", "size", "modified", "raw", "checkpoint", "status"],
            &rows,
        );
    }
    Ok(())
}

fn render_sources_errors(output: &CliOutput, snapshot: &SourceErrorsSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    if let Some(error) = &snapshot.query_error {
        output.section("Query", &[format!("warning: {error}")]);
    }

    if snapshot.errors.is_empty() {
        output.section(
            &format!("Source Errors: {}", snapshot.source_name),
            &["no errors recorded".to_string()],
        );
        return Ok(());
    }

    let rows = snapshot
        .errors
        .iter()
        .map(|err| {
            let mut row = vec![
                err.ingested_at.clone(),
                err.error_kind.clone(),
                err.source_file.clone(),
            ];
            if output.verbose {
                row.push(err.error_text.clone());
                row.push(err.raw_fragment.clone());
            }
            row
        })
        .collect::<Vec<_>>();

    if output.verbose {
        output.table(
            &format!(
                "Source Errors: {} ({})",
                snapshot.source_name,
                snapshot.errors.len()
            ),
            &["time", "kind", "file", "text", "raw fragment"],
            &rows,
        );
    } else {
        output.table(
            &format!(
                "Source Errors: {} ({})",
                snapshot.source_name,
                snapshot.errors.len()
            ),
            &["time", "kind", "file"],
            &rows,
        );
    }
    Ok(())
}

fn render_db_migrate(output: &CliOutput, outcome: &MigrationOutcome) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(outcome)?);
        return Ok(());
    }
    if outcome.applied.is_empty() {
        output.section("Database Migrations", &["already up to date".to_string()]);
        return Ok(());
    }
    let rows = outcome
        .applied
        .iter()
        .enumerate()
        .map(|(idx, migration)| vec![(idx + 1).to_string(), migration.to_string()])
        .collect::<Vec<_>>();
    output.table("Applied Migrations", &["#", "migration"], &rows);
    Ok(())
}

fn doctor_report(snapshot: &DoctorSnapshot) -> &DoctorReport {
    match snapshot {
        DoctorSnapshot::Basic(report) => report,
        DoctorSnapshot::Deep(report) => &report.report,
    }
}

fn doctor_findings(snapshot: &DoctorSnapshot) -> &[DoctorFinding] {
    match snapshot {
        DoctorSnapshot::Basic(_) => &[],
        DoctorSnapshot::Deep(report) => &report.findings,
    }
}

fn doctor_is_healthy(snapshot: &DoctorSnapshot) -> bool {
    let report = doctor_report(snapshot);
    report.clickhouse_healthy
        && report.database_exists
        && report.clickhouse_version_compatibility != ClickHouseVersionCompatibility::Unsupported
        && report.pending_migrations.is_empty()
        && report.missing_tables.is_empty()
        && report.errors.is_empty()
        && doctor_findings(snapshot)
            .iter()
            .all(|finding| finding.severity != DoctorSeverity::Error)
}

fn doctor_severity_label(severity: DoctorSeverity) -> &'static str {
    match severity {
        DoctorSeverity::Ok => "ok",
        DoctorSeverity::Warning => "warning",
        DoctorSeverity::Error => "error",
    }
}

fn clickhouse_compatibility_label(compatibility: ClickHouseVersionCompatibility) -> &'static str {
    match compatibility {
        ClickHouseVersionCompatibility::Supported => "supported",
        ClickHouseVersionCompatibility::Experimental => "experimental",
        ClickHouseVersionCompatibility::Unsupported => "unsupported",
        ClickHouseVersionCompatibility::Unknown => "unknown",
    }
}

fn clickhouse_compatibility_detail(report: &DoctorReport) -> String {
    match (
        report.clickhouse_version_compatibility,
        report.clickhouse_version_line.as_deref(),
    ) {
        (ClickHouseVersionCompatibility::Supported, Some(line)) => {
            format!("supported (line {line}, current pinned support line)")
        }
        (ClickHouseVersionCompatibility::Experimental, Some(line)) => {
            format!("experimental (line {line}, named next-candidate line)")
        }
        (ClickHouseVersionCompatibility::Unsupported, Some(line)) => {
            format!("unsupported (line {line})")
        }
        (ClickHouseVersionCompatibility::Unknown, Some(line)) => {
            format!("unknown (parsed line {line})")
        }
        _ => clickhouse_compatibility_label(report.clickhouse_version_compatibility).to_string(),
    }
}

fn render_db_doctor(output: &CliOutput, snapshot: &DoctorSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let report = doctor_report(snapshot);
    let mut lines = vec![
        format!("clickhouse: {}", health_label(report.clickhouse_healthy)),
        format!("database: {}", report.database),
        format!("database exists: {}", state_label(report.database_exists)),
        format!(
            "pending migrations: {}",
            if report.pending_migrations.is_empty() {
                "none".to_string()
            } else {
                report.pending_migrations.join(", ")
            }
        ),
        format!(
            "missing tables: {}",
            if report.missing_tables.is_empty() {
                "none".to_string()
            } else {
                report.missing_tables.join(", ")
            }
        ),
    ];
    if let Some(version) = &report.clickhouse_version {
        lines.push(format!("clickhouse version: {version}"));
    }
    lines.push(format!(
        "clickhouse compatibility: {}",
        clickhouse_compatibility_detail(report)
    ));
    if output.verbose && !report.applied_migrations.is_empty() {
        lines.push(format!(
            "applied migrations: {}",
            report.applied_migrations.join(", ")
        ));
    }
    if !report.errors.is_empty() {
        lines.push(format!("errors: {}", report.errors.join(" | ")));
    }
    output.section("DB Doctor", &lines);

    let findings = doctor_findings(snapshot);
    if !findings.is_empty() {
        let visible = if output.verbose {
            findings.iter().collect::<Vec<_>>()
        } else {
            findings
                .iter()
                .filter(|finding| finding.severity != DoctorSeverity::Ok)
                .collect::<Vec<_>>()
        };
        let finding_lines = if visible.is_empty() {
            vec!["all deep checks passed".to_string()]
        } else {
            visible
                .into_iter()
                .flat_map(|finding| {
                    [
                        format!(
                            "[{}] {}: {}",
                            doctor_severity_label(finding.severity),
                            finding.code,
                            finding.summary
                        ),
                        format!("fix: {}", finding.remediation),
                    ]
                })
                .collect::<Vec<_>>()
        };
        output.section("DB Doctor Findings", &finding_lines);
    }
    Ok(())
}

fn render_logs(output: &CliOutput, snapshot: &LogsSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    for section in &snapshot.sections {
        let mut lines = vec![
            format!("path: {}", section.path),
            format!("lines requested: {}", snapshot.requested_lines),
        ];
        if !section.exists {
            lines.push("log file: missing".to_string());
            output.section(&format!("Logs: {}", section.service.name()), &lines);
            continue;
        }
        lines.push(format!("lines returned: {}", section.lines.len()));
        output.section(&format!("Logs: {}", section.service.name()), &lines);
        for line in &section.lines {
            output.line(line);
        }
    }
    Ok(())
}

fn render_clickhouse_status(output: &CliOutput, snapshot: &ClickhouseStatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("managed root: {}", snapshot.managed_root),
        format!(
            "clickhouse binary: {}",
            state_label(snapshot.clickhouse_exists)
        ),
        format!(
            "clickhouse-server binary: {}",
            state_label(snapshot.clickhouse_server_exists)
        ),
        format!(
            "clickhouse-client binary: {}",
            state_label(snapshot.clickhouse_client_exists)
        ),
        format!("expected version: {}", snapshot.expected_version),
        format!(
            "active source: {}{}",
            snapshot.active_source,
            snapshot
                .active_source_path
                .as_ref()
                .map(|p| format!(" ({p})"))
                .unwrap_or_default()
        ),
        format!("checksum state: {}", snapshot.checksum_state),
    ];
    if let Some(version) = &snapshot.installed_version {
        lines.push(format!("installed version: {version}"));
    }
    output.section("Managed ClickHouse", &lines);
    Ok(())
}

fn render_up(output: &CliOutput, snapshot: &UpSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut rows = vec![vec![
        snapshot.clickhouse.service.name().to_string(),
        format_start_state(&snapshot.clickhouse),
        snapshot.clickhouse.pid.to_string(),
    ]];
    rows.extend(snapshot.services.iter().map(|outcome| {
        vec![
            outcome.service.name().to_string(),
            format_start_state(outcome),
            outcome.pid.to_string(),
        ]
    }));
    output.table("Startup Results", &["service", "result", "pid"], &rows);
    render_db_migrate(output, &snapshot.migrations)?;
    render_status(output, &snapshot.status)?;
    Ok(())
}

fn render_down(output: &CliOutput, snapshot: &DownSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    if snapshot.stopped.is_empty() {
        output.section("Shutdown", &["no running services found".to_string()]);
        return Ok(());
    }
    let rows = snapshot
        .stopped
        .iter()
        .map(|service| vec![service.name().to_string(), "stopped".to_string()])
        .collect::<Vec<_>>();
    output.table("Shutdown", &["service", "result"], &rows);
    Ok(())
}

fn imports_dir(cfg: &AppConfig) -> PathBuf {
    PathBuf::from(&cfg.runtime.root_dir).join("imports")
}

fn sync_manifest_path(cfg: &AppConfig, name: &str) -> PathBuf {
    imports_dir(cfg).join(format!("{name}.json"))
}

fn sync_timestamp() -> String {
    format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    )
}

fn build_sync_manifest(name: &str, profile: &moraine_config::ImportProfile) -> SyncManifest {
    SyncManifest {
        profile_name: name.to_string(),
        synced_at: sync_timestamp(),
        source_host: profile.host.clone(),
        source_paths: profile.remote_paths.clone(),
        local_mirror: profile.local_mirror.clone(),
        files_copied: 0,
        bytes_copied: 0,
        files_skipped: 0,
        duration_ms: 0,
        last_error: None,
    }
}

fn read_sync_manifest(cfg: &AppConfig, name: &str) -> Result<Option<SyncManifest>> {
    let path = sync_manifest_path(cfg, name);
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path)
        .with_context(|| format!("failed to read sync manifest {}", path.display()))?;
    let manifest: SyncManifest = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse sync manifest {}", path.display()))?;
    Ok(Some(manifest))
}

fn write_sync_manifest(cfg: &AppConfig, name: &str, manifest: &SyncManifest) -> Result<()> {
    let imports_dir = imports_dir(cfg);
    fs::create_dir_all(&imports_dir)
        .with_context(|| format!("failed to create imports dir {}", imports_dir.display()))?;
    let path = sync_manifest_path(cfg, name);
    let bytes = serde_json::to_vec_pretty(manifest).context("encode sync manifest")?;
    fs::write(&path, bytes)
        .with_context(|| format!("failed to write sync manifest {}", path.display()))
}

fn ensure_rsync_available() -> Result<()> {
    match Command::new("rsync")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            bail!("rsync is required for `moraine import sync --execute` (exit status {status})")
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            bail!("rsync is required for `moraine import sync --execute` and was not found on PATH")
        }
        Err(err) => Err(err).context("failed to probe rsync availability"),
    }
}

fn build_rsync_args(
    profile: &moraine_config::ImportProfile,
    remote_path: &str,
    destination: &Path,
) -> Vec<String> {
    let mut args = vec![
        "-az".to_string(),
        "--prune-empty-dirs".to_string(),
        "--stats".to_string(),
        "-e".to_string(),
        "ssh".to_string(),
    ];

    if !profile.include_patterns.is_empty() {
        args.push("--include".to_string());
        args.push("*/".to_string());
        for pattern in &profile.include_patterns {
            args.push("--include".to_string());
            args.push(pattern.clone());
        }
        for pattern in &profile.exclude_patterns {
            args.push("--exclude".to_string());
            args.push(pattern.clone());
        }
        args.push("--exclude".to_string());
        args.push("*".to_string());
    } else {
        for pattern in &profile.exclude_patterns {
            args.push("--exclude".to_string());
            args.push(pattern.clone());
        }
    }

    let remote_root = remote_path.trim_end_matches('/');
    args.push(format!("{}:{remote_root}/", profile.host));
    args.push(destination.display().to_string());
    args
}

fn parse_rsync_stat_value(text: &str, prefix: &str) -> Option<u64> {
    let rest = text.strip_prefix(prefix)?.trim();
    let number = rest
        .split_whitespace()
        .next()?
        .trim_end_matches(',')
        .replace(',', "");
    number.parse().ok()
}

fn parse_rsync_stats(output: &str) -> RsyncStats {
    let mut total_files = None;
    let mut files_copied = None;
    let mut bytes_copied = None;

    for line in output.lines().map(str::trim) {
        if total_files.is_none() {
            total_files = parse_rsync_stat_value(line, "Number of files:");
        }
        if files_copied.is_none() {
            files_copied = parse_rsync_stat_value(line, "Number of regular files transferred:");
        }
        if bytes_copied.is_none() {
            bytes_copied = parse_rsync_stat_value(line, "Total transferred file size:");
        }
    }

    let files_copied = files_copied.unwrap_or(0);
    let total_files = total_files.unwrap_or(files_copied);
    RsyncStats {
        files_copied,
        bytes_copied: bytes_copied.unwrap_or(0),
        files_skipped: total_files.saturating_sub(files_copied),
    }
}

fn command_output_text(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (false, false) => format!("{stdout}\n{stderr}"),
        (false, true) => stdout,
        (true, false) => stderr,
        (true, true) => String::new(),
    }
}

async fn cmd_import_sync(cfg: &AppConfig, name: &str, dry_run: bool) -> Result<SyncResult> {
    let profile = cfg
        .imports
        .get(name)
        .ok_or_else(|| anyhow!("import profile '{}' not found in config", name))?;

    let mut manifest = build_sync_manifest(name, profile);

    if !dry_run {
        ensure_rsync_available()?;

        let local_mirror = PathBuf::from(&profile.local_mirror);
        fs::create_dir_all(&local_mirror).with_context(|| {
            format!(
                "failed to create local import mirror {}",
                local_mirror.display()
            )
        })?;

        let started = Instant::now();
        for remote_path in &profile.remote_paths {
            let output = match Command::new("rsync")
                .args(build_rsync_args(profile, remote_path, &local_mirror))
                .output()
            {
                Ok(output) => output,
                Err(err) => {
                    manifest.duration_ms = started.elapsed().as_millis() as u64;
                    manifest.last_error = Some(format!(
                        "failed to execute rsync for {}:{}: {}",
                        profile.host, remote_path, err
                    ));
                    write_sync_manifest(cfg, name, &manifest)?;
                    bail!(manifest.last_error.clone().unwrap_or_default());
                }
            };

            if !output.status.success() {
                manifest.duration_ms = started.elapsed().as_millis() as u64;
                let detail = command_output_text(&output);
                manifest.last_error = Some(if detail.is_empty() {
                    format!("rsync failed for {}:{}", profile.host, remote_path)
                } else {
                    format!(
                        "rsync failed for {}:{}: {detail}",
                        profile.host, remote_path
                    )
                });
                write_sync_manifest(cfg, name, &manifest)?;
                bail!(manifest.last_error.clone().unwrap_or_default());
            }

            let stats = parse_rsync_stats(&command_output_text(&output));
            manifest.files_copied += stats.files_copied;
            manifest.bytes_copied += stats.bytes_copied;
            manifest.files_skipped += stats.files_skipped;
        }

        manifest.synced_at = sync_timestamp();
        manifest.duration_ms = started.elapsed().as_millis() as u64;
        write_sync_manifest(cfg, name, &manifest)?;
    }

    Ok(SyncResult {
        profile_name: name.to_string(),
        success: manifest.last_error.is_none(),
        manifest,
    })
}

fn preview_mode(dry_run: bool, execute: bool) -> bool {
    dry_run || !execute
}

async fn cmd_import_status(cfg: &AppConfig) -> Result<ImportStatusSnapshot> {
    let mut profiles = Vec::new();
    for (name, profile) in &cfg.imports {
        let last_sync = read_sync_manifest(cfg, name).ok().flatten();
        profiles.push(ImportProfileStatus {
            name: name.clone(),
            configured: true,
            host: profile.host.clone(),
            local_mirror: profile.local_mirror.clone(),
            cadence: profile.cadence.clone(),
            last_sync,
        });
    }
    if profiles.is_empty() {
        profiles.push(ImportProfileStatus {
            name: "(none)".to_string(),
            configured: false,
            host: "".to_string(),
            local_mirror: "".to_string(),
            cadence: "".to_string(),
            last_sync: None,
        });
    }
    Ok(ImportStatusSnapshot { profiles })
}

async fn cmd_archive_export(
    _cfg: &AppConfig,
    args: &ArchiveExportArgs,
) -> Result<ArchiveExportSnapshot> {
    if !preview_mode(args.dry_run, args.execute) {
        bail!("live archive export is not yet implemented; omit --execute to preview");
    }

    let mut tables = Vec::new();
    for table in ["events", "event_links", "tool_io"] {
        tables.push(ArchiveTableManifest {
            name: table.to_string(),
            rows: 0,
            file: format!("{table}.jsonl"),
        });
    }
    if args.raw {
        tables.push(ArchiveTableManifest {
            name: "raw_events".to_string(),
            rows: 0,
            file: "raw_events.jsonl".to_string(),
        });
    }

    let manifest = ArchiveManifest {
        schema_version: env!("CARGO_PKG_VERSION").to_string(),
        moraine_version: env!("CARGO_PKG_VERSION").to_string(),
        exported_at: format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ),
        tables,
    };

    Ok(ArchiveExportSnapshot {
        output_dir: args.out_dir.display().to_string(),
        manifest,
    })
}

async fn cmd_archive_import(
    _cfg: &AppConfig,
    args: &ArchiveImportArgs,
) -> Result<ArchiveImportSnapshot> {
    let manifest_path = args.input.join("manifest.json");
    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("missing manifest.json in {}", args.input.display()))?;
    let manifest: ArchiveManifest =
        serde_json::from_str(&manifest_text).context("failed to parse archive manifest")?;

    let current_version = env!("CARGO_PKG_VERSION").to_string();
    if manifest.moraine_version != current_version {
        eprintln!(
            "warning: archive was built with moraine {} (current {})",
            manifest.moraine_version, current_version
        );
    }

    let dry_run = preview_mode(args.dry_run, args.execute);
    if !dry_run {
        bail!("live archive import is not yet implemented; omit --execute to preview");
    }

    let mut imported_tables = Vec::new();
    for table_manifest in &manifest.tables {
        let file_path = args.input.join(&table_manifest.file);
        if !file_path.exists() {
            bail!("archive file missing: {}", file_path.display());
        }
        let data = fs::read_to_string(&file_path)
            .with_context(|| format!("failed to read {}", file_path.display()))?;
        let rows = data.lines().filter(|l| !l.trim().is_empty()).count() as u64;
        imported_tables.push(ArchiveTableImport {
            name: table_manifest.name.clone(),
            rows,
            file: table_manifest.file.clone(),
        });
    }

    Ok(ArchiveImportSnapshot {
        input_dir: args.input.display().to_string(),
        dry_run,
        imported_tables,
    })
}

fn cmd_archive_verify(path: &Path) -> Result<ArchiveManifest> {
    let manifest_path = path.join("manifest.json");
    let text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("missing manifest.json in {}", path.display()))?;
    let manifest: ArchiveManifest = serde_json::from_str(&text).context("invalid manifest")?;

    for table in &manifest.tables {
        let file_path = path.join(&table.file);
        if !file_path.exists() {
            bail!("manifest references missing file: {}", file_path.display());
        }
        let data = fs::read_to_string(&file_path)
            .with_context(|| format!("failed to read {}", file_path.display()))?;
        let actual_rows = data.lines().filter(|l| !l.trim().is_empty()).count() as u64;
        if actual_rows != table.rows {
            bail!(
                "row count mismatch for {}: manifest says {}, file has {}",
                table.name,
                table.rows,
                actual_rows
            );
        }
    }

    Ok(manifest)
}

#[derive(Debug, Deserialize)]
struct ClickHouseTableNameRow {
    name: String,
}

#[derive(Debug, Deserialize)]
struct AppliedMigrationRow {
    version: String,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    rows: u64,
}

fn extract_sql_body_after(sql: &str, marker: &str) -> Result<String> {
    let start = sql
        .find(marker)
        .ok_or_else(|| anyhow!("missing SQL marker `{marker}`"))?;
    let body = &sql[start + marker.len()..];
    Ok(body.trim().trim_end_matches(';').trim().to_string())
}

fn search_documents_select_sql() -> Result<String> {
    extract_sql_body_after(
        PRIVACY_METADATA_SQL,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_documents_from_events\nTO moraine.search_documents\nAS\n",
    )
    .map(|sql| sql.replace("moraine.", ""))
}

fn search_postings_select_sql() -> Result<String> {
    extract_sql_body_after(
        SEARCH_INDEX_SQL,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_postings\nTO moraine.search_postings\nAS\n",
    )
    .map(|sql| sql.replace("moraine.", ""))
}

fn search_conversation_terms_select_sql() -> Result<String> {
    let insert_sql = extract_sql_body_after(
        SEARCH_CONVERSATION_TERMS_SQL,
        "INSERT INTO moraine.search_conversation_terms\n",
    )?;
    let select_start = insert_sql
        .find("SELECT\n")
        .ok_or_else(|| anyhow!("search conversation terms SQL missing SELECT body"))?;
    Ok(insert_sql[select_start..].replace("moraine.", ""))
}

async fn query_count(ch: &ClickHouseClient, database: &str, query: &str) -> Result<u64> {
    let rows: Vec<CountRow> = ch
        .query_json_data(query, Some(database))
        .await
        .with_context(|| format!("failed count query: {query}"))?;
    Ok(rows.first().map(|row| row.rows).unwrap_or(0))
}

async fn current_search_counts(
    ch: &ClickHouseClient,
    database: &str,
) -> Result<ReindexPreviewCounts> {
    Ok(ReindexPreviewCounts {
        events: query_count(ch, database, "SELECT toUInt64(count()) AS rows FROM events").await?,
        search_documents: query_count(
            ch,
            database,
            "SELECT toUInt64(count()) AS rows FROM search_documents",
        )
        .await?,
        search_postings: query_count(
            ch,
            database,
            "SELECT toUInt64(count()) AS rows FROM search_postings",
        )
        .await?,
        search_conversation_terms: query_count(
            ch,
            database,
            "SELECT toUInt64(count()) AS rows FROM search_conversation_terms",
        )
        .await?,
    })
}

async fn projected_search_counts(
    ch: &ClickHouseClient,
    database: &str,
    documents_select: &str,
    postings_select: &str,
    conversation_terms_select: &str,
) -> Result<ReindexPreviewCounts> {
    Ok(ReindexPreviewCounts {
        events: query_count(ch, database, "SELECT toUInt64(count()) AS rows FROM events").await?,
        search_documents: query_count(
            ch,
            database,
            &format!("SELECT toUInt64(count()) AS rows FROM ({documents_select})"),
        )
        .await?,
        search_postings: query_count(
            ch,
            database,
            &format!("SELECT toUInt64(count()) AS rows FROM ({postings_select})"),
        )
        .await?,
        search_conversation_terms: query_count(
            ch,
            database,
            &format!("SELECT toUInt64(count()) AS rows FROM ({conversation_terms_select})"),
        )
        .await?,
    })
}

async fn ensure_reindex_ready(ch: &ClickHouseClient) -> Result<()> {
    ch.ping().await.context("ClickHouse ping failed")?;
    let pending = ch.pending_migration_versions().await?;
    if !pending.is_empty() {
        bail!(
            "search rebuild requires the current schema; run `moraine db migrate` first (pending: {})",
            pending.join(", ")
        );
    }
    Ok(())
}

async fn cmd_reindex(cfg: &AppConfig, args: &ReindexArgs) -> Result<ReindexSnapshot> {
    if !args.search_only {
        bail!(
            "only `moraine reindex --search-only` is implemented in this slice; full corpus replay is not available"
        );
    }

    let dry_run = preview_mode(args.dry_run, args.execute);
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ensure_reindex_ready(&ch).await?;
    if !dry_run {
        require_recent_verified_backup(
            cfg,
            "`moraine reindex --search-only --execute`",
            args.no_backup_check,
        )?;
    }

    let documents_select = search_documents_select_sql()?;
    let postings_select = search_postings_select_sql()?;
    let conversation_terms_select = search_conversation_terms_select_sql()?;
    let database = cfg.clickhouse.database.clone();

    let current = current_search_counts(&ch, &database).await?;
    let projected = projected_search_counts(
        &ch,
        &database,
        &documents_select,
        &postings_select,
        &conversation_terms_select,
    )
    .await?;

    if !dry_run {
        for table in [
            "search_conversation_terms",
            "search_postings",
            "search_documents",
        ] {
            ch.request_text(
                &format!("TRUNCATE TABLE {table}"),
                None,
                Some(&database),
                false,
                None,
            )
            .await
            .with_context(|| format!("failed to truncate {table}"))?;
        }
        ch.request_text(
            &format!("INSERT INTO search_documents {documents_select}"),
            None,
            Some(&database),
            false,
            None,
        )
        .await
        .context("failed to rebuild search_documents from events")?;
    }

    let mode = if dry_run { "dry_run" } else { "execute" }.to_string();
    let notes = vec![
        "scope: rebuilds only derived search tables from canonical events".to_string(),
        "derived tables touched: search_documents, search_postings, search_conversation_terms".to_string(),
        "search_term_stats and search_corpus_stats are views and will reflect rebuilt rows automatically".to_string(),
        "canonical tables like events and raw_events are not deleted or replayed".to_string(),
    ];

    Ok(ReindexSnapshot {
        mode,
        target: "search_only".to_string(),
        database,
        current,
        projected: if dry_run {
            projected
        } else {
            current_search_counts(&ch, &cfg.clickhouse.database).await?
        },
        notes,
    })
}

fn backup_table_specs(include_derived: bool) -> Vec<BackupTableSpec> {
    let mut specs = vec![
        BackupTableSpec {
            name: "raw_events",
            kind: "base",
            derived: false,
        },
        BackupTableSpec {
            name: "events",
            kind: "base",
            derived: false,
        },
        BackupTableSpec {
            name: "event_links",
            kind: "base",
            derived: false,
        },
        BackupTableSpec {
            name: "tool_io",
            kind: "base",
            derived: false,
        },
        BackupTableSpec {
            name: "ingest_errors",
            kind: "operational",
            derived: false,
        },
        BackupTableSpec {
            name: "ingest_checkpoints",
            kind: "operational",
            derived: false,
        },
        BackupTableSpec {
            name: "ingest_heartbeats",
            kind: "operational",
            derived: false,
        },
        BackupTableSpec {
            name: "schema_migrations",
            kind: "schema",
            derived: false,
        },
    ];

    if include_derived {
        specs.extend([
            BackupTableSpec {
                name: "search_documents",
                kind: "derived",
                derived: true,
            },
            BackupTableSpec {
                name: "search_postings",
                kind: "derived",
                derived: true,
            },
            BackupTableSpec {
                name: "search_conversation_terms",
                kind: "derived",
                derived: true,
            },
            BackupTableSpec {
                name: "search_query_log",
                kind: "derived",
                derived: true,
            },
            BackupTableSpec {
                name: "search_hit_log",
                kind: "derived",
                derived: true,
            },
            BackupTableSpec {
                name: "search_interaction_log",
                kind: "derived",
                derived: true,
            },
        ]);
    }

    specs
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn backup_default_root(cfg: &AppConfig) -> PathBuf {
    PathBuf::from(&cfg.runtime.root_dir).join("backups")
}

fn make_backup_id() -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("moraine-backup-{stamp}")
}

fn backup_target_dir(cfg: &AppConfig, out_dir: Option<&PathBuf>, backup_id: &str) -> PathBuf {
    out_dir
        .cloned()
        .unwrap_or_else(|| backup_default_root(cfg).join(backup_id))
}

fn sha256_bytes_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn count_jsonl_rows_text(text: &str) -> u64 {
    text.lines().filter(|line| !line.trim().is_empty()).count() as u64
}

fn count_jsonl_rows_bytes(bytes: &[u8]) -> Result<u64> {
    let text = std::str::from_utf8(bytes).context("backup table file is not valid utf-8")?;
    Ok(count_jsonl_rows_text(text))
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn format_age_seconds(age_seconds: u64) -> String {
    if age_seconds < 60 {
        format!("{age_seconds}s")
    } else if age_seconds < 60 * 60 {
        format!("{}m", age_seconds / 60)
    } else if age_seconds < 24 * 60 * 60 {
        format!("{}h", age_seconds / (60 * 60))
    } else {
        format!("{}d", age_seconds / (24 * 60 * 60))
    }
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid backup file path {}", path.display()))?;
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", tmp_path.display()))?;
    drop(file);
    fs::rename(&tmp_path, path).with_context(|| format!("failed to install {}", path.display()))?;
    Ok(())
}

fn backup_source_inventory(cfg: &AppConfig) -> Vec<BackupSourceInventory> {
    cfg.ingest
        .sources
        .iter()
        .map(|source| BackupSourceInventory {
            name: source.name.clone(),
            harness: source.harness.clone(),
            enabled: source.enabled,
            glob: source.glob.clone(),
            watch_root: source.watch_root.clone(),
            format: source.format.clone(),
        })
        .collect()
}

async fn existing_clickhouse_tables(
    ch: &ClickHouseClient,
    database: &str,
) -> Result<HashSet<String>> {
    let query = format!(
        "SELECT name FROM system.tables WHERE database = {} FORMAT JSONEachRow",
        quote_sql_string(database)
    );
    let rows: Vec<ClickHouseTableNameRow> = ch
        .query_json_each_row(&query, Some("system"))
        .await
        .context("failed to list ClickHouse tables")?;
    Ok(rows.into_iter().map(|row| row.name).collect())
}

async fn applied_migrations(ch: &ClickHouseClient, database: &str) -> Result<Vec<String>> {
    let query = format!(
        "SELECT toString(version) AS version FROM {}.schema_migrations GROUP BY version ORDER BY version FORMAT JSONEachRow",
        quote_identifier(database)
    );
    let rows: Vec<AppliedMigrationRow> = ch
        .query_json_each_row(&query, None)
        .await
        .context("failed to read applied schema migrations")?;
    Ok(rows.into_iter().map(|row| row.version).collect())
}

fn backup_relative_path(path: &str) -> Result<PathBuf> {
    let relative = Path::new(path);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("backup manifest contains unsafe relative path: {path}");
    }
    Ok(relative.to_path_buf())
}

fn read_backup_manifest(path: &Path) -> Result<BackupManifest> {
    let manifest_path = path.join(BACKUP_MANIFEST_FILE);
    let text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("missing {BACKUP_MANIFEST_FILE} in {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("invalid backup manifest {}", manifest_path.display()))
}

async fn cmd_backup_create(
    cfg: &AppConfig,
    args: &BackupCreateArgs,
) -> Result<BackupCreateSnapshot> {
    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    ch.ping().await.context("ClickHouse ping failed")?;

    let existing_tables = existing_clickhouse_tables(&ch, &cfg.clickhouse.database).await?;
    let backup_id = make_backup_id();
    let backup_dir = backup_target_dir(cfg, args.out_dir.as_ref(), &backup_id);
    if backup_dir.exists() && backup_dir.read_dir()?.next().transpose()?.is_some() {
        bail!(
            "backup output directory is not empty: {}",
            backup_dir.display()
        );
    }
    fs::create_dir_all(backup_dir.join(BACKUP_TABLES_DIR))
        .with_context(|| format!("failed to create {}", backup_dir.display()))?;

    let mut tables = Vec::new();
    for spec in backup_table_specs(args.include_derived) {
        if !existing_tables.contains(spec.name) {
            if spec.derived {
                continue;
            }
            bail!(
                "required backup table `{}` is missing from ClickHouse database `{}`",
                spec.name,
                cfg.clickhouse.database
            );
        }

        let query = format!(
            "SELECT * FROM {}.{} FORMAT JSONEachRow",
            quote_identifier(&cfg.clickhouse.database),
            quote_identifier(spec.name)
        );
        let text = ch
            .request_text(&query, None, Some(&cfg.clickhouse.database), false, None)
            .await
            .with_context(|| format!("failed to export table {}", spec.name))?;
        let file = format!("{BACKUP_TABLES_DIR}/{}.jsonl", spec.name);
        write_atomic_bytes(&backup_dir.join(&file), text.as_bytes())?;
        tables.push(BackupTableManifest {
            name: spec.name.to_string(),
            kind: spec.kind.to_string(),
            file,
            row_count: count_jsonl_rows_text(&text),
            sha256: sha256_bytes_hex(text.as_bytes()),
        });
    }

    let manifest = BackupManifest {
        manifest_version: BACKUP_MANIFEST_VERSION,
        backup_id,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        moraine_version: env!("CARGO_PKG_VERSION").to_string(),
        clickhouse_database: cfg.clickhouse.database.clone(),
        clickhouse_version: ch.version().await.ok(),
        include_derived: args.include_derived,
        bundled_migrations: bundled_migrations()
            .iter()
            .map(|migration| migration.version.to_string())
            .collect(),
        applied_migrations: applied_migrations(&ch, &cfg.clickhouse.database).await?,
        privacy_key_material: "external_not_included".to_string(),
        sources: backup_source_inventory(cfg),
        tables,
    };

    let manifest_bytes = serde_json::to_vec_pretty(&manifest).context("encode backup manifest")?;
    write_atomic_bytes(
        &backup_dir.join(BACKUP_MANIFEST_FILE),
        manifest_bytes.as_slice(),
    )?;

    Ok(BackupCreateSnapshot {
        backup_dir: backup_dir.display().to_string(),
        manifest,
    })
}

fn cmd_backup_list(cfg: &AppConfig, args: &BackupListArgs) -> Result<BackupListSnapshot> {
    let root = args
        .root
        .clone()
        .unwrap_or_else(|| backup_default_root(cfg));
    let mut backups = Vec::new();
    let mut skipped = Vec::new();

    if !root.exists() {
        return Ok(BackupListSnapshot {
            root: root.display().to_string(),
            backups,
            skipped,
        });
    }
    if !root.is_dir() {
        bail!("backup root is not a directory: {}", root.display());
    }

    let candidates = if root.join(BACKUP_MANIFEST_FILE).exists() {
        vec![root.clone()]
    } else {
        fs::read_dir(&root)
            .with_context(|| format!("failed to read {}", root.display()))?
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                path.is_dir().then_some(path)
            })
            .collect()
    };

    for path in candidates {
        if !path.join(BACKUP_MANIFEST_FILE).exists() {
            continue;
        }
        match read_backup_manifest(&path) {
            Ok(manifest) => backups.push(BackupListEntry {
                backup_id: manifest.backup_id,
                path: path.display().to_string(),
                created_unix_seconds: manifest.created_unix_seconds,
                table_count: manifest.tables.len(),
                total_rows: manifest.tables.iter().map(|table| table.row_count).sum(),
            }),
            Err(err) => skipped.push(format!("{}: {err:#}", path.display())),
        }
    }

    backups.sort_by_key(|backup| std::cmp::Reverse(backup.created_unix_seconds));

    Ok(BackupListSnapshot {
        root: root.display().to_string(),
        backups,
        skipped,
    })
}

fn cmd_backup_verify(path: &Path) -> BackupVerifySnapshot {
    let mut errors = Vec::new();
    let manifest = match read_backup_manifest(path) {
        Ok(manifest) => Some(manifest),
        Err(err) => {
            errors.push(format!("{err:#}"));
            None
        }
    };

    if let Some(manifest) = &manifest {
        if manifest.manifest_version != BACKUP_MANIFEST_VERSION {
            errors.push(format!(
                "unsupported manifest version {} (expected {BACKUP_MANIFEST_VERSION})",
                manifest.manifest_version
            ));
        }

        let mut seen_files = HashSet::new();
        let mut seen_tables = HashSet::new();
        for table in &manifest.tables {
            if !seen_tables.insert(table.name.clone()) {
                errors.push(format!("duplicate table entry: {}", table.name));
            }
            if !seen_files.insert(table.file.clone()) {
                errors.push(format!("duplicate table file entry: {}", table.file));
            }

            let relative_path = match backup_relative_path(&table.file) {
                Ok(relative_path) => relative_path,
                Err(err) => {
                    errors.push(format!("{err:#}"));
                    continue;
                }
            };
            let table_path = path.join(relative_path);
            let bytes = match fs::read(&table_path) {
                Ok(bytes) => bytes,
                Err(err) => {
                    errors.push(format!("failed to read {}: {err}", table_path.display()));
                    continue;
                }
            };

            match count_jsonl_rows_bytes(&bytes) {
                Ok(actual_rows) if actual_rows == table.row_count => {}
                Ok(actual_rows) => errors.push(format!(
                    "row count mismatch for {}: manifest says {}, file has {}",
                    table.name, table.row_count, actual_rows
                )),
                Err(err) => errors.push(format!("{}: {err:#}", table_path.display())),
            }

            let actual_sha = sha256_bytes_hex(&bytes);
            if actual_sha != table.sha256 {
                errors.push(format!(
                    "sha256 mismatch for {}: manifest says {}, file has {}",
                    table.name, table.sha256, actual_sha
                ));
            }
        }
    }

    BackupVerifySnapshot {
        path: path.display().to_string(),
        ok: errors.is_empty(),
        errors,
        manifest,
    }
}

fn latest_verified_backup_for_database(cfg: &AppConfig) -> Result<Option<VerifiedBackupSummary>> {
    let snapshot = cmd_backup_list(cfg, &BackupListArgs { root: None })?;

    for backup in snapshot.backups {
        let path = PathBuf::from(&backup.path);
        let verify = cmd_backup_verify(&path);
        if !verify.ok {
            continue;
        }
        let Some(manifest) = verify.manifest else {
            continue;
        };
        if manifest.clickhouse_database != cfg.clickhouse.database {
            continue;
        }

        return Ok(Some(VerifiedBackupSummary {
            backup_id: manifest.backup_id,
            path,
            created_unix_seconds: manifest.created_unix_seconds,
        }));
    }

    Ok(None)
}

fn require_recent_verified_backup(
    cfg: &AppConfig,
    operation: &str,
    no_backup_check: bool,
) -> Result<()> {
    if no_backup_check {
        return Ok(());
    }

    let root = backup_default_root(cfg);
    let heuristic = format!(
        "heuristic preflight: requires a backup under {} for database `{}` that currently passes `moraine backup verify` and is no older than {}",
        root.display(),
        cfg.clickhouse.database,
        DESTRUCTIVE_BACKUP_MAX_AGE_LABEL
    );

    let Some(backup) = latest_verified_backup_for_database(cfg)? else {
        bail!(
            "{operation} requires a recent verified backup before changing ClickHouse data.\n\
             no verified backup for database `{}` was found under {}\n\
             {heuristic}\n\
             run `moraine backup create`, optionally confirm it with `moraine backup verify <dir>`, or pass `--no-backup-check` to bypass this heuristic",
            cfg.clickhouse.database,
            root.display(),
        );
    };

    let age_seconds = unix_now_seconds().saturating_sub(backup.created_unix_seconds);
    if age_seconds <= DESTRUCTIVE_BACKUP_MAX_AGE_SECONDS {
        return Ok(());
    }

    bail!(
        "{operation} requires a recent verified backup before changing ClickHouse data.\n\
         latest verified backup: {} at {} (age {}, threshold {})\n\
         {heuristic}\n\
         create a fresh backup or pass `--no-backup-check` to bypass this heuristic",
        backup.backup_id,
        backup.path.display(),
        format_age_seconds(age_seconds),
        DESTRUCTIVE_BACKUP_MAX_AGE_LABEL,
    );
}

fn migration_backup_required(report: &DoctorReport) -> bool {
    report.database_exists && !report.pending_migrations.is_empty()
}

async fn require_recent_verified_backup_for_pending_migrations(
    cfg: &AppConfig,
    operation: &str,
    no_backup_check: bool,
) -> Result<()> {
    if no_backup_check {
        return Ok(());
    }

    let ch = ClickHouseClient::new(cfg.clickhouse.clone())?;
    let report = ch.doctor_report().await?;
    if migration_backup_required(&report) {
        require_recent_verified_backup(cfg, operation, false)?;
    }
    Ok(())
}

fn cmd_restore_plan(cfg: &AppConfig, args: &RestoreArgs) -> RestorePlanSnapshot {
    let verify = cmd_backup_verify(&args.input);
    let target_database = args
        .target_database
        .clone()
        .unwrap_or_else(|| cfg.clickhouse.database.clone());
    let dry_run = preview_mode(args.dry_run, args.execute);
    let mut blockers = verify.errors.clone();
    let mut warnings = Vec::new();

    if !dry_run {
        blockers.push(
            "live restore --execute is not implemented yet; run without --execute to inspect the restore plan"
                .to_string(),
        );
    }

    let (table_count, total_rows) = verify
        .manifest
        .as_ref()
        .map(|manifest| {
            if manifest.moraine_version != env!("CARGO_PKG_VERSION") {
                warnings.push(format!(
                    "backup was created by moraine {} (current {})",
                    manifest.moraine_version,
                    env!("CARGO_PKG_VERSION")
                ));
            }
            (
                manifest.tables.len(),
                manifest.tables.iter().map(|table| table.row_count).sum(),
            )
        })
        .unwrap_or((0, 0));

    RestorePlanSnapshot {
        input_dir: args.input.display().to_string(),
        target_database,
        dry_run,
        can_restore: blockers.is_empty(),
        blockers,
        warnings,
        table_count,
        total_rows,
    }
}

fn cmd_config_detect() -> ConfigDetectSnapshot {
    ConfigDetectSnapshot {
        sources: moraine_config::discover_sources(),
    }
}

fn cmd_config_validate(cfg: &AppConfig) -> ConfigValidateSnapshot {
    let issues = moraine_config::validate_sources(&cfg.ingest.sources);
    ConfigValidateSnapshot {
        ok: issues.is_empty(),
        issues,
    }
}

fn cmd_config_wizard(cfg_path: &Path, cfg: &AppConfig) -> Result<()> {
    let discovered = moraine_config::discover_sources();
    println!("Moraine Config Wizard");
    println!("=====================\n");

    let existing_names: std::collections::HashSet<String> =
        cfg.ingest.sources.iter().map(|s| s.name.clone()).collect();

    let mut to_add = Vec::new();
    for src in discovered {
        if !src.exists {
            continue;
        }
        if existing_names.contains(&src.name) {
            println!("[skip] source '{}' already configured", src.name);
            continue;
        }
        print!("Add source '{}' ({})? [Y/n] ", src.name, src.glob);
        std::io::stdout().flush()?;
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        let answer = buf.trim().to_ascii_lowercase();
        if answer.is_empty() || answer.starts_with('y') {
            to_add.push(src);
        }
    }

    if to_add.is_empty() {
        println!("\nNo new sources selected.");
        return Ok(());
    }

    println!("\nPreview of sources to add:");
    for src in &to_add {
        println!("  [[ingest.sources]]");
        println!("  name = {:?}", src.name);
        println!("  harness = {:?}", src.harness);
        println!("  enabled = true");
        println!("  glob = {:?}", src.glob);
        println!("  watch_root = {:?}", src.watch_root);
        println!("  format = {:?}", src.format);
        println!();
    }

    print!("Write to {}? [Y/n] ", cfg_path.display());
    std::io::stdout().flush()?;
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    let answer = buf.trim().to_ascii_lowercase();
    if !answer.is_empty() && !answer.starts_with('y') {
        println!("Aborted.");
        return Ok(());
    }

    if cfg_path.exists() {
        let backup = cfg_path.with_extension("toml.bak");
        fs::copy(cfg_path, &backup)
            .with_context(|| format!("failed to write backup {}", backup.display()))?;
        println!("Backed up existing config to {}", backup.display());
    }

    let mut toml_text = fs::read_to_string(cfg_path).unwrap_or_default();
    for src in &to_add {
        toml_text.push_str("\n[[ingest.sources]]\n");
        toml_text.push_str(&format!("name = {:?}\n", src.name));
        toml_text.push_str(&format!("harness = {:?}\n", src.harness));
        toml_text.push_str("enabled = true\n");
        toml_text.push_str(&format!("glob = {:?}\n", src.glob));
        toml_text.push_str(&format!("watch_root = {:?}\n", src.watch_root));
        toml_text.push_str(&format!("format = {:?}\n", src.format));
    }
    fs::write(cfg_path, toml_text)
        .with_context(|| format!("failed to write config {}", cfg_path.display()))?;
    println!("Config updated.");

    Ok(())
}

fn render_import_sync(output: &CliOutput, result: &SyncResult) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(result)?);
        return Ok(());
    }
    let lines = import_sync_lines(result);
    output.section("Import Sync", &lines);
    Ok(())
}

fn import_sync_lines(result: &SyncResult) -> Vec<String> {
    let mut lines = vec![
        format!("profile: {}", result.profile_name),
        format!("success: {}", result.success),
        format!("host: {}", result.manifest.source_host),
        format!("source paths: {}", result.manifest.source_paths.join(", ")),
        format!("mirror: {}", result.manifest.local_mirror),
        format!("synced at: {}", result.manifest.synced_at),
        format!("files copied: {}", result.manifest.files_copied),
        format!("files skipped: {}", result.manifest.files_skipped),
        format!("bytes copied: {}", result.manifest.bytes_copied),
        format!("duration: {}ms", result.manifest.duration_ms),
    ];
    if let Some(error) = &result.manifest.last_error {
        lines.push(format!("last error: {error}"));
    }
    lines
}

fn render_import_status(output: &CliOutput, snapshot: &ImportStatusSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = snapshot
        .profiles
        .iter()
        .map(|p| {
            let last_sync = p
                .last_sync
                .as_ref()
                .map(|m| format!("{} ({} files)", m.synced_at, m.files_copied))
                .unwrap_or_else(|| "never".to_string());
            vec![
                p.name.clone(),
                p.host.clone(),
                p.local_mirror.clone(),
                p.cadence.clone(),
                last_sync,
            ]
        })
        .collect();
    output.table(
        "Import Profiles",
        &["name", "host", "mirror", "cadence", "last_sync"],
        &rows,
    );
    Ok(())
}

fn render_archive_export(output: &CliOutput, snapshot: &ArchiveExportSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![format!("output dir: {}", snapshot.output_dir)];
    for table in &snapshot.manifest.tables {
        lines.push(format!(
            "  {}: {} rows ({})",
            table.name, table.rows, table.file
        ));
    }
    output.section("Archive Export", &lines);
    Ok(())
}

fn render_archive_import(output: &CliOutput, snapshot: &ArchiveImportSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("input dir: {}", snapshot.input_dir),
        format!("dry run: {}", snapshot.dry_run),
    ];
    for table in &snapshot.imported_tables {
        lines.push(format!(
            "  {}: {} rows ({})",
            table.name, table.rows, table.file
        ));
    }
    output.section("Archive Import", &lines);
    Ok(())
}

fn render_archive_verify(output: &CliOutput, manifest: &ArchiveManifest) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(manifest)?);
        return Ok(());
    }
    let mut lines = vec![
        format!("schema version: {}", manifest.schema_version),
        format!("exported at: {}", manifest.exported_at),
    ];
    for table in &manifest.tables {
        lines.push(format!(
            "  {}: {} rows ({})",
            table.name, table.rows, table.file
        ));
    }
    output.section("Archive Verify", &lines);
    Ok(())
}

fn render_backup_create(output: &CliOutput, snapshot: &BackupCreateSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let total_rows: u64 = snapshot
        .manifest
        .tables
        .iter()
        .map(|table| table.row_count)
        .sum();
    let mut lines = vec![
        format!("backup id: {}", snapshot.manifest.backup_id),
        format!("backup dir: {}", snapshot.backup_dir),
        format!("database: {}", snapshot.manifest.clickhouse_database),
        format!("tables: {}", snapshot.manifest.tables.len()),
        format!("rows: {total_rows}"),
        "privacy keys: external/not included".to_string(),
    ];
    for table in &snapshot.manifest.tables {
        lines.push(format!(
            "  {}: {} rows, sha256 {}",
            table.name, table.row_count, table.sha256
        ));
    }
    output.section("Backup Create", &lines);
    Ok(())
}

fn render_backup_list(output: &CliOutput, snapshot: &BackupListSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let mut lines = vec![format!("root: {}", snapshot.root)];
    if snapshot.backups.is_empty() {
        lines.push("no backups found".to_string());
    }
    for backup in &snapshot.backups {
        lines.push(format!(
            "  {}: {} tables, {} rows ({})",
            backup.backup_id, backup.table_count, backup.total_rows, backup.path
        ));
    }
    for skipped in &snapshot.skipped {
        lines.push(format!("  skipped: {skipped}"));
    }
    output.section("Backup List", &lines);
    Ok(())
}

fn render_backup_verify(output: &CliOutput, snapshot: &BackupVerifySnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let mut lines = vec![
        format!("path: {}", snapshot.path),
        format!("ok: {}", state_label(snapshot.ok)),
    ];
    if let Some(manifest) = &snapshot.manifest {
        let total_rows: u64 = manifest.tables.iter().map(|table| table.row_count).sum();
        lines.push(format!("backup id: {}", manifest.backup_id));
        lines.push(format!("tables: {}", manifest.tables.len()));
        lines.push(format!("rows: {total_rows}"));
    }
    for error in &snapshot.errors {
        lines.push(format!("  error: {error}"));
    }
    output.section("Backup Verify", &lines);
    Ok(())
}

fn render_restore_plan(output: &CliOutput, snapshot: &RestorePlanSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }

    let mut lines = vec![
        format!("input dir: {}", snapshot.input_dir),
        format!("target database: {}", snapshot.target_database),
        format!("dry run: {}", snapshot.dry_run),
        format!("can restore: {}", state_label(snapshot.can_restore)),
        format!("tables: {}", snapshot.table_count),
        format!("rows: {}", snapshot.total_rows),
    ];
    for blocker in &snapshot.blockers {
        lines.push(format!("  blocker: {blocker}"));
    }
    for warning in &snapshot.warnings {
        lines.push(format!("  warning: {warning}"));
    }
    output.section("Restore Plan", &lines);
    Ok(())
}

fn reindex_lines(snapshot: &ReindexSnapshot) -> Vec<String> {
    vec![
        format!("mode: {}", snapshot.mode),
        format!("target: {}", snapshot.target),
        format!("database: {}", snapshot.database),
        format!("canonical events: {}", snapshot.projected.events),
        format!(
            "search_documents: {} -> {}",
            snapshot.current.search_documents, snapshot.projected.search_documents
        ),
        format!(
            "search_postings: {} -> {}",
            snapshot.current.search_postings, snapshot.projected.search_postings
        ),
        format!(
            "search_conversation_terms: {} -> {}",
            snapshot.current.search_conversation_terms,
            snapshot.projected.search_conversation_terms
        ),
    ]
    .into_iter()
    .chain(snapshot.notes.iter().map(|note| format!("note: {note}")))
    .collect()
}

fn render_reindex(output: &CliOutput, snapshot: &ReindexSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    output.section("Reindex", &reindex_lines(snapshot));
    Ok(())
}

fn render_config_detect(output: &CliOutput, snapshot: &ConfigDetectSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = snapshot
        .sources
        .iter()
        .map(|s| {
            vec![
                s.name.clone(),
                s.harness.clone(),
                s.glob.clone(),
                if s.exists {
                    "found".to_string()
                } else {
                    "missing".to_string()
                },
            ]
        })
        .collect();
    output.table(
        "Discovered Sources",
        &["name", "harness", "glob", "status"],
        &rows,
    );
    Ok(())
}

fn render_config_validate(output: &CliOutput, snapshot: &ConfigValidateSnapshot) -> Result<()> {
    if output.is_json() {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    if snapshot.ok {
        output.section("Config Validate", &["ok".to_string()]);
    } else {
        let lines: Vec<String> = snapshot.issues.iter().map(|i| format!("{:?}", i)).collect();
        output.section("Config Validate", &lines);
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();
    let output = CliOutput::from_cli(&cli);

    match cli.command {
        CliCommand::Up(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let services_to_start = selected_up_services(&args, &cfg);
            preflight_required_service_binaries(&services_to_start, &paths)?;
            ensure_runtime_dirs(&paths)?;

            let clickhouse = start_clickhouse(&cfg, &paths).await?;
            require_recent_verified_backup_for_pending_migrations(
                &cfg,
                "`moraine up` auto-migration",
                args.no_backup_check,
            )
            .await?;
            let migrations = cmd_db_migrate(&cfg).await?;

            let mut started_services = Vec::new();
            for service in services_to_start {
                started_services.push(start_background_service(
                    service,
                    &config_path,
                    &cfg,
                    &paths,
                    &[],
                )?);
            }

            let status = cmd_status(&paths, &cfg).await?;
            let snapshot = UpSnapshot {
                clickhouse,
                migrations,
                services: started_services,
                status,
            };
            render_up(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Down => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let mut stopped = Vec::new();
            for service in [
                Service::Mcp,
                Service::Monitor,
                Service::Ingest,
                Service::ClickHouse,
            ] {
                if stop_service(&paths, service)? {
                    stopped.push(service);
                }
            }
            render_down(&output, &DownSnapshot { stopped })?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Status => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let snapshot = cmd_status(&paths, &cfg).await?;
            render_status(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Logs(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            let snapshot = collect_logs(&paths, args.service, args.lines)?;
            render_logs(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Db(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                DbCommand::Migrate(migrate) => {
                    require_recent_verified_backup_for_pending_migrations(
                        &cfg,
                        "`moraine db migrate`",
                        migrate.no_backup_check,
                    )
                    .await?;
                    let outcome = cmd_db_migrate(&cfg).await?;
                    render_db_migrate(&output, &outcome)?;
                    Ok(ExitCode::SUCCESS)
                }
                DbCommand::Doctor(doctor) => {
                    let report = cmd_db_doctor(&cfg, doctor.deep).await?;
                    render_db_doctor(&output, &report)?;
                    if doctor_is_healthy(&report) {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
            }
        }
        CliCommand::Clickhouse(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let paths = runtime_paths(&cfg);
            match args.command {
                ClickhouseCommand::Install(install) => {
                    let version = install
                        .version
                        .unwrap_or_else(|| cfg.runtime.clickhouse_version.clone());
                    let installed = cmd_clickhouse_install(&paths, &version, install.force).await?;
                    if output.is_json() {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "installed_path": installed.display().to_string(),
                                "version": version,
                                "force": install.force,
                            }))?
                        );
                    } else {
                        output.section(
                            "Managed ClickHouse Install",
                            &[
                                format!("installed binary: {}", installed.display()),
                                format!("version: {version}"),
                                format!("force: {}", state_label(install.force)),
                            ],
                        );
                    }
                    Ok(ExitCode::SUCCESS)
                }
                ClickhouseCommand::Status => {
                    let snapshot = cmd_clickhouse_status(&cfg, &paths);
                    render_clickhouse_status(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ClickhouseCommand::Uninstall => {
                    let removed = cmd_clickhouse_uninstall(&paths)?;
                    if output.is_json() {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "removed_path": removed
                            }))?
                        );
                    } else {
                        output.section(
                            "Managed ClickHouse Uninstall",
                            &[format!("removed: {removed}")],
                        );
                    }
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Config(args) => {
            let (config_path, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                ConfigCommand::Get(get) => {
                    let value = cmd_config_get(&cfg, &get.key)?;
                    if output.is_json() {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "key": get.key,
                                "value": value,
                            }))?
                        );
                    } else {
                        println!("{value}");
                    }
                    Ok(ExitCode::SUCCESS)
                }
                ConfigCommand::Detect(detect) => {
                    let snapshot = cmd_config_detect();
                    if detect.json {
                        println!("{}", serde_json::to_string_pretty(&snapshot)?);
                    } else {
                        render_config_detect(&output, &snapshot)?;
                    }
                    Ok(ExitCode::SUCCESS)
                }
                ConfigCommand::Validate => {
                    let snapshot = cmd_config_validate(&cfg);
                    render_config_validate(&output, &snapshot)?;
                    if snapshot.ok {
                        Ok(ExitCode::SUCCESS)
                    } else {
                        Ok(ExitCode::from(1))
                    }
                }
                ConfigCommand::Wizard => {
                    cmd_config_wizard(&config_path, &cfg)?;
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Sources(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                SourcesCommand::Status(status) => {
                    let snapshot = cmd_sources_status(&cfg, status.include_disabled).await?;
                    render_sources_status(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                SourcesCommand::Files(files) => {
                    let snapshot = cmd_sources_files(&cfg, &files.source).await?;
                    render_sources_files(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                SourcesCommand::Errors(errors) => {
                    let snapshot = cmd_sources_errors(&cfg, &errors.source, errors.limit).await?;
                    render_sources_errors(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Import(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                ImportCommand::Sync(sync) => {
                    let dry_run = preview_mode(sync.dry_run, sync.execute);
                    let result = cmd_import_sync(&cfg, &sync.name, dry_run).await?;
                    render_import_sync(&output, &result)?;
                    Ok(ExitCode::SUCCESS)
                }
                ImportCommand::Status => {
                    let snapshot = cmd_import_status(&cfg).await?;
                    render_import_status(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Archive(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                ArchiveCommand::Export(export) => {
                    let snapshot = cmd_archive_export(&cfg, &export).await?;
                    render_archive_export(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ArchiveCommand::Import(import) => {
                    let snapshot = cmd_archive_import(&cfg, &import).await?;
                    render_archive_import(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                ArchiveCommand::Verify(verify) => {
                    let manifest = cmd_archive_verify(&verify.path)?;
                    render_archive_verify(&output, &manifest)?;
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        CliCommand::Backup(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            match args.command {
                BackupCommand::Create(create) => {
                    let snapshot = cmd_backup_create(&cfg, &create).await?;
                    render_backup_create(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                BackupCommand::List(list) => {
                    let snapshot = cmd_backup_list(&cfg, &list)?;
                    render_backup_list(&output, &snapshot)?;
                    Ok(ExitCode::SUCCESS)
                }
                BackupCommand::Verify(verify) => {
                    let snapshot = cmd_backup_verify(&verify.path);
                    render_backup_verify(&output, &snapshot)?;
                    Ok(if snapshot.ok {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::from(1)
                    })
                }
            }
        }
        CliCommand::Restore(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let snapshot = cmd_restore_plan(&cfg, &args);
            render_restore_plan(&output, &snapshot)?;
            Ok(if snapshot.can_restore {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        CliCommand::Reindex(args) => {
            let (_, cfg) = load_cfg(cli.config.clone())?;
            let snapshot = cmd_reindex(&cfg, &args).await?;
            render_reindex(&output, &snapshot)?;
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Run(run) => {
            let (inline_config, passthrough) = parse_config_flag(&run.args)?;
            let raw_config = inline_config.or(cli.config.clone());
            let (config_path, cfg) = load_cfg(raw_config)?;
            let paths = runtime_paths(&cfg);
            run_foreground_service(run.service, &config_path, &cfg, &paths, &passthrough).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};

    static ENV_VAR_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        original: Option<OsString>,
    }

    impl EnvVarGuard {
        fn capture(key: &'static str) -> Self {
            Self {
                key,
                original: std::env::var_os(key),
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.original {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn lock_env_vars() -> MutexGuard<'static, ()> {
        ENV_VAR_LOCK.lock().expect("env-var lock poisoned")
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("moraine-{name}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn write_file(path: &Path) {
        fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
        fs::write(path, b"#!/bin/sh\n").expect("write file");
    }

    fn test_doctor_report(clickhouse_healthy: bool) -> DoctorReport {
        DoctorReport {
            clickhouse_healthy,
            clickhouse_version: None,
            clickhouse_version_compatibility: ClickHouseVersionCompatibility::Unknown,
            clickhouse_version_line: None,
            database: "moraine".to_string(),
            database_exists: true,
            applied_migrations: Vec::new(),
            pending_migrations: Vec::new(),
            missing_tables: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn test_doctor_deep_report(
        clickhouse_healthy: bool,
        findings: Vec<DoctorFinding>,
    ) -> DoctorDeepReport {
        DoctorDeepReport {
            report: test_doctor_report(clickhouse_healthy),
            findings,
        }
    }

    fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
            .block_on(future)
    }

    #[test]
    fn tail_lines_returns_last_n_without_trailing_newline() {
        let root = temp_dir("tail-lines-basic");
        let path = root.join("test.log");
        fs::write(&path, "one\ntwo\nthree").expect("write log");

        let lines = tail_lines(&path, 2).expect("tail lines");
        assert_eq!(lines, vec!["two".to_string(), "three".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tail_lines_handles_utf8_chunk_boundary() {
        let root = temp_dir("tail-lines-utf8");
        let path = root.join("test.log");
        let prefix = "é".repeat(4500);
        let content = format!("{prefix}\nmiddle\ntail\n");
        fs::write(&path, content).expect("write log");

        let one = tail_lines(&path, 1).expect("tail one line");
        assert_eq!(one, vec!["tail".to_string()]);

        let two = tail_lines(&path, 2).expect("tail two lines");
        assert_eq!(two, vec!["middle".to_string(), "tail".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn clickhouse_asset_selection_covers_target_matrix() {
        let linux_x64 =
            clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "x86_64-unknown-linux-gnu")
                .expect("linux x64");
        assert_eq!(linux_x64.url, CH_URL_LINUX_X86_64);
        assert!(linux_x64.is_archive);

        let linux_arm =
            clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "aarch64-unknown-linux-gnu")
                .expect("linux arm");
        assert_eq!(linux_arm.url, CH_URL_LINUX_AARCH64);
        assert!(linux_arm.is_archive);

        let mac_x64 = clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "x86_64-apple-darwin")
            .expect("mac x64");
        assert_eq!(mac_x64.url, CH_URL_MACOS_X86_64);
        assert!(!mac_x64.is_archive);

        let mac_arm = clickhouse_asset_for_target(DEFAULT_CLICKHOUSE_TAG, "aarch64-apple-darwin")
            .expect("mac arm");
        assert_eq!(mac_arm.url, CH_URL_MACOS_AARCH64);
        assert!(!mac_arm.is_archive);
    }

    #[test]
    fn clickhouse_asset_rejects_unsupported_version() {
        let err = clickhouse_asset_for_target("v0.0.0", "x86_64-unknown-linux-gnu")
            .expect_err("unsupported version");
        assert!(
            err.to_string().contains("unsupported ClickHouse version"),
            "{}",
            err
        );
    }

    #[test]
    fn clickhouse_logs_use_internal_rotating_path() {
        let root = temp_dir("clickhouse-log-path");
        let logs_dir = root.join("logs");

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.runtime.logs_dir = logs_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        assert_eq!(
            log_path(&paths, Service::ClickHouse),
            root.join("clickhouse/log/clickhouse-server.log")
        );
        assert_eq!(
            log_path(&paths, Service::Ingest),
            logs_dir.join("ingest.log")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_legacy_clickhouse_pipe_log_removes_legacy_file() {
        let root = temp_dir("legacy-clickhouse-log");
        let logs_dir = root.join("logs");
        fs::create_dir_all(&logs_dir).expect("create logs dir");

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.runtime.logs_dir = logs_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        let legacy_log = legacy_clickhouse_pipe_log_path(&paths);
        fs::write(&legacy_log, b"legacy clickhouse stdout").expect("write legacy log");
        assert!(legacy_log.exists());

        cleanup_legacy_clickhouse_pipe_log(&paths);
        assert!(!legacy_log.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn find_file_ending_with_prefers_usr_bin_clickhouse() {
        let root = temp_dir("find-file-ending-with");
        let completion = root.join("pkg/usr/share/bash-completion/completions/clickhouse");
        let binary = root.join("pkg/usr/bin/clickhouse");
        write_file(&completion);
        write_file(&binary);

        let resolved = find_file_ending_with(&root, &["usr", "bin", "clickhouse"])
            .expect("resolve clickhouse path")
            .expect("clickhouse path");

        assert_eq!(resolved, binary);
    }

    #[test]
    fn build_status_notes_flags_healthy_external_clickhouse() {
        let services = vec![ServiceRuntimeStatus {
            service: Service::ClickHouse,
            pid: None,
            supervisor: "pid_file".to_string(),
        }];
        let report = test_doctor_report(true);
        let notes = build_status_notes(&services, &report, "http://127.0.0.1:8123");
        assert_eq!(notes.len(), 1);
        assert!(
            notes[0].contains("endpoint is healthy while managed clickhouse runtime is stopped")
        );
        assert!(notes[0].contains("http://127.0.0.1:8123"));
    }

    #[test]
    fn build_status_notes_flags_unhealthy_managed_clickhouse() {
        let services = vec![ServiceRuntimeStatus {
            service: Service::ClickHouse,
            pid: Some(4242),
            supervisor: "pid_file".to_string(),
        }];
        let report = test_doctor_report(false);
        let notes = build_status_notes(&services, &report, "http://127.0.0.1:8123");
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("managed clickhouse runtime is running"));
        assert!(notes[0].contains("are failing"));
        assert!(notes[0].contains("http://127.0.0.1:8123"));
    }

    #[test]
    fn doctor_is_healthy_ignores_warning_findings() {
        let snapshot = DoctorSnapshot::Deep(test_doctor_deep_report(
            true,
            vec![DoctorFinding {
                severity: DoctorSeverity::Warning,
                code: "search.index_freshness".to_string(),
                summary: "search documents drifted".to_string(),
                remediation: "run `moraine reindex --search-only --execute`".to_string(),
            }],
        ));

        assert!(doctor_is_healthy(&snapshot));
    }

    #[test]
    fn doctor_is_healthy_fails_on_error_findings() {
        let snapshot = DoctorSnapshot::Deep(test_doctor_deep_report(
            true,
            vec![DoctorFinding {
                severity: DoctorSeverity::Error,
                code: "integrity.event_links_orphans".to_string(),
                summary: "orphan event_links rows".to_string(),
                remediation: "repair events".to_string(),
            }],
        ));

        assert!(!doctor_is_healthy(&snapshot));
    }

    #[test]
    fn source_health_status_classifies_source_rows() {
        use moraine_source_status::{source_health_status, SourceHealthStatus};
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
    fn clap_parses_clickhouse_install_flags() {
        let cli = Cli::parse_from([
            "moraine",
            "clickhouse",
            "install",
            "--version",
            "v25.12.5.44-stable",
            "--force",
        ]);
        match cli.command {
            CliCommand::Clickhouse(ClickhouseArgs {
                command: ClickhouseCommand::Install(install),
            }) => {
                assert!(install.force);
                assert_eq!(install.version.as_deref(), Some("v25.12.5.44-stable"));
            }
            _ => panic!("expected clickhouse install command"),
        }
    }

    #[test]
    fn clap_parses_db_doctor_deep_flag() {
        let cli = Cli::parse_from(["moraine", "db", "doctor", "--deep"]);
        match cli.command {
            CliCommand::Db(DbArgs {
                command: DbCommand::Doctor(doctor),
            }) => assert!(doctor.deep),
            _ => panic!("expected db doctor command"),
        }
    }

    #[test]
    fn clap_parses_up_backup_override_flag() {
        let cli = Cli::parse_from(["moraine", "up", "--no-backup-check"]);
        match cli.command {
            CliCommand::Up(up) => assert!(up.no_backup_check),
            _ => panic!("expected up command"),
        }
    }

    #[test]
    fn clap_parses_db_migrate_backup_override_flag() {
        let cli = Cli::parse_from(["moraine", "db", "migrate", "--no-backup-check"]);
        match cli.command {
            CliCommand::Db(DbArgs {
                command: DbCommand::Migrate(migrate),
            }) => assert!(migrate.no_backup_check),
            _ => panic!("expected db migrate command"),
        }
    }

    #[test]
    fn clap_parses_sources_status_flags() {
        let cli = Cli::parse_from(["moraine", "sources", "status", "--include-disabled"]);
        match cli.command {
            CliCommand::Sources(SourcesArgs {
                command: SourcesCommand::Status(status),
            }) => assert!(status.include_disabled),
            _ => panic!("expected sources status command"),
        }
    }

    #[test]
    fn clap_parses_sources_files_command() {
        let cli = Cli::parse_from(["moraine", "sources", "files", "codex"]);
        match cli.command {
            CliCommand::Sources(SourcesArgs {
                command: SourcesCommand::Files(files),
            }) => assert_eq!(files.source, "codex"),
            _ => panic!("expected sources files command"),
        }
    }

    #[test]
    fn clap_parses_sources_errors_command() {
        let cli = Cli::parse_from(["moraine", "sources", "errors", "codex", "--limit", "10"]);
        match cli.command {
            CliCommand::Sources(SourcesArgs {
                command: SourcesCommand::Errors(errors),
            }) => {
                assert_eq!(errors.source, "codex");
                assert_eq!(errors.limit, 10);
            }
            _ => panic!("expected sources errors command"),
        }
    }

    #[test]
    fn clap_parses_config_get_key() {
        let cli = Cli::parse_from(["moraine", "config", "get", "clickhouse.url"]);
        match cli.command {
            CliCommand::Config(ConfigArgs {
                command: ConfigCommand::Get(get),
            }) => assert_eq!(get.key, "clickhouse.url"),
            _ => panic!("expected config get command"),
        }
    }

    #[test]
    fn clap_parses_run_passthrough_args() {
        let cli = Cli::parse_from([
            "moraine",
            "--output",
            "plain",
            "run",
            "mcp",
            "--",
            "--stdio",
            "--transport",
            "jsonrpc",
        ]);
        match cli.command {
            CliCommand::Run(run) => {
                assert_eq!(run.service, Service::Mcp);
                assert_eq!(
                    run.args,
                    vec![
                        "--stdio".to_string(),
                        "--transport".to_string(),
                        "jsonrpc".to_string(),
                    ]
                );
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn cmd_config_get_returns_supported_keys() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:18123".to_string();
        cfg.clickhouse.database = "analytics".to_string();

        assert_eq!(
            cmd_config_get(&cfg, "clickhouse.url").expect("url"),
            "http://127.0.0.1:18123"
        );
        assert_eq!(
            cmd_config_get(&cfg, "clickhouse.database").expect("database"),
            "analytics"
        );
    }

    #[test]
    fn cmd_config_get_rejects_unknown_key() {
        let cfg = AppConfig::default();
        let err = cmd_config_get(&cfg, "runtime.root_dir").expect_err("unknown key");
        assert!(err.to_string().contains("unsupported config key"));
    }

    #[test]
    fn output_mode_respects_json_flag() {
        let cli = Cli::parse_from(["moraine", "--output", "json", "status"]);
        let output = CliOutput::from_cli(&cli);
        assert_eq!(output.mode, OutputMode::Json);
    }

    #[test]
    fn clickhouse_ports_follow_url_port() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "http://127.0.0.1:18123".to_string();
        let ports = clickhouse_ports_from_url(&cfg).expect("ports");
        assert_eq!(ports, (18123, 19000, 19009));
    }

    #[test]
    fn clickhouse_ports_require_valid_url() {
        let mut cfg = AppConfig::default();
        cfg.clickhouse.url = "not-a-url".to_string();
        let err = clickhouse_ports_from_url(&cfg).expect_err("invalid url");
        assert!(err.to_string().contains("invalid clickhouse.url"));
    }

    #[test]
    fn monitor_runtime_url_uses_configured_bind() {
        let mut cfg = AppConfig::default();
        cfg.monitor.host = "127.0.0.1".to_string();
        cfg.monitor.port = 18080;
        assert_eq!(monitor_runtime_url(&cfg), "http://127.0.0.1:18080");
    }

    #[test]
    fn monitor_runtime_url_wraps_ipv6_host() {
        let mut cfg = AppConfig::default();
        cfg.monitor.host = "::1".to_string();
        cfg.monitor.port = 18080;
        assert_eq!(monitor_runtime_url(&cfg), "http://[::1]:18080");
    }

    #[test]
    fn monitor_runtime_running_checks_monitor_pid() {
        let services = vec![
            ServiceRuntimeStatus {
                service: Service::ClickHouse,
                pid: Some(100),
                supervisor: "pid_file".to_string(),
            },
            ServiceRuntimeStatus {
                service: Service::Monitor,
                pid: Some(200),
                supervisor: "launchd:local.moraine.monitor".to_string(),
            },
        ];
        assert!(monitor_runtime_running(&services));

        let stopped_monitor = vec![ServiceRuntimeStatus {
            service: Service::Monitor,
            pid: None,
            supervisor: "none".to_string(),
        }];
        assert!(!monitor_runtime_running(&stopped_monitor));
    }

    #[test]
    fn parse_launchctl_pid_reads_running_job_pid() {
        let output = r#"
gui/501/local.moraine.monitor = {
    state = running
    pid = 96013
}
"#;

        assert_eq!(parse_launchctl_pid(output), Some(96013));
        assert_eq!(parse_launchctl_pid("state = waiting"), None);
    }

    #[test]
    fn resolve_service_binary_prefers_env_then_config() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver");
        let env_dir = root.join("env");
        let cfg_dir = root.join("cfg");
        let env_bin = env_dir.join("moraine-ingest");
        let cfg_bin = cfg_dir.join("moraine-ingest");
        write_file(&env_bin);
        write_file(&cfg_bin);

        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = cfg_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        std::env::set_var("MORAINE_SERVICE_BIN_DIR", &env_dir);
        assert_eq!(
            resolve_service_binary(Service::Ingest, &paths).resolved_path,
            Some(env_bin.clone())
        );

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        assert_eq!(
            resolve_service_binary(Service::Ingest, &paths).resolved_path,
            Some(cfg_bin)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_service_binary_reports_missing_without_path_fallback() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver-path");
        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = root.join("missing").to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        let resolved = resolve_service_binary(Service::Mcp, &paths);
        assert_eq!(resolved.binary_name, "moraine-mcp");
        assert!(resolved.resolved_path.is_none());
        assert!(resolved
            .checked_paths
            .iter()
            .any(|probe| probe.source == "runtime.service_bin_dir"
                && probe.path == paths.service_bin_dir.join("moraine-mcp")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn require_service_binary_includes_remediation() {
        let _env_lock = lock_env_vars();
        let _service_bin_dir_guard = EnvVarGuard::capture("MORAINE_SERVICE_BIN_DIR");
        let _source_tree_mode_guard = EnvVarGuard::capture("MORAINE_SOURCE_TREE_MODE");

        let root = temp_dir("resolver-remediation");
        let mut cfg = AppConfig::default();
        cfg.runtime.service_bin_dir = root.join("missing").to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        std::env::remove_var("MORAINE_SERVICE_BIN_DIR");
        std::env::remove_var("MORAINE_SOURCE_TREE_MODE");
        let err = require_service_binary(Service::Mcp, &paths).expect_err("missing mcp binary");
        let message = err.to_string();
        assert!(message.contains("required service binary `moraine-mcp`"));
        assert!(message.contains("runtime.service_bin_dir"));
        assert!(message.contains("MORAINE_SERVICE_BIN_DIR"));
        assert!(message.contains("cargo build --workspace --locked"));
        assert!(message.contains("does not fall back to PATH"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn managed_checksum_state_reports_verified() {
        let root = temp_dir("checksum");
        let managed_dir = root.join("managed");
        fs::create_dir_all(&managed_dir).expect("managed dir");

        let mut cfg = AppConfig::default();
        cfg.runtime.managed_clickhouse_dir = managed_dir.to_string_lossy().to_string();
        let paths = runtime_paths(&cfg);

        let expected = clickhouse_asset_for_host(&cfg.runtime.clickhouse_version)
            .expect("host asset")
            .sha256;
        fs::write(
            managed_clickhouse_checksum_file(&paths),
            format!("{expected}\n"),
        )
        .expect("write checksum");

        assert_eq!(managed_clickhouse_checksum_state(&cfg, &paths), "verified");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn archive_verify_passes_for_valid_archive() {
        let root = temp_dir("archive-verify");
        let manifest = ArchiveManifest {
            schema_version: "0.4.3".to_string(),
            moraine_version: "0.4.3".to_string(),
            exported_at: "12345".to_string(),
            tables: vec![ArchiveTableManifest {
                name: "events".to_string(),
                rows: 2,
                file: "events.jsonl".to_string(),
            }],
        };
        fs::write(
            root.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .expect("write manifest");
        fs::write(
            root.join("events.jsonl"),
            "{\"event_uid\":\"a\"}\n{\"event_uid\":\"b\"}\n",
        )
        .expect("write events");

        let verified = cmd_archive_verify(&root).expect("verify");
        assert_eq!(verified.tables[0].rows, 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_sync_preview_mode_still_works() {
        let root = temp_dir("import-sync-preview");
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.imports.insert(
            "pc".to_string(),
            moraine_config::ImportProfile {
                host: "pc".to_string(),
                remote_paths: vec!["~/.codex/sessions".to_string()],
                local_mirror: root.join("mirror").display().to_string(),
                include_patterns: vec!["**/*.jsonl".to_string()],
                exclude_patterns: vec!["**/.git".to_string()],
                cadence: "manual".to_string(),
            },
        );

        let result = run_async(cmd_import_sync(&cfg, "pc", true)).expect("preview sync");
        assert!(result.success);
        assert_eq!(result.manifest.files_copied, 0);
        assert_eq!(result.manifest.bytes_copied, 0);
        assert!(!sync_manifest_path(&cfg, "pc").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_sync_execute_rejects_missing_rsync() {
        let _env_lock = lock_env_vars();
        let _path_guard = EnvVarGuard::capture("PATH");

        let root = temp_dir("import-sync-missing-rsync");
        let empty_path = root.join("bin");
        fs::create_dir_all(&empty_path).expect("create empty path dir");
        std::env::set_var("PATH", &empty_path);

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();
        cfg.imports.insert(
            "pc".to_string(),
            moraine_config::ImportProfile {
                host: "pc".to_string(),
                remote_paths: vec!["~/.codex/sessions".to_string()],
                local_mirror: root.join("mirror").display().to_string(),
                include_patterns: Vec::new(),
                exclude_patterns: Vec::new(),
                cadence: "manual".to_string(),
            },
        );

        let err = run_async(cmd_import_sync(&cfg, "pc", false)).expect_err("missing rsync");
        assert!(err.to_string().contains("rsync is required"));
        assert!(!sync_manifest_path(&cfg, "pc").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sync_manifest_round_trip_and_render_lines_include_error() {
        let root = temp_dir("import-sync-manifest");
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();

        let profile = moraine_config::ImportProfile {
            host: "pc".to_string(),
            remote_paths: vec!["~/.codex/sessions".to_string()],
            local_mirror: root.join("mirror").display().to_string(),
            include_patterns: Vec::new(),
            exclude_patterns: Vec::new(),
            cadence: "manual".to_string(),
        };
        let mut manifest = build_sync_manifest("pc", &profile);
        manifest.files_copied = 3;
        manifest.bytes_copied = 42;
        manifest.duration_ms = 99;
        manifest.last_error = Some("rsync failed".to_string());

        write_sync_manifest(&cfg, "pc", &manifest).expect("write sync manifest");
        let round_trip = read_sync_manifest(&cfg, "pc")
            .expect("read manifest")
            .expect("manifest exists");
        assert_eq!(round_trip.profile_name, manifest.profile_name);
        assert_eq!(round_trip.local_mirror, manifest.local_mirror);
        assert_eq!(round_trip.files_copied, 3);
        assert_eq!(round_trip.last_error.as_deref(), Some("rsync failed"));

        let lines = import_sync_lines(&SyncResult {
            profile_name: "pc".to_string(),
            success: false,
            manifest: round_trip,
        });
        assert!(lines.iter().any(|line| line.contains("mirror:")));
        assert!(lines
            .iter()
            .any(|line| line.contains("last error: rsync failed")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_rsync_stats_extracts_transfer_counts() {
        let stats = parse_rsync_stats(
            r#"
Number of files: 8 (reg: 5, dir: 3)
Number of regular files transferred: 2
Total transferred file size: 1,024 bytes
"#,
        );

        assert_eq!(stats.files_copied, 2);
        assert_eq!(stats.bytes_copied, 1024);
        assert_eq!(stats.files_skipped, 6);
    }

    #[test]
    fn archive_verify_fails_when_row_count_mismatch() {
        let root = temp_dir("archive-verify-mismatch");
        let manifest = ArchiveManifest {
            schema_version: "0.4.3".to_string(),
            moraine_version: "0.4.3".to_string(),
            exported_at: "12345".to_string(),
            tables: vec![ArchiveTableManifest {
                name: "events".to_string(),
                rows: 5,
                file: "events.jsonl".to_string(),
            }],
        };
        fs::write(
            root.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .expect("write manifest");
        fs::write(root.join("events.jsonl"), "{\"event_uid\":\"a\"}\n").expect("write events");

        let err = cmd_archive_verify(&root).expect_err("verify should fail");
        assert!(err.to_string().contains("row count mismatch"));
        let _ = fs::remove_dir_all(root);
    }

    fn test_backup_manifest(table_file: &str, table_bytes: &[u8]) -> BackupManifest {
        BackupManifest {
            manifest_version: BACKUP_MANIFEST_VERSION,
            backup_id: "backup-test".to_string(),
            created_unix_seconds: 42,
            moraine_version: env!("CARGO_PKG_VERSION").to_string(),
            clickhouse_database: "moraine".to_string(),
            clickhouse_version: Some("25.12.5.44".to_string()),
            include_derived: false,
            bundled_migrations: vec!["001".to_string()],
            applied_migrations: vec!["001".to_string()],
            privacy_key_material: "external_not_included".to_string(),
            sources: vec![BackupSourceInventory {
                name: "codex".to_string(),
                harness: "codex".to_string(),
                enabled: true,
                glob: "/tmp/codex/**/*.jsonl".to_string(),
                watch_root: "/tmp/codex".to_string(),
                format: "jsonl".to_string(),
            }],
            tables: vec![BackupTableManifest {
                name: "events".to_string(),
                kind: "base".to_string(),
                file: table_file.to_string(),
                row_count: count_jsonl_rows_bytes(table_bytes).unwrap(),
                sha256: sha256_bytes_hex(table_bytes),
            }],
        }
    }

    fn write_test_backup(root: &Path, manifest: &BackupManifest, table_bytes: &[u8]) {
        fs::write(
            root.join(BACKUP_MANIFEST_FILE),
            serde_json::to_string_pretty(manifest).unwrap(),
        )
        .expect("write manifest");
        let table_path = root.join(&manifest.tables[0].file);
        fs::create_dir_all(table_path.parent().expect("table parent")).expect("create table dir");
        fs::write(table_path, table_bytes).expect("write table file");
    }

    #[test]
    fn backup_verify_passes_for_valid_backup() {
        let root = temp_dir("backup-verify-ok");
        let table_bytes = br#"{"event_uid":"a"}
{"event_uid":"b"}
"#;
        let manifest = test_backup_manifest("tables/events.jsonl", table_bytes);
        write_test_backup(&root, &manifest, table_bytes);

        let snapshot = cmd_backup_verify(&root);
        assert!(snapshot.ok, "{:?}", snapshot.errors);
        assert_eq!(
            snapshot.manifest.as_ref().unwrap().tables[0].sha256,
            sha256_bytes_hex(table_bytes)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_verify_reports_checksum_mismatch() {
        let root = temp_dir("backup-verify-checksum");
        let original = br#"{"event_uid":"a"}
"#;
        let manifest = test_backup_manifest("tables/events.jsonl", original);
        write_test_backup(
            &root,
            &manifest,
            br#"{"event_uid":"changed"}
"#,
        );

        let snapshot = cmd_backup_verify(&root);
        assert!(!snapshot.ok);
        assert!(snapshot
            .errors
            .iter()
            .any(|error| error.contains("sha256 mismatch")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_verify_rejects_unsafe_manifest_paths() {
        let root = temp_dir("backup-verify-unsafe");
        let table_bytes = br#"{"event_uid":"a"}
"#;
        let manifest = test_backup_manifest("../events.jsonl", table_bytes);
        fs::write(
            root.join(BACKUP_MANIFEST_FILE),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .expect("write manifest");

        let snapshot = cmd_backup_verify(&root);
        assert!(!snapshot.ok);
        assert!(snapshot
            .errors
            .iter()
            .any(|error| error.contains("unsafe relative path")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_list_reads_valid_manifests_and_skips_bad_ones() {
        let root = temp_dir("backup-list");
        let good = root.join("good");
        let bad = root.join("bad");
        fs::create_dir_all(&good).expect("create good");
        fs::create_dir_all(&bad).expect("create bad");
        let table_bytes = br#"{"event_uid":"a"}
"#;
        let manifest = test_backup_manifest("tables/events.jsonl", table_bytes);
        write_test_backup(&good, &manifest, table_bytes);
        fs::write(bad.join(BACKUP_MANIFEST_FILE), "{bad json").expect("write bad manifest");

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.display().to_string();
        let snapshot = cmd_backup_list(
            &cfg,
            &BackupListArgs {
                root: Some(root.clone()),
            },
        )
        .expect("list backups");
        assert_eq!(snapshot.backups.len(), 1);
        assert_eq!(snapshot.backups[0].backup_id, "backup-test");
        assert_eq!(snapshot.skipped.len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restore_plan_blocks_invalid_or_execute_requests() {
        let root = temp_dir("restore-plan");
        let cfg = AppConfig::default();
        let invalid = cmd_restore_plan(
            &cfg,
            &RestoreArgs {
                input: root.clone(),
                dry_run: true,
                execute: false,
                target_database: None,
            },
        );
        assert!(!invalid.can_restore);
        assert!(!invalid.blockers.is_empty());

        let table_bytes = br#"{"event_uid":"a"}
"#;
        let manifest = test_backup_manifest("tables/events.jsonl", table_bytes);
        write_test_backup(&root, &manifest, table_bytes);
        let execute = cmd_restore_plan(
            &cfg,
            &RestoreArgs {
                input: root.clone(),
                dry_run: false,
                execute: true,
                target_database: Some("restore_db".to_string()),
            },
        );
        assert!(!execute.can_restore);
        assert_eq!(execute.target_database, "restore_db");
        assert!(execute
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not implemented")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_gate_rejects_missing_verified_backup() {
        let root = temp_dir("backup-gate-missing");
        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();

        let err = require_recent_verified_backup(&cfg, "`moraine db migrate`", false)
            .expect_err("missing backup should fail");
        let message = err.to_string();
        assert!(message.contains("requires a recent verified backup"));
        assert!(message.contains("heuristic preflight"));
        assert!(message.contains("--no-backup-check"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_gate_rejects_stale_verified_backup() {
        let root = temp_dir("backup-gate-stale");
        let backups_root = root.join("backups");
        let backup_dir = backups_root.join("old");
        fs::create_dir_all(&backup_dir).expect("create backup dir");

        let table_bytes = br#"{"event_uid":"a"}
"#;
        let mut manifest = test_backup_manifest("tables/events.jsonl", table_bytes);
        manifest.created_unix_seconds =
            unix_now_seconds() - (DESTRUCTIVE_BACKUP_MAX_AGE_SECONDS + 60);
        write_test_backup(&backup_dir, &manifest, table_bytes);

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();

        let err = require_recent_verified_backup(&cfg, "`moraine db migrate`", false)
            .expect_err("stale backup should fail");
        let message = err.to_string();
        assert!(message.contains("latest verified backup"));
        assert!(message.contains("threshold 24h"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_gate_accepts_recent_verified_backup_for_current_database() {
        let root = temp_dir("backup-gate-fresh");
        let backups_root = root.join("backups");
        let backup_dir = backups_root.join("fresh");
        fs::create_dir_all(&backup_dir).expect("create backup dir");

        let table_bytes = br#"{"event_uid":"a"}
"#;
        let mut manifest = test_backup_manifest("tables/events.jsonl", table_bytes);
        manifest.created_unix_seconds = unix_now_seconds();
        write_test_backup(&backup_dir, &manifest, table_bytes);

        let mut cfg = AppConfig::default();
        cfg.runtime.root_dir = root.to_string_lossy().to_string();

        require_recent_verified_backup(&cfg, "`moraine db migrate`", false)
            .expect("fresh verified backup should pass");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_gate_allows_explicit_override() {
        let cfg = AppConfig::default();
        require_recent_verified_backup(&cfg, "`moraine db migrate`", true)
            .expect("override should bypass backup gate");
    }

    #[test]
    fn migration_backup_required_only_for_existing_db_with_pending_migrations() {
        let mut report = test_doctor_report(true);
        report.database_exists = false;
        report.pending_migrations = vec!["014".to_string()];
        assert!(!migration_backup_required(&report));

        report.database_exists = true;
        report.pending_migrations.clear();
        assert!(!migration_backup_required(&report));

        report.pending_migrations = vec!["014".to_string()];
        assert!(migration_backup_required(&report));
    }

    #[test]
    fn clap_parses_import_sync_name() {
        let cli = Cli::parse_from(["moraine", "import", "sync", "vm503"]);
        match cli.command {
            CliCommand::Import(ImportArgs {
                command: ImportCommand::Sync(sync),
            }) => {
                assert_eq!(sync.name, "vm503");
                assert!(preview_mode(sync.dry_run, sync.execute));
            }
            _ => panic!("expected import sync command"),
        }
    }

    #[test]
    fn clap_parses_archive_export_flags() {
        let cli = Cli::parse_from([
            "moraine",
            "archive",
            "export",
            "--out-dir",
            "/tmp/export",
            "--session-ids",
            "a,b",
            "--since",
            "7d",
            "--raw",
        ]);
        match cli.command {
            CliCommand::Archive(ArchiveArgs {
                command: ArchiveCommand::Export(export),
            }) => {
                assert_eq!(export.out_dir, PathBuf::from("/tmp/export"));
                assert_eq!(
                    export.session_ids,
                    Some(vec!["a".to_string(), "b".to_string()])
                );
                assert_eq!(export.since, Some("7d".to_string()));
                assert!(export.raw);
                assert!(preview_mode(export.dry_run, export.execute));
            }
            _ => panic!("expected archive export command"),
        }
    }

    #[test]
    fn clap_parses_archive_import_dry_run() {
        let cli = Cli::parse_from([
            "moraine",
            "archive",
            "import",
            "--input",
            "/tmp/import",
            "--dry-run",
        ]);
        match cli.command {
            CliCommand::Archive(ArchiveArgs {
                command: ArchiveCommand::Import(import),
            }) => {
                assert_eq!(import.input, PathBuf::from("/tmp/import"));
                assert!(import.dry_run);
                assert!(!import.execute);
            }
            _ => panic!("expected archive import command"),
        }
    }

    #[test]
    fn clap_parses_archive_export_execute() {
        let cli = Cli::parse_from([
            "moraine",
            "archive",
            "export",
            "--out-dir",
            "/tmp/export",
            "--execute",
        ]);
        match cli.command {
            CliCommand::Archive(ArchiveArgs {
                command: ArchiveCommand::Export(export),
            }) => {
                assert!(export.execute);
                assert!(!preview_mode(export.dry_run, export.execute));
            }
            _ => panic!("expected archive export command"),
        }
    }

    #[test]
    fn clap_parses_backup_and_restore_commands() {
        let create = Cli::parse_from([
            "moraine",
            "backup",
            "create",
            "--out-dir",
            "/tmp/backup",
            "--include-derived",
        ]);
        match create.command {
            CliCommand::Backup(BackupArgs {
                command: BackupCommand::Create(args),
            }) => {
                assert_eq!(args.out_dir, Some(PathBuf::from("/tmp/backup")));
                assert!(args.include_derived);
            }
            _ => panic!("expected backup create command"),
        }

        let list = Cli::parse_from(["moraine", "backup", "list", "--root", "/tmp/backups"]);
        match list.command {
            CliCommand::Backup(BackupArgs {
                command: BackupCommand::List(args),
            }) => assert_eq!(args.root, Some(PathBuf::from("/tmp/backups"))),
            _ => panic!("expected backup list command"),
        }

        let verify = Cli::parse_from(["moraine", "backup", "verify", "/tmp/backups/one"]);
        match verify.command {
            CliCommand::Backup(BackupArgs {
                command: BackupCommand::Verify(args),
            }) => assert_eq!(args.path, PathBuf::from("/tmp/backups/one")),
            _ => panic!("expected backup verify command"),
        }

        let restore = Cli::parse_from([
            "moraine",
            "restore",
            "--input",
            "/tmp/backups/one",
            "--target-database",
            "restore_db",
        ]);
        match restore.command {
            CliCommand::Restore(args) => {
                assert_eq!(args.input, PathBuf::from("/tmp/backups/one"));
                assert_eq!(args.target_database, Some("restore_db".to_string()));
                assert!(preview_mode(args.dry_run, args.execute));
            }
            _ => panic!("expected restore command"),
        }
    }

    #[test]
    fn clap_parses_reindex_search_only_flags() {
        let preview = Cli::parse_from(["moraine", "reindex", "--search-only"]);
        match preview.command {
            CliCommand::Reindex(args) => {
                assert!(args.search_only);
                assert!(preview_mode(args.dry_run, args.execute));
                assert!(!args.no_backup_check);
            }
            _ => panic!("expected reindex command"),
        }

        let execute = Cli::parse_from([
            "moraine",
            "reindex",
            "--search-only",
            "--execute",
            "--no-backup-check",
        ]);
        match execute.command {
            CliCommand::Reindex(args) => {
                assert!(args.search_only);
                assert!(args.execute);
                assert!(args.no_backup_check);
                assert!(!preview_mode(args.dry_run, args.execute));
            }
            _ => panic!("expected reindex command"),
        }
    }

    #[test]
    fn doctor_is_healthy_fails_on_unsupported_clickhouse_version() {
        let mut report = test_doctor_report(true);
        report.clickhouse_version = Some("25.8.1.1".to_string());
        report.clickhouse_version_compatibility = ClickHouseVersionCompatibility::Unsupported;
        report.clickhouse_version_line = Some("25.8".to_string());

        assert!(!doctor_is_healthy(&DoctorSnapshot::Basic(report)));
    }

    #[test]
    fn clap_parses_config_detect_json() {
        let cli = Cli::parse_from(["moraine", "config", "detect", "--json"]);
        match cli.command {
            CliCommand::Config(ConfigArgs {
                command: ConfigCommand::Detect(detect),
            }) => assert!(detect.json),
            _ => panic!("expected config detect command"),
        }
    }

    #[test]
    fn cmd_config_validate_reports_overlap() {
        use moraine_config::IngestSource;
        let mut cfg = AppConfig::default();
        cfg.ingest.sources = vec![
            IngestSource {
                name: "a".to_string(),
                harness: "codex".to_string(),
                enabled: true,
                glob: "/tmp/a/**/*.jsonl".to_string(),
                watch_root: "/tmp/a".to_string(),
                format: "jsonl".to_string(),
            },
            IngestSource {
                name: "b".to_string(),
                harness: "codex".to_string(),
                enabled: true,
                glob: "/tmp/a/b/**/*.jsonl".to_string(),
                watch_root: "/tmp/a/b".to_string(),
                format: "jsonl".to_string(),
            },
        ];
        let snapshot = cmd_config_validate(&cfg);
        assert!(!snapshot.ok);
        assert!(snapshot.issues.iter().any(|i| matches!(
            i,
            moraine_config::SourceValidationIssue::OverlappingWatchRoot { .. }
        )));
    }

    #[test]
    fn reindex_lines_render_preview_counts() {
        let lines = reindex_lines(&ReindexSnapshot {
            mode: "dry_run".to_string(),
            target: "search_only".to_string(),
            database: "moraine".to_string(),
            current: ReindexPreviewCounts {
                events: 12,
                search_documents: 8,
                search_postings: 16,
                search_conversation_terms: 4,
            },
            projected: ReindexPreviewCounts {
                events: 12,
                search_documents: 10,
                search_postings: 20,
                search_conversation_terms: 5,
            },
            notes: vec!["canonical tables are untouched".to_string()],
        });

        assert!(lines.iter().any(|line| line == "mode: dry_run"));
        assert!(lines.iter().any(|line| line == "search_documents: 8 -> 10"));
        assert!(lines
            .iter()
            .any(|line| line == "note: canonical tables are untouched"));
    }

    #[test]
    fn search_rebuild_sql_helpers_track_current_schema_sql() {
        let documents = search_documents_select_sql().expect("documents sql");
        assert!(documents.contains("privacy_policy_version"));
        assert!(documents.contains("FROM events"));
        assert!(!documents.contains("moraine.events"));

        let postings = search_postings_select_sql().expect("postings sql");
        assert!(postings.contains("FROM\n(\n  SELECT"));
        assert!(postings.contains("FROM search_documents"));

        let conversation = search_conversation_terms_select_sql().expect("conversation sql");
        assert!(conversation.contains("FROM search_postings FINAL"));
        assert!(!conversation.contains("INSERT INTO"));
    }
}
