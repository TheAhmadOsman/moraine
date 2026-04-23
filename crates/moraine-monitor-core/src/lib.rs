use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::time::Instant;

use moraine_clickhouse::ClickHouseClient;
use moraine_config::AppConfig;
use moraine_source_status::{
    build_source_detail_snapshot, build_source_errors_snapshot, build_source_files_snapshot,
    build_source_status_snapshot,
};

#[derive(Clone)]
struct AppState {
    clickhouse: ClickHouseClient,
    static_dir: PathBuf,
    cfg: AppConfig,
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct AnalyticsQuery {
    range: Option<String>,
}

#[derive(Serialize)]
struct TableSummary {
    name: String,
    engine: String,
    is_temporary: u8,
    rows: u64,
}

pub async fn run_server(
    cfg: AppConfig,
    host: String,
    port: u16,
    static_dir: PathBuf,
) -> Result<()> {
    validate_static_dir(&static_dir)?;

    let clickhouse = ClickHouseClient::new(cfg.clickhouse.clone())?;

    let state = AppState {
        clickhouse,
        static_dir,
        cfg,
    };

    let app = Router::new()
        .route("/api/health", get(api_health))
        .route("/api/status", get(api_status))
        .route("/api/analytics", get(api_analytics))
        .route("/api/tables", get(api_tables))
        .route("/api/web-searches", get(api_web_searches))
        .route("/api/tables/:table", get(api_table_rows))
        .route("/api/sessions", get(api_sessions))
        .route("/api/sessions/:session_id", get(api_session_detail))
        .route("/api/sources", get(api_sources))
        .route("/api/sources/:source", get(api_source_detail))
        .route("/api/sources/:source/files", get(api_source_files))
        .route("/api/sources/:source/errors", get(api_source_errors))
        .fallback(get(static_fallback))
        .with_state(state.clone());

    let bind = format!("{}:{}", host, port)
        .parse::<SocketAddr>()
        .map_err(|err| anyhow!("invalid bind address: {err}"))?;

    println!("moraine-monitor running at http://{}", bind);
    println!("serving UI from {}", state.static_dir.display());

    let listener = tokio::net::TcpListener::bind(bind).await.map_err(|error| {
        if error.kind() == ErrorKind::AddrInUse {
            anyhow!(
                "failed to bind {bind}: address already in use. another monitor may already be running (including legacy cortex-monitor). stop it or rerun with `moraine run monitor -- --port <free-port>`"
            )
        } else {
            anyhow!("failed to bind {bind}: {error}")
        }
    })?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn validate_static_dir(static_dir: &FsPath) -> Result<()> {
    let metadata = std::fs::metadata(static_dir).map_err(|error| {
        anyhow!(
            "monitor static directory `{}` is unavailable: {error}. if running from source, build UI assets with `(cd web/monitor && bun install --frozen-lockfile && bun run build)`; otherwise ensure packaged `web/monitor/dist` assets are installed or pass `--static-dir <path>`",
            static_dir.display()
        )
    })?;

    if !metadata.is_dir() {
        return Err(anyhow!(
            "monitor static directory `{}` is not a directory; pass `--static-dir <path>` pointing to a built monitor dist directory",
            static_dir.display()
        ));
    }

    let index_path = static_dir.join("index.html");
    if !index_path.is_file() {
        return Err(anyhow!(
            "monitor static directory `{}` does not contain `index.html`; build monitor assets or pass `--static-dir <path>`",
            static_dir.display()
        ));
    }

    Ok(())
}

fn json_response<T: Serialize>(payload: T, status: StatusCode) -> Response {
    let mut response = Json(payload).into_response();
    *response.status_mut() = status;
    response
}

fn clickhouse_ping_payload(version: String, ping_result: Result<()>, ping_ms: f64) -> Value {
    match ping_result {
        Ok(()) => json!({
            "healthy": true,
            "version": version,
            "ping_ms": ping_ms,
            "error": Value::Null,
        }),
        Err(error) => json!({
            "healthy": false,
            "version": version,
            "ping_ms": ping_ms,
            "error": error.to_string(),
        }),
    }
}

async fn api_health(State(state): State<AppState>) -> Response {
    let start = Instant::now();
    let connection_stats = query_clickhouse_connections(&state)
        .await
        .unwrap_or_else(|error| json!({"total": Value::Null, "error": error.to_string()}));

    match state.clickhouse.ping().await {
        Ok(()) => match state.clickhouse.version().await {
            Ok(version) => {
                let body = json!({
                    "ok": true,
                    "url": state.clickhouse.config().url,
                    "database": state.clickhouse.config().database,
                    "version": version,
                    "ping_ms": (Instant::elapsed(&start).as_secs_f64() * 1000.0),
                    "connections": connection_stats,
                });
                json_response(body, StatusCode::OK)
            }
            Err(error) => json_response(
                json!({
                    "ok": false,
                    "url": state.clickhouse.config().url,
                    "database": state.clickhouse.config().database,
                    "error": error.to_string(),
                    "connections": connection_stats,
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        },
        Err(error) => json_response(
            json!({
                "ok": false,
                "url": state.clickhouse.config().url,
                "database": state.clickhouse.config().database,
                "error": error.to_string(),
                "connections": connection_stats,
            }),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

async fn api_status(State(state): State<AppState>) -> Response {
    let db_exists = match state
        .clickhouse
        .query_rows::<Value>(
            &format!(
                "SELECT name FROM system.databases WHERE name = '{}'",
                escape_literal(&state.clickhouse.config().database)
            ),
            None,
        )
        .await
    {
        Ok(rows) => !rows.is_empty(),
        Err(_) => false,
    };

    let tables = if db_exists {
        match query_table_summaries(&state).await {
            Ok(value) => value,
            Err(error) => {
                return json_response(
                    json!({"ok": false, "error": error.to_string()}),
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            }
        }
    } else {
        Vec::new()
    };

    let estimated_total_rows = tables.iter().map(|table| table.rows).sum::<u64>();
    let heartbeat = if db_exists {
        query_heartbeat(&state).await.unwrap_or_else(|_| {
            json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null})
        })
    } else {
        json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null})
    };

    let clickhouse_ping = if db_exists {
        match state.clickhouse.version().await {
            Ok(version) => {
                let start = Instant::now();
                let ping_result = state.clickhouse.ping().await;
                let ping_ms = Instant::elapsed(&start).as_secs_f64() * 1000.0;
                clickhouse_ping_payload(version, ping_result, ping_ms)
            }
            Err(error) => {
                json!({
                    "healthy": false,
                    "version": Value::Null,
                    "ping_ms": Value::Null,
                    "error": error.to_string(),
                })
            }
        }
    } else {
        json!({
            "healthy": false,
            "version": Value::Null,
            "ping_ms": Value::Null,
            "error": "database not found",
        })
    };

    json_response(
        json!({
            "ok": true,
            "clickhouse": {
                "url": state.clickhouse.config().url,
            "database": state.clickhouse.config().database,
            "healthy": clickhouse_ping["healthy"],
            "version": clickhouse_ping["version"],
            "ping_ms": clickhouse_ping["ping_ms"],
            "error": clickhouse_ping["error"],
            "connections": if db_exists {
                query_clickhouse_connections(&state)
                    .await
                    .unwrap_or_else(|error| json!({"total": Value::Null, "error": error.to_string()}))
            } else {
                json!({"total": Value::Null, "error": "database not found"})
            },
        },
        "database": {
            "exists": db_exists,
            "table_count": tables.len(),
            "estimated_total_rows": estimated_total_rows,
                "tables": tables,
            },
            "ingestor": heartbeat,
        }),
        StatusCode::OK,
    )
}

async fn api_tables(State(state): State<AppState>) -> Response {
    match query_table_summaries(&state).await {
        Ok(tables) => json_response(json!({"ok": true, "tables": tables}), StatusCode::OK),
        Err(error) => json_response(
            json!({"ok": false, "error": error.to_string()}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

async fn api_web_searches(
    Query(params): Query<LimitQuery>,
    State(state): State<AppState>,
) -> Response {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct WebSearchRow {
        event_time: String,
        harness: String,
        source_name: String,
        session_id: String,
        model: String,
        action: String,
        search_query: String,
        result_url: String,
        source_ref: String,
    }

    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let database = &state.clickhouse.config().database;
    let table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );

    let query = format!(
        "SELECT \
            toString(event_ts) AS event_time, \
            harness, \
            source_name, \
            session_id, \
            lowerUTF8(trim(BOTH ' ' FROM model)) AS model, \
            if(payload_type = 'web_search_call', op_kind, if(tool_name = 'WebFetch', 'open_page', if(tool_name = 'WebSearch', 'search', payload_type))) AS action, \
            if(length(JSONExtractString(payload_json, 'action', 'query')) > 0, \
               JSONExtractString(payload_json, 'action', 'query'), \
               if(length(JSONExtractString(payload_json, 'input', 'query')) > 0, \
                  JSONExtractString(payload_json, 'input', 'query'), \
                  if(length(JSONExtractString(payload_json, 'data', 'query')) > 0, \
                     JSONExtractString(payload_json, 'data', 'query'), \
                     text_content))) AS search_query, \
            if(length(JSONExtractString(payload_json, 'action', 'url')) > 0, \
               JSONExtractString(payload_json, 'action', 'url'), \
               JSONExtractString(payload_json, 'input', 'url')) AS result_url, \
            source_ref \
         FROM {table} \
         WHERE payload_type = 'web_search_call' \
            OR (payload_type = 'tool_use' AND tool_name IN ('WebSearch', 'WebFetch')) \
            OR payload_type = 'search_results_received' \
         ORDER BY event_ts DESC \
         LIMIT {limit}",
        table = table,
        limit = limit,
    );

    let rows = match state
        .clickhouse
        .query_rows::<WebSearchRow>(&query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("web search query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    json_response(
        json!({
            "ok": true,
            "table": "web_searches",
            "limit": limit,
            "schema": [
                {"name": "event_time", "type": "String", "default_expression": ""},
                {"name": "harness", "type": "String", "default_expression": ""},
                {"name": "source_name", "type": "String", "default_expression": ""},
                {"name": "session_id", "type": "String", "default_expression": ""},
                {"name": "model", "type": "String", "default_expression": ""},
                {"name": "action", "type": "String", "default_expression": ""},
                {"name": "search_query", "type": "String", "default_expression": ""},
                {"name": "result_url", "type": "String", "default_expression": ""},
                {"name": "source_ref", "type": "String", "default_expression": ""}
            ],
            "rows": rows,
        }),
        StatusCode::OK,
    )
}

#[derive(Clone, Copy)]
struct AnalyticsRange {
    key: &'static str,
    label: &'static str,
    window_seconds: u32,
    bucket_seconds: u32,
}

fn resolve_analytics_range(value: Option<&str>) -> AnalyticsRange {
    match value.unwrap_or("24h") {
        "15m" => AnalyticsRange {
            key: "15m",
            label: "Last 15m",
            window_seconds: 15 * 60,
            bucket_seconds: 60,
        },
        "1h" => AnalyticsRange {
            key: "1h",
            label: "Last 1h",
            window_seconds: 60 * 60,
            bucket_seconds: 5 * 60,
        },
        "6h" => AnalyticsRange {
            key: "6h",
            label: "Last 6h",
            window_seconds: 6 * 60 * 60,
            bucket_seconds: 15 * 60,
        },
        "24h" => AnalyticsRange {
            key: "24h",
            label: "Last 24h",
            window_seconds: 24 * 60 * 60,
            bucket_seconds: 60 * 60,
        },
        "7d" => AnalyticsRange {
            key: "7d",
            label: "Last 7d",
            window_seconds: 7 * 24 * 60 * 60,
            bucket_seconds: 6 * 60 * 60,
        },
        "30d" => AnalyticsRange {
            key: "30d",
            label: "Last 30d",
            window_seconds: 30 * 24 * 60 * 60,
            bucket_seconds: 24 * 60 * 60,
        },
        _ => AnalyticsRange {
            key: "24h",
            label: "Last 24h",
            window_seconds: 24 * 60 * 60,
            bucket_seconds: 60 * 60,
        },
    }
}

fn analytics_window_query(table: &str, window_filter: &str, window_seconds: u32) -> String {
    format!(
        "SELECT \
           toUInt64(anchor_unix) AS now_unix, \
           toUInt64(greatest(anchor_unix - toInt64({window_seconds}), toInt64(0))) AS from_unix \
         FROM ( \
           SELECT \
             if( \
               count() = 0, \
               toInt64(toUnixTimestamp(now())), \
               toInt64(intDiv(toUnixTimestamp64Milli(max(event_ts)), 1000)) \
             ) AS anchor_unix \
           FROM {table} \
           WHERE {window_filter} \
         )",
    )
}

async fn api_analytics(
    Query(params): Query<AnalyticsQuery>,
    State(state): State<AppState>,
) -> Response {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct TokenRow {
        bucket_unix: u64,
        model: String,
        tokens: u64,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct TurnRow {
        bucket_unix: u64,
        model: String,
        turns: u64,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct ConcurrentRow {
        bucket_unix: u64,
        concurrent_sessions: u64,
    }

    let range = resolve_analytics_range(params.range.as_deref());
    let database = &state.clickhouse.config().database;
    let table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );
    let model_expr = "if(lowerUTF8(trim(BOTH ' ' FROM model)) = 'codex', 'gpt-5.3-codex-xhigh', lowerUTF8(trim(BOTH ' ' FROM model)))";
    let model_expr_latest = "if(lowerUTF8(trim(BOTH ' ' FROM model_latest)) = 'codex', 'gpt-5.3-codex-xhigh', lowerUTF8(trim(BOTH ' ' FROM model_latest)))";
    let window_filter = format!(
        "event_ts >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM model)) > 0 AND lowerUTF8(trim(BOTH ' ' FROM model)) != '<synthetic>'",
        range.window_seconds
    );
    let generation_latest_filter = format!(
        "event_ts_latest >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM model_latest)) > 0 AND lowerUTF8(trim(BOTH ' ' FROM model_latest)) != '<synthetic>' AND output_tokens_latest > 0",
        range.window_seconds
    );
    let turn_filter = format!(
        "{} AND length(trim(BOTH ' ' FROM request_id)) > 0",
        window_filter
    );
    let concurrent_filter = format!(
        "event_ts >= now() - INTERVAL {} SECOND AND length(trim(BOTH ' ' FROM session_id)) > 0 AND (input_tokens > 0 OR output_tokens > 0 OR cache_read_tokens > 0 OR cache_write_tokens > 0)",
        range.window_seconds
    );

    let token_query = format!(
        "SELECT bucket_unix, model, toUInt64(sum(tokens)) AS tokens \
         FROM ( \
           SELECT bucket_unix, model, toUInt64(max(output_tokens_latest)) AS tokens \
           FROM ( \
             SELECT \
               toUInt64(toUnixTimestamp(toStartOfInterval(event_ts_latest, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
               {model_expr_latest} AS model, \
               harness_latest, \
               session_id_latest, \
               request_id_latest, \
               output_tokens_latest \
             FROM ( \
               SELECT \
                 event_uid, \
                 argMax(event_ts, event_version) AS event_ts_latest, \
                 argMax(model, event_version) AS model_latest, \
                 argMax(harness, event_version) AS harness_latest, \
                 argMax(session_id, event_version) AS session_id_latest, \
                 argMax(request_id, event_version) AS request_id_latest, \
                 argMax(output_tokens, event_version) AS output_tokens_latest \
               FROM {table} \
               GROUP BY event_uid \
             ) WHERE {generation_latest_filter} \
           ) \
           WHERE harness_latest = 'claude-code' AND length(trim(BOTH ' ' FROM request_id_latest)) > 0 \
           GROUP BY bucket_unix, model, session_id_latest, request_id_latest \
           UNION ALL \
           SELECT \
             toUInt64(toUnixTimestamp(toStartOfInterval(event_ts_latest, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
             {model_expr_latest} AS model, \
             toUInt64(output_tokens_latest) AS tokens \
           FROM ( \
             SELECT \
               event_uid, \
               argMax(event_ts, event_version) AS event_ts_latest, \
               argMax(model, event_version) AS model_latest, \
               argMax(harness, event_version) AS harness_latest, \
               argMax(session_id, event_version) AS session_id_latest, \
               argMax(request_id, event_version) AS request_id_latest, \
               argMax(output_tokens, event_version) AS output_tokens_latest \
             FROM {table} \
             GROUP BY event_uid \
           ) WHERE {generation_latest_filter} \
           AND NOT (harness_latest = 'claude-code' AND length(trim(BOTH ' ' FROM request_id_latest)) > 0) \
         ) \
         GROUP BY bucket_unix, model \
         ORDER BY bucket_unix ASC, model ASC",
        bucket_seconds = range.bucket_seconds,
        model_expr_latest = model_expr_latest,
        table = table,
        generation_latest_filter = generation_latest_filter,
    );

    let turns_query = format!(
        "SELECT \
            toUInt64(toUnixTimestamp(toStartOfInterval(event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
            {model_expr} AS model, \
            toUInt64(uniqExact(tuple(session_id, request_id))) AS turns \
         FROM {table} \
         WHERE {turn_filter} \
         GROUP BY bucket_unix, model \
         ORDER BY bucket_unix ASC, model ASC",
        bucket_seconds = range.bucket_seconds,
        model_expr = model_expr,
        table = table,
        turn_filter = turn_filter,
    );

    let concurrent_query = format!(
        "SELECT bucket_unix, toUInt64(uniqExact(session_stream_key)) AS concurrent_sessions \
         FROM ( \
           SELECT \
             toUInt64(toUnixTimestamp(toStartOfInterval(event_ts, INTERVAL {bucket_seconds} SECOND))) AS bucket_unix, \
             if(harness = 'claude-code' AND length(trim(BOTH ' ' FROM agent_run_id)) > 0, concat(session_id, '::', agent_run_id), session_id) AS session_stream_key \
           FROM {table} \
           WHERE {concurrent_filter} \
         ) \
         GROUP BY bucket_unix \
         ORDER BY bucket_unix ASC",
        bucket_seconds = range.bucket_seconds,
        table = table,
        concurrent_filter = concurrent_filter,
    );

    let token_rows = match state
        .clickhouse
        .query_rows::<TokenRow>(&token_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics token query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let turn_rows = match state
        .clickhouse
        .query_rows::<TurnRow>(&turns_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics turns query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let concurrent_rows = match state
        .clickhouse
        .query_rows::<ConcurrentRow>(&concurrent_query, None)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("analytics concurrent-session query failed: {error}")}),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct WindowRow {
        now_unix: u64,
        from_unix: u64,
    }

    let window_query = analytics_window_query(&table, &window_filter, range.window_seconds);

    let (now_unix, from_unix) = match state
        .clickhouse
        .query_rows::<WindowRow>(&window_query, None)
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            let row = &rows[0];
            (row.now_unix, row.from_unix)
        }
        _ => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            (now, now.saturating_sub(range.window_seconds as u64))
        }
    };

    json_response(
        json!({
            "ok": true,
            "range": {
                "key": range.key,
                "label": range.label,
                "window_seconds": range.window_seconds,
                "bucket_seconds": range.bucket_seconds,
                "from_unix": from_unix,
                "to_unix": now_unix,
            },
            "series": {
                "tokens": token_rows,
                "turns": turn_rows,
                "concurrent_sessions": concurrent_rows,
            }
        }),
        StatusCode::OK,
    )
}

#[derive(Deserialize)]
struct SessionsQuery {
    limit: Option<u32>,
    since: Option<String>,
    cursor: Option<String>,
    query: Option<String>,
    model: Option<String>,
    status: Option<String>,
    harness: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionsCursor {
    since_seconds: u64,
    ended_at_ms: i64,
    session_id: String,
}

#[derive(Debug, Default, Clone)]
struct SessionsFilterInput {
    query: Option<String>,
    model: Option<String>,
    status: Option<String>,
    harness: Option<String>,
}

#[derive(Deserialize)]
struct SessionDetailQuery {
    turn_limit: Option<u32>,
    turn_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionDetailCursor {
    session_id: String,
    page_start: u64,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct SessionTurnAnchorRow {
    event_ts_ms: i64,
    source_file: String,
    source_generation: u32,
    source_offset: u64,
    source_line_no: u64,
}

#[derive(Debug, Clone)]
struct SessionDetailRenderMeta {
    requested_turn_limit: u32,
    loaded_turn_count: usize,
    total_turn_count: usize,
    has_more_turns: bool,
    has_previous_turns: bool,
    next_turn_cursor: Option<String>,
    truncated_reason: Option<String>,
}

fn encode_sessions_cursor(cursor: &SessionsCursor) -> Result<String> {
    let json = serde_json::to_vec(cursor)
        .map_err(|err| anyhow!("failed to serialize sessions cursor: {err}"))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_sessions_cursor(token: &str) -> Result<SessionsCursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|err| anyhow!("invalid cursor: invalid base64: {err}"))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| anyhow!("invalid cursor: invalid payload: {err}"))
}

fn encode_session_detail_cursor(cursor: &SessionDetailCursor) -> Result<String> {
    let json = serde_json::to_vec(cursor)
        .map_err(|err| anyhow!("failed to serialize session detail cursor: {err}"))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_session_detail_cursor(token: &str) -> Result<SessionDetailCursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|err| anyhow!("invalid turn cursor: invalid base64: {err}"))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| anyhow!("invalid turn cursor: invalid payload: {err}"))
}

fn normalize_filter_value(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("all") {
        return None;
    }
    Some(trimmed.to_string())
}

async fn api_sessions(
    Query(params): Query<SessionsQuery>,
    State(state): State<AppState>,
) -> Response {
    let limit = params.limit.unwrap_or(25).clamp(1, 200);
    let since = params.since.as_deref().unwrap_or("30d");

    let since_seconds: u64 = match since {
        "1h" => 60 * 60,
        "6h" => 6 * 60 * 60,
        "24h" => 24 * 60 * 60,
        "7d" => 7 * 24 * 60 * 60,
        "30d" => 30 * 24 * 60 * 60,
        "90d" => 90 * 24 * 60 * 60,
        "all" => 0,
        _ => 30 * 24 * 60 * 60,
    };

    let cursor = match params.cursor.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(token) => Some(token),
    };

    let filters = SessionsFilterInput {
        query: normalize_filter_value(params.query.as_deref()),
        model: normalize_filter_value(params.model.as_deref()).map(|value| value.to_ascii_lowercase()),
        status: normalize_filter_value(params.status.as_deref()).map(|value| value.to_ascii_lowercase()),
        harness: normalize_filter_value(params.harness.as_deref()),
    };

    match build_sessions_payload(&state, limit, since_seconds, cursor, &filters).await {
        Ok(payload) => json_response(payload, StatusCode::OK),
        Err(error) => {
            let message = format!("sessions query failed: {error}");
            let status = if message.contains("invalid cursor:") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(json!({"ok": false, "error": message}), status)
        }
    }
}

async fn api_session_detail(
    Path(session_id): Path<String>,
    Query(params): Query<SessionDetailQuery>,
    State(state): State<AppState>,
) -> Response {
    let turn_limit = params.turn_limit.unwrap_or(50).clamp(1, 200);
    let turn_cursor = match params.turn_cursor.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(token) => Some(token),
    };

    match build_session_detail_payload(&state, &session_id, turn_limit, turn_cursor).await {
        Ok(Some(payload)) => json_response(json!({"ok": true, "session": payload}), StatusCode::OK),
        Ok(None) => json_response(
            json!({"ok": false, "error": "session not found"}),
            StatusCode::NOT_FOUND,
        ),
        Err(error) => {
            let message = format!("session detail query failed: {error}");
            let status = if message.contains("invalid turn cursor:") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(json!({"ok": false, "error": message}), status)
        }
    }
}

async fn api_sources(State(state): State<AppState>) -> Response {
    match build_source_status_snapshot(&state.cfg, true).await {
        Ok(snapshot) => json_response(
            json!({"ok": true, "sources": snapshot.sources, "query_error": snapshot.query_error}),
            StatusCode::OK,
        ),
        Err(error) => json_response(
            json!({"ok": false, "error": error.to_string()}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    }
}

fn source_detail_payload(snapshot: moraine_source_status::SourceDetailSnapshot) -> Value {
    json!({
        "ok": true,
        "source": snapshot.source,
        "query_error": snapshot.query_error,
        "runtime": snapshot.runtime,
        "runtime_query_error": snapshot.runtime_query_error,
        "warnings": snapshot.warnings,
    })
}

#[derive(Deserialize)]
struct SourceErrorsQuery {
    limit: Option<u32>,
}

async fn api_source_detail(Path(source): Path<String>, State(state): State<AppState>) -> Response {
    match build_source_detail_snapshot(&state.cfg, &source).await {
        Ok(snapshot) => json_response(source_detail_payload(snapshot), StatusCode::OK),
        Err(error) => {
            let status = if error.to_string().contains("not found in config") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(json!({"ok": false, "error": error.to_string()}), status)
        }
    }
}

async fn api_source_files(Path(source): Path<String>, State(state): State<AppState>) -> Response {
    match build_source_files_snapshot(&state.cfg, &source).await {
        Ok(snapshot) => json_response(
            json!({
                "ok": true,
                "source_name": snapshot.source_name,
                "watch_root": snapshot.watch_root,
                "glob": snapshot.glob,
                "files": snapshot.files,
                "glob_match_count": snapshot.glob_match_count,
                "fs_error": snapshot.fs_error,
                "query_error": snapshot.query_error,
            }),
            StatusCode::OK,
        ),
        Err(error) => {
            let status = if error.to_string().contains("not found in config") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(json!({"ok": false, "error": error.to_string()}), status)
        }
    }
}

async fn api_source_errors(
    Path(source): Path<String>,
    Query(params): Query<SourceErrorsQuery>,
    State(state): State<AppState>,
) -> Response {
    let limit = params.limit.unwrap_or(50);
    match build_source_errors_snapshot(&state.cfg, &source, limit).await {
        Ok(snapshot) => json_response(
            json!({
                "ok": true,
                "source_name": snapshot.source_name,
                "errors": snapshot.errors,
                "query_error": snapshot.query_error,
            }),
            StatusCode::OK,
        ),
        Err(error) => {
            let status = if error.to_string().contains("not found in config") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json_response(json!({"ok": false, "error": error.to_string()}), status)
        }
    }
}

#[derive(serde::Deserialize)]
struct SessionSummaryRow {
    session_id: String,
    harness: String,
    source_name: String,
    started_at_ms: i64,
    ended_at_ms: i64,
    models_blob: String,
    trace_id: String,
    first_user_text: String,
    total_tokens: u64,
    total_tool_calls: u64,
    turn_count: u64,
}

#[derive(serde::Deserialize)]
struct SessionEventRow {
    event_ts_ms: i64,
    event_kind: String,
    actor_kind: String,
    payload_type: String,
    tool_name: String,
    tool_call_id: String,
    tool_error: u8,
    latency_ms: u32,
    model: String,
    input_tokens: u32,
    output_tokens: u32,
    cache_read_tokens: u32,
    cache_write_tokens: u32,
    text_preview: String,
    text_content: String,
    tool_args_json: String,
}

async fn build_sessions_payload(
    state: &AppState,
    limit: u32,
    since_seconds: u64,
    cursor_token: Option<&str>,
    filters: &SessionsFilterInput,
) -> Result<Value> {
    let database = &state.clickhouse.config().database;
    let events_table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let cursor = match cursor_token {
        Some(token) => {
            let cursor = decode_sessions_cursor(token)?;
            if cursor.since_seconds != since_seconds {
                return Err(anyhow!(
                    "invalid cursor: cursor does not match requested sessions window"
                ));
            }
            if cursor.session_id.trim().is_empty() {
                return Err(anyhow!("invalid cursor: missing session_id"));
            }
            Some(cursor)
        }
        None => None,
    };

    let recency_filter = if since_seconds == 0 {
        String::new()
    } else {
        format!(" AND event_ts >= now() - INTERVAL {} SECOND", since_seconds)
    };

    let limit_plus = limit.saturating_add(1);
    let summary_subquery = format!(
        "SELECT \
            session_id, \
            any(harness) AS harness, \
            any(source_name) AS source_name, \
            toInt64(toUnixTimestamp64Milli(min(event_ts))) AS started_at_ms, \
            toInt64(toUnixTimestamp64Milli(max(event_ts))) AS ended_at_ms, \
            toUInt64(sum(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens)) AS total_tokens, \
            toUInt64(countIf(event_kind = 'tool_call')) AS total_tool_calls, \
            toUInt64(countIf(event_kind = 'message' AND actor_kind = 'user')) AS turn_count, \
            arrayStringConcat( \
                arrayFilter(m -> length(m) > 0, \
                    groupUniqArray(lowerUTF8(trim(BOTH ' ' FROM model)))), \
                '\u{1f}' \
            ) AS models_blob, \
            any(if(length(trim(BOTH ' ' FROM trace_id)) > 0, trace_id, '')) AS trace_id, \
            argMin( \
                if(length(text_content) > 0, substring(text_content, 1, 400), substring(text_preview, 1, 400)), \
                if(event_kind = 'message' AND actor_kind = 'user', event_ts, toDateTime64('2099-01-01', 3)) \
            ) AS first_user_text \
         FROM {events_table} \
         WHERE length(trim(BOTH ' ' FROM session_id)) > 0{recency_filter} \
         GROUP BY session_id"
    );
    let mut outer_filters: Vec<String> = Vec::new();
    if let Some(query) = &filters.query {
        let escaped = escape_literal(query);
        outer_filters.push(format!(
            "(positionCaseInsensitiveUTF8(first_user_text, '{escaped}') > 0 \
              OR positionCaseInsensitiveUTF8(session_id, '{escaped}') > 0 \
              OR positionCaseInsensitiveUTF8(harness, '{escaped}') > 0 \
              OR positionCaseInsensitiveUTF8(trace_id, '{escaped}') > 0)"
        ));
    }
    if let Some(model) = &filters.model {
        let escaped = escape_literal(model);
        outer_filters.push(format!(
            "position(concat('\u{1f}', models_blob, '\u{1f}'), concat('\u{1f}', '{escaped}', '\u{1f}')) > 0"
        ));
    }
    if let Some(status) = &filters.status {
        match status.as_str() {
            "active" => outer_filters.push(format!("ended_at_ms >= {}", now_ms.saturating_sub(60_000))),
            "completed" => outer_filters.push(format!("ended_at_ms < {}", now_ms.saturating_sub(60_000))),
            "cancelled" | "error" => outer_filters.push("0".to_string()),
            _ => {}
        }
    }
    if let Some(harness) = &filters.harness {
        outer_filters.push(format!("harness = '{}'", escape_literal(harness)));
    }
    if let Some(cursor) = &cursor {
        outer_filters.push(format!(
            "((ended_at_ms < {ended_at_ms}) OR (ended_at_ms = {ended_at_ms} AND session_id < '{session_id}'))",
            ended_at_ms = cursor.ended_at_ms,
            session_id = escape_literal(&cursor.session_id),
        ));
    }
    let where_clause = if outer_filters.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", outer_filters.join(" AND "))
    };
    let summary_query = format!(
        "SELECT \
            session_id, \
            harness, \
            source_name, \
            started_at_ms, \
            ended_at_ms, \
            total_tokens, \
            total_tool_calls, \
            turn_count, \
            models_blob, \
            trace_id, \
            first_user_text \
         FROM ({summary_subquery}) AS summaries{where_clause} \
         ORDER BY ended_at_ms DESC, session_id DESC \
         LIMIT {limit_plus}"
    );

    let mut summary_rows = state
        .clickhouse
        .query_rows::<SessionSummaryRow>(&summary_query, None)
        .await?;

    let has_more = summary_rows.len() as u32 > limit;
    if has_more {
        summary_rows.truncate(limit as usize);
    }

    let next_cursor = if has_more {
        summary_rows
            .last()
            .map(|summary| {
                encode_sessions_cursor(&SessionsCursor {
                    since_seconds,
                    ended_at_ms: summary.ended_at_ms,
                    session_id: summary.session_id.clone(),
                })
            })
            .transpose()?
    } else {
        None
    };

    if summary_rows.is_empty() {
        return Ok(json!({
            "ok": true,
            "sessions": [],
            "meta": {
                "requested_limit": limit,
                "effective_limit": limit,
                "loaded_count": 0,
                "has_more": false,
                "since_seconds": since_seconds,
                "next_cursor": Value::Null,
            }
        }));
    }

    let sessions: Vec<Value> = summary_rows
        .into_iter()
        .map(|summary| build_session_summary_json(summary, now_ms))
        .collect();

    Ok(json!({
        "ok": true,
        "sessions": sessions,
        "filters": {
            "query": filters.query.clone(),
            "model": filters.model.clone(),
            "status": filters.status.clone(),
            "harness": filters.harness.clone(),
        },
        "meta": {
            "requested_limit": limit,
            "effective_limit": limit,
            "loaded_count": sessions.len(),
            "has_more": has_more,
            "since_seconds": since_seconds,
            "next_cursor": next_cursor,
        }
    }))
}

async fn build_session_detail_payload(
    state: &AppState,
    session_id: &str,
    turn_limit: u32,
    turn_cursor_token: Option<&str>,
) -> Result<Option<Value>> {
    let database = &state.clickhouse.config().database;
    let events_table = format!(
        "{}.{}",
        escape_identifier(database),
        escape_identifier("events")
    );
    let quoted_session_id = escape_literal(session_id);
    let summary_query = format!(
        "SELECT \
            session_id, \
            any(harness) AS harness, \
            any(source_name) AS source_name, \
            toInt64(toUnixTimestamp64Milli(min(event_ts))) AS started_at_ms, \
            toInt64(toUnixTimestamp64Milli(max(event_ts))) AS ended_at_ms, \
            toUInt64(sum(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens)) AS total_tokens, \
            toUInt64(countIf(event_kind = 'tool_call')) AS total_tool_calls, \
            toUInt64(countIf(event_kind = 'message' AND actor_kind = 'user')) AS turn_count, \
            arrayStringConcat( \
                arrayFilter(m -> length(m) > 0, \
                    groupUniqArray(lowerUTF8(trim(BOTH ' ' FROM model)))), \
                '\u{1f}' \
            ) AS models_blob, \
            any(if(length(trim(BOTH ' ' FROM trace_id)) > 0, trace_id, '')) AS trace_id, \
            argMin( \
                if(length(text_content) > 0, substring(text_content, 1, 400), substring(text_preview, 1, 400)), \
                if(event_kind = 'message' AND actor_kind = 'user', event_ts, toDateTime64('2099-01-01', 3)) \
            ) AS first_user_text \
         FROM {events_table} \
         WHERE session_id = '{quoted_session_id}' \
         GROUP BY session_id"
    );

    let mut summary_rows = state
        .clickhouse
        .query_rows::<SessionSummaryRow>(&summary_query, None)
        .await?;

    let Some(mut summary) = summary_rows.pop() else {
        return Ok(None);
    };

    let anchors_query = format!(
        "SELECT \
            toInt64(toUnixTimestamp64Milli(event_ts)) AS event_ts_ms, \
            source_file, \
            toUInt32(source_generation) AS source_generation, \
            toUInt64(source_offset) AS source_offset, \
            toUInt64(source_line_no) AS source_line_no \
         FROM {events_table} \
         WHERE session_id = '{quoted_session_id}' \
           AND event_kind = 'message' \
           AND actor_kind = 'user' \
         ORDER BY event_ts, source_file, source_generation, source_offset, source_line_no"
    );
    let anchors = state
        .clickhouse
        .query_rows::<SessionTurnAnchorRow>(&anchors_query, None)
        .await?;
    let total_turn_count = anchors.len();
    summary.turn_count = total_turn_count as u64;
    let default_start = total_turn_count.saturating_sub(turn_limit as usize);
    let page_start = match turn_cursor_token {
        Some(token) => {
            let cursor = decode_session_detail_cursor(token)?;
            if cursor.session_id != session_id {
                return Err(anyhow!("invalid turn cursor: session mismatch"));
            }
            let requested_start = usize::try_from(cursor.page_start)
                .map_err(|_| anyhow!("invalid turn cursor: page_start overflow"))?;
            if total_turn_count == 0 {
                0
            } else if requested_start >= total_turn_count {
                return Err(anyhow!("invalid turn cursor: page_start out of range"));
            } else {
                requested_start
            }
        }
        None => default_start,
    };
    let page_end = (page_start + turn_limit as usize).min(total_turn_count);
    let has_older_turns = page_start > 0;
    let has_newer_turns = page_end < total_turn_count;
    let next_turn_cursor = if has_older_turns {
        Some(encode_session_detail_cursor(&SessionDetailCursor {
            session_id: session_id.to_string(),
            page_start: page_start.saturating_sub(turn_limit as usize) as u64,
        })?)
    } else {
        None
    };
    let truncated_reason = if total_turn_count > (page_end.saturating_sub(page_start)) {
        Some(format!("detail paginated to {} turns per page", turn_limit))
    } else {
        None
    };

    let event_rows = if total_turn_count == 0 {
        Vec::new()
    } else {
        let start_anchor = &anchors[page_start];
        let end_anchor = if page_end < total_turn_count {
            Some(&anchors[page_end])
        } else {
            None
        };
        let position_expr = event_position_expr();
        let mut predicates = vec![format!(
            "{position_expr} >= {}",
            event_position_tuple_sql(start_anchor)
        )];
        if let Some(end_anchor) = end_anchor {
            predicates.push(format!(
                "{position_expr} < {}",
                event_position_tuple_sql(end_anchor)
            ));
        }

        let events_query = format!(
            "SELECT \
                toInt64(toUnixTimestamp64Milli(event_ts)) AS event_ts_ms, \
                event_kind, \
                actor_kind, \
                payload_type, \
                tool_name, \
                tool_call_id, \
                tool_error, \
                latency_ms, \
                model, \
                input_tokens, \
                output_tokens, \
                cache_read_tokens, \
                cache_write_tokens, \
                substring(text_preview, 1, 2000) AS text_preview, \
                substring(text_content, 1, 2000) AS text_content, \
                if(event_kind = 'tool_call', substring(JSONExtractRaw(payload_json, 'input'), 1, 2000), '') AS tool_args_json \
             FROM {events_table} \
             WHERE session_id = '{quoted_session_id}' \
               AND {} \
             ORDER BY session_id, event_ts, source_file, source_generation, source_offset, source_line_no",
            predicates.join(" AND ")
        );

        state
            .clickhouse
            .query_rows::<SessionEventRow>(&events_query, None)
            .await?
    };

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    Ok(Some(build_session_json(
        summary,
        event_rows,
        now_ms,
        Some(SessionDetailRenderMeta {
            requested_turn_limit: turn_limit,
            loaded_turn_count: page_end.saturating_sub(page_start),
            total_turn_count,
            has_more_turns: has_older_turns,
            has_previous_turns: has_newer_turns,
            next_turn_cursor,
            truncated_reason,
        }),
    )))
}

fn build_session_summary_json(summary: SessionSummaryRow, now_ms: i64) -> Value {
    let duration_ms = (summary.ended_at_ms - summary.started_at_ms).max(0);
    let status = if now_ms - summary.ended_at_ms < 60_000 {
        "active"
    } else {
        "completed"
    };

    let models: Vec<String> = if summary.models_blob.is_empty() {
        Vec::new()
    } else {
        summary
            .models_blob
            .split('\u{1f}')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    };

    let harness = harness_descriptor(&summary.harness, &summary.source_name);
    let title = derive_title(&summary.first_user_text);
    let preview_text = summary.first_user_text.trim().to_string();

    json!({
        "id": summary.session_id,
        "title": title,
        "previewText": preview_text,
        "harness": harness,
        "startedAt": summary.started_at_ms,
        "endedAt": summary.ended_at_ms,
        "durationMs": duration_ms,
        "status": status,
        "models": models,
        "turnCount": summary.turn_count,
        "turns": [],
        "totalTokens": summary.total_tokens,
        "totalToolCalls": summary.total_tool_calls,
        "tags": Vec::<String>::new(),
        "traceId": summary.trace_id,
        "hasDetail": false,
    })
}

fn build_session_json(
    summary: SessionSummaryRow,
    events: Vec<SessionEventRow>,
    now_ms: i64,
    detail_meta: Option<SessionDetailRenderMeta>,
) -> Value {
    let duration_ms = (summary.ended_at_ms - summary.started_at_ms).max(0);

    let status = if now_ms - summary.ended_at_ms < 60_000 {
        "active"
    } else {
        "completed"
    };

    let models: Vec<String> = if summary.models_blob.is_empty() {
        Vec::new()
    } else {
        summary
            .models_blob
            .split('\u{1f}')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    };

    let title = derive_title(&summary.first_user_text);

    let mut turns = build_turns(&events);
    let detail_meta = detail_meta.map(|mut meta| {
        if turns.len() > meta.loaded_turn_count {
            let keep_from = turns.len().saturating_sub(meta.loaded_turn_count);
            turns = turns.split_off(keep_from);
        }
        meta.loaded_turn_count = turns.len();
        json!({
            "requestedTurnLimit": meta.requested_turn_limit,
            "loadedTurnCount": meta.loaded_turn_count,
            "totalTurnCount": meta.total_turn_count,
            "hasMoreTurns": meta.has_more_turns,
            "hasPreviousTurns": meta.has_previous_turns,
            "nextTurnCursor": meta.next_turn_cursor,
            "truncatedReason": meta.truncated_reason,
        })
    });

    let harness = harness_descriptor(&summary.harness, &summary.source_name);

    json!({
        "id": summary.session_id,
        "title": title,
        "previewText": summary.first_user_text.trim(),
        "harness": harness,
        "startedAt": summary.started_at_ms,
        "endedAt": summary.ended_at_ms,
        "durationMs": duration_ms,
        "status": status,
        "models": models,
        "turnCount": summary.turn_count,
        "turns": turns,
        "totalTokens": summary.total_tokens,
        "totalToolCalls": summary.total_tool_calls,
        "tags": Vec::<String>::new(),
        "traceId": summary.trace_id,
        "hasDetail": true,
        "detailMeta": detail_meta,
    })
}

fn event_position_expr() -> &'static str {
    "(toInt64(toUnixTimestamp64Milli(event_ts)), source_file, toUInt32(source_generation), toUInt64(source_offset), toUInt64(source_line_no))"
}

fn event_position_tuple_sql(anchor: &SessionTurnAnchorRow) -> String {
    format!(
        "({}, '{}', toUInt32({}), toUInt64({}), toUInt64({}))",
        anchor.event_ts_ms,
        escape_literal(&anchor.source_file),
        anchor.source_generation,
        anchor.source_offset,
        anchor.source_line_no,
    )
}

fn derive_title(first_user_text: &str) -> String {
    let trimmed = first_user_text.trim();
    if trimmed.is_empty() {
        return "(untitled session)".to_string();
    }

    let first_line = trimmed.lines().next().unwrap_or(trimmed);
    let clean = first_line.trim();
    let max_chars = 120;
    let char_count = clean.chars().count();
    if char_count <= max_chars {
        clean.to_string()
    } else {
        let truncated: String = clean.chars().take(max_chars).collect();
        format!("{truncated}\u{2026}")
    }
}

fn harness_descriptor(harness_id: &str, source_name: &str) -> Value {
    let id = if harness_id.trim().is_empty() {
        source_name.trim()
    } else {
        harness_id.trim()
    };
    let id = if id.is_empty() { "unknown" } else { id };
    let label = id.to_string();
    let short: String = id
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .take(2)
        .filter_map(|part| part.chars().next())
        .collect::<String>()
        .to_uppercase();
    let short = if short.is_empty() {
        id.chars().take(2).collect::<String>().to_uppercase()
    } else {
        short
    };

    let hue = hue_for_label(id);

    json!({
        "id": id,
        "label": label,
        "short": short,
        "hue": hue,
    })
}

fn hue_for_label(label: &str) -> u32 {
    match label {
        "claude-code" => 25,
        "codex" => 150,
        "hermes" => 265,
        "cursor" => 200,
        "aider" => 340,
        "continue" => 100,
        "cli" => 60,
        _ => {
            let mut hash: u32 = 0;
            for byte in label.bytes() {
                hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
            }
            hash % 360
        }
    }
}

fn build_turns(events: &[SessionEventRow]) -> Vec<Value> {
    struct TurnBuilder {
        idx: u32,
        model: String,
        started_at: i64,
        ended_at: i64,
        steps: Vec<Value>,
        prompt_tokens: u64,
        completion_tokens: u64,
        tool_calls: u64,
    }

    if events.is_empty() {
        return Vec::new();
    }

    let mut turns: Vec<TurnBuilder> = Vec::new();
    let mut current: Option<TurnBuilder> = None;
    let mut open_tool_calls: HashMap<String, usize> = HashMap::new();

    for event in events {
        let starts_new_turn = event.event_kind == "message" && event.actor_kind == "user";

        if starts_new_turn {
            if let Some(finished) = current.take() {
                turns.push(finished);
            }
            current = Some(TurnBuilder {
                idx: turns.len() as u32,
                model: String::new(),
                started_at: event.event_ts_ms,
                ended_at: event.event_ts_ms,
                steps: Vec::new(),
                prompt_tokens: 0,
                completion_tokens: 0,
                tool_calls: 0,
            });
            open_tool_calls.clear();
        }

        let turn = match current.as_mut() {
            Some(t) => t,
            None => continue,
        };

        turn.ended_at = turn.ended_at.max(event.event_ts_ms);
        turn.prompt_tokens += (event.input_tokens + event.cache_read_tokens) as u64;
        turn.completion_tokens += (event.output_tokens + event.cache_write_tokens) as u64;
        if !event.model.trim().is_empty() && turn.model.is_empty() {
            turn.model = event.model.trim().to_string();
        }

        match (
            event.event_kind.as_str(),
            event.actor_kind.as_str(),
            event.payload_type.as_str(),
        ) {
            ("message", "user", _) => {
                turn.steps.push(json!({
                    "kind": "user",
                    "at": event.event_ts_ms,
                    "text": preferred_text(event),
                }));
            }
            ("message", "assistant", _) | (_, "assistant", "text") => {
                turn.steps.push(json!({
                    "kind": "assistant",
                    "at": event.event_ts_ms,
                    "text": preferred_text(event),
                    "tokens": event.output_tokens,
                }));
            }
            ("reasoning", _, _) | (_, _, "thinking") => {
                let text = preferred_text(event);
                if !text.is_empty() {
                    turn.steps.push(json!({
                        "kind": "thinking",
                        "at": event.event_ts_ms,
                        "text": text,
                    }));
                }
            }
            ("tool_call", _, _) | (_, _, "tool_use") => {
                turn.tool_calls += 1;
                let step_index = turn.steps.len();
                if !event.tool_call_id.is_empty() {
                    open_tool_calls.insert(event.tool_call_id.clone(), step_index);
                }
                let args = if event.tool_args_json.trim().is_empty() {
                    Value::Object(Default::default())
                } else {
                    serde_json::from_str::<Value>(&event.tool_args_json)
                        .unwrap_or_else(|_| Value::Object(Default::default()))
                };
                turn.steps.push(json!({
                    "kind": "tool_call",
                    "at": event.event_ts_ms,
                    "tool": if event.tool_name.is_empty() { "tool".to_string() } else { event.tool_name.clone() },
                    "args": args,
                    "latencyMs": event.latency_ms,
                    "result": "",
                    "resultAt": event.event_ts_ms,
                    "status": if event.tool_error != 0 { "error" } else { "ok" },
                    "callId": event.tool_call_id,
                }));
            }
            ("tool_result", _, _) | (_, "tool", "tool_result") => {
                let result_text = preferred_text(event);
                if let Some(&step_index) = open_tool_calls.get(&event.tool_call_id) {
                    if let Some(step) = turn.steps.get_mut(step_index) {
                        if let Some(obj) = step.as_object_mut() {
                            let call_at = obj
                                .get("at")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(event.event_ts_ms);
                            obj.insert("resultAt".into(), json!(event.event_ts_ms));
                            obj.insert(
                                "latencyMs".into(),
                                json!((event.event_ts_ms - call_at).max(0) as u32),
                            );
                            obj.insert("result".into(), json!(result_text));
                            if event.tool_error != 0 {
                                obj.insert("status".into(), json!("error"));
                            }
                        }
                    }
                    open_tool_calls.remove(&event.tool_call_id);
                }
            }
            _ => {}
        }
    }

    if let Some(finished) = current.take() {
        turns.push(finished);
    }

    turns
        .into_iter()
        .map(|builder| {
            let duration = (builder.ended_at - builder.started_at).max(0);
            let total = builder.prompt_tokens + builder.completion_tokens;
            json!({
                "idx": builder.idx,
                "model": builder.model,
                "startedAt": builder.started_at,
                "endedAt": builder.ended_at,
                "durationMs": duration,
                "promptTokens": builder.prompt_tokens,
                "completionTokens": builder.completion_tokens,
                "totalTokens": total,
                "toolCalls": builder.tool_calls,
                "steps": builder.steps,
            })
        })
        .collect()
}

fn preferred_text(event: &SessionEventRow) -> String {
    if !event.text_content.trim().is_empty() {
        event.text_content.trim().to_string()
    } else {
        event.text_preview.trim().to_string()
    }
}

async fn query_table_summaries(state: &AppState) -> Result<Vec<TableSummary>> {
    #[derive(serde::Deserialize)]
    struct TableRow {
        name: String,
        engine: String,
        is_temporary: u8,
    }

    #[derive(serde::Deserialize)]
    struct PartsRow {
        table: String,
        rows: u64,
    }

    let database = &state.clickhouse.config().database;
    let tables = state
        .clickhouse
        .query_rows::<TableRow>(&format!(
            "SELECT name, engine, is_temporary FROM system.tables WHERE database = '{}' ORDER BY name",
            escape_literal(database)
        ), None)
        .await?;

    let parts = state
        .clickhouse
        .query_rows::<PartsRow>(&format!(
            "SELECT table, SUM(rows) AS rows FROM system.parts WHERE database = '{}' AND active GROUP BY table",
            escape_literal(database)
        ), None)
        .await
        .unwrap_or_default();

    let counts: HashMap<String, u64> = parts.into_iter().map(|row| (row.table, row.rows)).collect();

    let values = tables
        .into_iter()
        .map(|row| TableSummary {
            name: row.name.clone(),
            engine: row.engine,
            is_temporary: row.is_temporary,
            rows: counts.get(&row.name).copied().unwrap_or(0),
        })
        .collect();

    Ok(values)
}

async fn query_heartbeat(state: &AppState) -> Result<Value> {
    let database = &state.clickhouse.config().database;
    let present = state
        .clickhouse
        .query_rows::<Value>(
            &format!(
            "SELECT name FROM system.tables WHERE database = '{}' AND name = 'ingest_heartbeats'",
            escape_literal(database)
        ),
            None,
        )
        .await?;

    if present.is_empty() {
        return Ok(json!({
            "present": false,
            "alive": false,
            "latest": Value::Null,
            "age_seconds": Value::Null,
        }));
    }

    let query = format!(
        "SELECT ts, toUnixTimestamp64Milli(ts) AS ts_unix_ms, host, service_version, queue_depth, files_active, files_watched, rows_raw_written, rows_events_written, rows_errors_written, flush_latency_ms, append_to_visible_p50_ms, append_to_visible_p95_ms, last_error FROM {}.ingest_heartbeats ORDER BY ts DESC LIMIT 1",
        escape_identifier(database)
    );

    let latest = state
        .clickhouse
        .query_rows::<Value>(&query, None)
        .await?
        .into_iter()
        .next();

    let Some(latest) = latest else {
        return Ok(
            json!({"present": false, "alive": false, "latest": Value::Null, "age_seconds": Value::Null}),
        );
    };

    let age_seconds = latest
        .get("ts_unix_ms")
        .and_then(value_to_i64)
        .and_then(|ts_ms| {
            if ts_ms < 0 {
                return None;
            }

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_millis() as i64;
            if now_ms < ts_ms {
                return Some(0);
            }

            Some(((now_ms - ts_ms) / 1000) as u64)
        });

    Ok(json!({
        "present": true,
        "latest": latest,
        "age_seconds": age_seconds,
        "alive": age_seconds.map(|age| age <= 30).unwrap_or(false),
    }))
}

async fn query_clickhouse_connections(state: &AppState) -> Result<Value> {
    #[derive(serde::Deserialize)]
    struct MetricRow {
        metric: String,
        value: u64,
    }

    let metrics = state
        .clickhouse
        .query_rows::<MetricRow>(
            "SELECT metric, value FROM system.metrics WHERE metric IN ('TCPConnection','HTTPConnection','MySQLConnection','PostgreSQLConnection','InterserverConnection')",
            None,
        )
        .await?;

    let mut tcp = 0_u64;
    let mut http = 0_u64;
    let mut mysql = 0_u64;
    let mut postgres = 0_u64;
    let mut interserver = 0_u64;

    for row in metrics {
        match row.metric.as_str() {
            "TCPConnection" => tcp = tcp.saturating_add(row.value),
            "HTTPConnection" => http = http.saturating_add(row.value),
            "MySQLConnection" => mysql = mysql.saturating_add(row.value),
            "PostgreSQLConnection" => postgres = postgres.saturating_add(row.value),
            "InterserverConnection" => interserver = interserver.saturating_add(row.value),
            _ => {}
        }
    }

    let total = tcp
        .saturating_add(http)
        .saturating_add(mysql)
        .saturating_add(postgres)
        .saturating_add(interserver);

    Ok(json!({
        "total": total,
        "tcp": tcp,
        "http": http,
        "mysql": mysql,
        "postgres": postgres,
        "interserver": interserver,
    }))
}

async fn api_table_rows(
    Path(table): Path<String>,
    Query(params): Query<LimitQuery>,
    State(state): State<AppState>,
) -> Response {
    if !is_safe_identifier(&table) {
        return json_response(
            json!({"ok": false, "error": "invalid table name"}),
            StatusCode::BAD_REQUEST,
        );
    }

    let limit = params.limit.unwrap_or(25).clamp(1, 500);
    let database = &state.clickhouse.config().database;

    #[derive(serde::Deserialize)]
    struct SchemaRow {
        name: String,
        #[serde(rename = "type")]
        type_name: String,
        default_expression: Option<String>,
    }

    let schema = match state
        .clickhouse
        .query_rows::<SchemaRow>(&format!(
            "SELECT name, type, default_expression FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            escape_literal(database),
            escape_literal(&table)
        ), None)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({
                    "ok": false,
                    "error": format!("unable to read schema for table {table}: {error}"),
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let preview_columns: Vec<String> = schema.iter().map(|entry| entry.name.clone()).collect();
    let rows_query = table_preview_rows_query(database, &table, &preview_columns, limit);

    let rows = match state
        .clickhouse
        .query_rows::<Value>(&rows_query, None)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({
                    "ok": false,
                    "error": format!("unable to read table {table}: {error}"),
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };

    let schema_payload: Vec<Value> = schema
        .into_iter()
        .map(|entry| {
            json!({
                "name": entry.name,
                "type": entry.type_name,
                "default_expression": entry.default_expression.unwrap_or_default(),
            })
        })
        .collect();

    json_response(
        json!({
            "ok": true,
            "table": table,
            "limit": limit,
            "schema": schema_payload,
            "rows": rows,
        }),
        StatusCode::OK,
    )
}

async fn static_fallback(State(state): State<AppState>, uri: Uri) -> Response {
    let requested = uri.path();
    if requested.contains("..") {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let file_path = if requested == "/" || requested.is_empty() {
        state.static_dir.join("index.html")
    } else {
        let mut target = state.static_dir.join(requested.trim_start_matches('/'));
        if target.is_dir() {
            target.push("index.html");
        }
        target
    };

    let canonical_root = match fs::canonicalize(&state.static_dir).await {
        Ok(path) => path,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("static directory unavailable: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let canonical_file = match fs::canonicalize(&file_path).await {
        Ok(path) => path,
        Err(_) => {
            return json_response(
                json!({"ok": false, "error": "not found"}),
                StatusCode::NOT_FOUND,
            );
        }
    };

    if !canonical_file.starts_with(&canonical_root) {
        return json_response(
            json!({"ok": false, "error": "forbidden"}),
            StatusCode::FORBIDDEN,
        );
    }

    let bytes = match fs::read(&canonical_file).await {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                json!({"ok": false, "error": format!("failed to read file: {error}")}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let content_type = mime_guess::from_path(&canonical_file)
        .first_or_octet_stream()
        .essence_str()
        .to_string();

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return false,
    }

    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn table_preview_rows_query(
    database: &str,
    table: &str,
    column_names: &[String],
    limit: u32,
) -> String {
    let database = escape_identifier(database);
    let table = escape_identifier(table);

    if column_names.is_empty() {
        return format!("SELECT * FROM {database}.{table} LIMIT {limit}");
    }

    let order_by = column_names
        .iter()
        .map(|name| escape_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");

    format!("SELECT * FROM {database}.{table} ORDER BY {order_by} LIMIT {limit}")
}

fn value_to_i64(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(n) = value.as_u64() {
        return i64::try_from(n).ok();
    }
    value.as_str().and_then(|raw| raw.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use moraine_source_status::{
        SourceDetailSnapshot, SourceDetailWarning, SourceHealthStatus, SourceLagIndicator,
        SourceRuntimeSnapshot, SourceStatusRow, SourceWarningKind, SourceWarningSeverity,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(suffix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "moraine-monitor-core-{suffix}-{}-{stamp}",
            std::process::id()
        ))
    }

    #[test]
    fn identifier_safety_helper() {
        assert!(is_safe_identifier("events"));
        assert!(is_safe_identifier("_tmp_1"));
        assert!(!is_safe_identifier("1events"));
        assert!(!is_safe_identifier("events;drop"));
    }

    #[test]
    fn escaping_helpers() {
        assert_eq!(escape_literal("a'b"), "a''b");
        assert_eq!(escape_identifier("ev`ents"), "`ev``ents`");
    }

    #[test]
    fn table_preview_query_orders_by_schema_columns() {
        let query = table_preview_rows_query(
            "analytics",
            "events",
            &[
                "event_ts".to_string(),
                "session_id".to_string(),
                "event_id".to_string(),
            ],
            25,
        );
        assert_eq!(
            query,
            "SELECT * FROM `analytics`.`events` ORDER BY `event_ts`, `session_id`, `event_id` LIMIT 25"
        );
    }

    #[test]
    fn table_preview_query_escapes_identifiers() {
        let query = table_preview_rows_query("ana`lytics", "ev`ents", &["co`l".to_string()], 10);
        assert_eq!(
            query,
            "SELECT * FROM `ana``lytics`.`ev``ents` ORDER BY `co``l` LIMIT 10"
        );
    }

    #[test]
    fn table_preview_query_handles_empty_schema() {
        let columns: Vec<String> = Vec::new();
        let query = table_preview_rows_query("analytics", "events", &columns, 5);
        assert_eq!(query, "SELECT * FROM `analytics`.`events` LIMIT 5");
    }

    #[test]
    fn analytics_window_query_guards_empty_recent_window() {
        let query = analytics_window_query(
            "`moraine`.`events`",
            "event_ts >= now() - INTERVAL 86400 SECOND",
            86_400,
        );

        assert!(query.contains("count() = 0"));
        assert!(query.contains("toUnixTimestamp64Milli(max(event_ts))"));
        assert!(query.contains("greatest(anchor_unix - toInt64(86400), toInt64(0))"));
        assert!(!query.contains("max(event_ts) - INTERVAL"));
    }

    #[test]
    fn clickhouse_ping_payload_marks_healthy_when_ping_succeeds() {
        let payload = clickhouse_ping_payload("24.8".to_string(), Ok(()), 3.5);
        assert_eq!(payload["healthy"], json!(true));
        assert_eq!(payload["version"], json!("24.8"));
        assert_eq!(payload["ping_ms"], json!(3.5));
        assert_eq!(payload["error"], Value::Null);
    }

    #[test]
    fn clickhouse_ping_payload_marks_unhealthy_when_ping_fails() {
        let payload =
            clickhouse_ping_payload("24.8".to_string(), Err(anyhow!("ping failed")), 8.25);
        assert_eq!(payload["healthy"], json!(false));
        assert_eq!(payload["version"], json!("24.8"));
        assert_eq!(payload["ping_ms"], json!(8.25));
        assert_eq!(payload["error"], json!("ping failed"));
    }

    #[test]
    fn source_detail_payload_serializes_summary_and_partial_error() {
        let payload = source_detail_payload(SourceDetailSnapshot {
            source: SourceStatusRow {
                name: "opencode".to_string(),
                harness: "opencode".to_string(),
                format: "opencode_sqlite".to_string(),
                enabled: true,
                glob: "/tmp/opencode.db".to_string(),
                watch_root: "/tmp".to_string(),
                status: SourceHealthStatus::Warning,
                checkpoint_count: 3,
                latest_checkpoint_at: Some("2026-04-20 10:15:00".to_string()),
                latest_checkpoint_age_seconds: Some(15),
                raw_event_count: 42,
                ingest_error_count: 1,
                latest_error_at: Some("2026-04-20 10:20:00".to_string()),
                latest_error_kind: Some("schema_drift".to_string()),
                latest_error_text: Some("missing field".to_string()),
            },
            query_error: Some("checkpoint query failed: timeout".to_string()),
            runtime: SourceRuntimeSnapshot {
                latest_heartbeat_at: Some("2026-04-20 10:20:30".to_string()),
                latest_heartbeat_age_seconds: Some(6),
                queue_depth: Some(0),
                files_active: Some(1),
                files_watched: Some(4),
                append_to_visible_p50_ms: Some(25),
                append_to_visible_p95_ms: Some(130),
                watcher_backend: Some("native".to_string()),
                watcher_error_count: Some(0),
                watcher_reset_count: Some(0),
                watcher_last_reset_at: None,
                heartbeat_cadence_seconds: 5.0,
                reconcile_cadence_seconds: 30.0,
                lag_indicator: Some(SourceLagIndicator::Healthy),
            },
            runtime_query_error: Some("heartbeat query failed: timeout".to_string()),
            warnings: vec![SourceDetailWarning {
                kind: SourceWarningKind::FileState,
                severity: SourceWarningSeverity::Warning,
                summary: "This source is ingesting data, but recent file processing also recorded ingest errors.".to_string(),
            }],
        });

        assert_eq!(payload["ok"], json!(true));
        assert_eq!(payload["source"]["name"], json!("opencode"));
        assert_eq!(payload["source"]["status"], json!("warning"));
        assert_eq!(payload["source"]["watch_root"], json!("/tmp"));
        assert_eq!(payload["source"]["checkpoint_count"], json!(3));
        assert_eq!(
            payload["source"]["latest_checkpoint_age_seconds"],
            json!(15)
        );
        assert_eq!(
            payload["source"]["latest_error_kind"],
            json!("schema_drift")
        );
        assert_eq!(payload["runtime"]["watcher_backend"], json!("native"));
        assert_eq!(payload["runtime"]["lag_indicator"], json!("healthy"));
        assert_eq!(
            payload["runtime_query_error"],
            json!("heartbeat query failed: timeout")
        );
        assert_eq!(payload["warnings"][0]["kind"], json!("file_state"));
        assert_eq!(
            payload["query_error"],
            json!("checkpoint query failed: timeout")
        );
    }

    #[test]
    fn validate_static_dir_accepts_built_directory() {
        let root = temp_path("static-valid");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("index.html"), "<!doctype html>").expect("write index");

        validate_static_dir(&root).expect("valid static dir");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_static_dir_rejects_missing_directory() {
        let missing = temp_path("static-missing");
        let err = validate_static_dir(&missing).expect_err("missing static dir should fail");
        assert!(err.to_string().contains("is unavailable"));
    }

    #[test]
    fn validate_static_dir_rejects_non_directory() {
        let root = temp_path("static-file");
        fs::create_dir_all(&root).expect("create root");
        let path = root.join("dist");
        fs::write(&path, "not a dir").expect("write file");

        let err = validate_static_dir(&path).expect_err("file should fail");
        assert!(err.to_string().contains("is not a directory"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_static_dir_requires_index_html() {
        let root = temp_path("static-no-index");
        fs::create_dir_all(&root).expect("create root");

        let err = validate_static_dir(&root).expect_err("missing index should fail");
        assert!(err.to_string().contains("does not contain `index.html`"));

        let _ = fs::remove_dir_all(root);
    }
}
