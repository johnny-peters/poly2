use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::stream::{self, StreamExt};
use reqwest::Url;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::config::ExecutionSettings;

#[derive(Clone, Debug)]
pub struct BtcCandleMarket {
    pub market_id: String,
    pub market_slug: String,
    /// Map "Up" outcome token -> StrategySignal.yes_token_id
    pub up_token_id: String,
    /// Map "Down" outcome token -> StrategySignal.no_token_id
    pub down_token_id: String,
}

#[derive(Clone, Debug)]
pub struct CheckItem {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Default)]
pub struct HealthcheckReport {
    pub checks: Vec<CheckItem>,
}

impl HealthcheckReport {
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.ok)
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcResp {
    result: Option<String>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ArbCandidate {
    /// Stable id for dedup and position key (condition_id or API id preferred over slug/question).
    pub market_id: String,
    pub question: String,
    pub market_slug: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub bid_a: Decimal,
    pub price_a: Decimal,
    pub bid_b: Decimal,
    pub price_b: Decimal,
    pub volume_24h: Option<Decimal>,
    pub sum: Decimal,
    pub edge: Decimal,
}

pub async fn run_healthcheck(execution: &ExecutionSettings, dotenv_path: &Path) -> HealthcheckReport {
    let mut report = HealthcheckReport::default();
    let env_map = load_dotenv_map(dotenv_path).unwrap_or_default();
    let rpc_url = env_map
        .get("RPC_URL")
        .cloned()
        .unwrap_or_else(|| "https://poly.api.pocket.network".to_string());
    let usdc = env_map
        .get("USDC_CONTRACT_ADDRESS")
        .cloned()
        .unwrap_or_else(|| "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string());

    push_result(
        &mut report,
        "clob_http_markets",
        check_clob_http(execution).await,
    );
    push_result(&mut report, "clob_ws_tcp443", check_ws_tcp(execution));
    push_result(&mut report, "polygon_chain_id", check_chain_id(&rpc_url).await);
    push_result(
        &mut report,
        "polygon_latest_block",
        check_latest_block(&rpc_url).await,
    );
    push_result(
        &mut report,
        "polygon_usdc_code",
        check_contract_code(&rpc_url, &usdc).await,
    );

    report
}

pub async fn scan_arb_candidates(
    execution: &ExecutionSettings,
    top_n: usize,
    max_sum: Decimal,
    min_edge: Decimal,
    settle_guard_minutes: u64,
) -> Result<Vec<ArbCandidate>> {
    const LATEST_WINDOW: usize = 200;
    const PRICE_SCAN_CONCURRENCY: usize = 24;
    let clob_base = "https://clob.polymarket.com";
    let request_timeout = Duration::from_secs(execution.timeout_secs.max(1));
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(request_timeout)
        .build()
        .context("failed to build http client")?;
    let base = scan_markets_base_url(execution);
    let url = format!(
        "{}/markets?limit=1000&closed=false&archived=false",
        base.trim_end_matches('/')
    );
    let resp = http.get(&url).send().await.context("markets request failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!("markets request failed with status {}", resp.status()));
    }
    let root: Value = resp.json().await.context("failed to parse markets json")?;
    let data = if let Some(arr) = root.as_array() {
        arr
    } else {
        root.get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array in markets response"))?
    };

    let now_ms = Utc::now().timestamp_millis();
    let min_end_time_ms = now_ms + (settle_guard_minutes as i64) * 60 * 1000;
    let mut active_open = Vec::new();
    let mut filtered_non_tradeable = 0usize;
    let mut filtered_near_settlement = 0usize;
    for m in data {
        if !is_market_tradeable(m) {
            filtered_non_tradeable += 1;
            continue;
        }
        if settle_guard_minutes > 0
            && market_end_time_ms(m).is_some_and(|end_ms| end_ms <= min_end_time_ms)
        {
            filtered_near_settlement += 1;
            continue;
        }
        active_open.push(m);
    }

    active_open.sort_by_key(|m| std::cmp::Reverse(extract_created_at_ms(m)));
    let window = &active_open[..active_open.len().min(LATEST_WINDOW)];

    let mut out = Vec::new();
    let active_open_count = active_open.len();
    let mut token_ids_ok_count = 0usize;
    let mut clob_price_ok_count = 0usize;
    let mut under_sum_count = 0usize;
    let mut edge_pass_count = 0usize;
    let mut outcomes = stream::iter(window.iter().cloned().map(|market| {
        let http = http.clone();
        async move { evaluate_market_for_arb(&http, clob_base, market, max_sum, min_edge).await }
    }))
    .buffer_unordered(PRICE_SCAN_CONCURRENCY);

    while let Some(outcome) = outcomes.next().await {
        match outcome {
            MarketScanOutcome::MissingTokenIds => {}
            MarketScanOutcome::TokenIdsResolved => {
                token_ids_ok_count += 1;
            }
            MarketScanOutcome::PriceOkButNotArb => {
                token_ids_ok_count += 1;
                clob_price_ok_count += 1;
            }
            MarketScanOutcome::UnderSumOnly => {
                token_ids_ok_count += 1;
                clob_price_ok_count += 1;
                under_sum_count += 1;
            }
            MarketScanOutcome::Candidate(candidate) => {
                token_ids_ok_count += 1;
                clob_price_ok_count += 1;
                under_sum_count += 1;
                edge_pass_count += 1;
                out.push(candidate);
            }
        }
    }

    out.sort_by(|a, b| b.edge.cmp(&a.edge));
    if out.len() > top_n {
        out.truncate(top_n);
    }
    println!(
        "scan_filter_stats: total_raw={}, filtered_non_tradeable={}, filtered_near_settlement={}, active_open={}, latest_window={}, token_ids_ok={}, clob_buy_price_ok={}, sum_below_threshold={}, edge_passed={}, min_edge={}, settle_guard_minutes={}, sorted_by=edge, top_returned={}",
        data.len(),
        filtered_non_tradeable,
        filtered_near_settlement,
        active_open_count,
        window.len(),
        token_ids_ok_count,
        clob_price_ok_count,
        under_sum_count,
        edge_pass_count,
        min_edge,
        settle_guard_minutes,
        out.len()
    );
    Ok(out)
}

pub async fn discover_btc_candle_market(epoch_ts: i64) -> Result<BtcCandleMarket> {
    let mut offsets = vec![0_i64, -300, 300];
    offsets.dedup();
    for off in offsets {
        let candidate_ts = epoch_ts + off;
        let slug = format!("btc-updown-5m-{candidate_ts}");
        if let Ok(market) = discover_btc_candle_market_by_slug(&slug).await {
            return Ok(market);
        }
    }
    Err(anyhow!(
        "failed to discover btc 5m market for epoch_ts={} (tried offsets 0,-300,+300)",
        epoch_ts
    ))
}

async fn discover_btc_candle_market_by_slug(slug: &str) -> Result<BtcCandleMarket> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build http client")?;
    let resp = http.get(&url).send().await.context("gamma events request failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!("gamma events request failed: status={}", resp.status()));
    }
    let root: Value = resp.json().await.context("failed to parse gamma events json")?;
    let arr = root
        .as_array()
        .ok_or_else(|| anyhow!("gamma events response is not an array"))?;
    let first = arr.first().ok_or_else(|| anyhow!("gamma events returned no results"))?;
    let markets = first
        .get("markets")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("gamma event missing markets array"))?;
    let market = markets
        .first()
        .ok_or_else(|| anyhow!("gamma event has empty markets"))?;

    let market_id = extract_stable_market_id(market);

    let outcomes = market
        .get("outcomes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let token_ids = extract_clob_token_ids_vec(market)
        .context("missing/invalid clobTokenIds for btc candle market")?;
    if token_ids.len() < 2 {
        return Err(anyhow!("btc candle market: clobTokenIds length < 2"));
    }

    let (up_idx, down_idx) = resolve_up_down_indices(&outcomes).unwrap_or((0_usize, 1_usize));
    if up_idx >= token_ids.len() || down_idx >= token_ids.len() {
        return Err(anyhow!(
            "btc candle market: outcome indices out of bounds (up_idx={}, down_idx={}, token_ids_len={})",
            up_idx,
            down_idx,
            token_ids.len()
        ));
    }

    Ok(BtcCandleMarket {
        market_id,
        market_slug: slug.to_string(),
        up_token_id: token_ids[up_idx].clone(),
        down_token_id: token_ids[down_idx].clone(),
    })
}

fn resolve_up_down_indices(outcomes: &[String]) -> Option<(usize, usize)> {
    let mut up: Option<usize> = None;
    let mut down: Option<usize> = None;
    for (i, o) in outcomes.iter().enumerate() {
        let n = o.to_ascii_lowercase();
        if up.is_none()
            && (n == "up" || n.contains("higher") || n.contains("up "))
        {
            up = Some(i);
        }
        if down.is_none()
            && (n == "down" || n.contains("lower") || n.contains("down "))
        {
            down = Some(i);
        }
    }
    match (up, down) {
        (Some(u), Some(d)) if u != d => Some((u, d)),
        _ => None,
    }
}

fn extract_clob_token_ids_vec(market: &Value) -> Option<Vec<String>> {
    let raw = market.get("clobTokenIds")?;
    if let Some(arr) = raw.as_array() {
        let ids: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect();
        return if ids.is_empty() { None } else { Some(ids) };
    }
    if let Some(s) = raw.as_str() {
        return serde_json::from_str::<Vec<String>>(s).ok().filter(|v| !v.is_empty());
    }
    None
}

/// Force-poll executable quotes for specific market ids (used by exit checks on held positions).
pub async fn fetch_market_snapshots_by_ids(
    execution: &ExecutionSettings,
    market_ids: &[String],
) -> Result<Vec<crate::types::MarketSnapshot>> {
    if market_ids.is_empty() {
        return Ok(Vec::new());
    }

    let request_timeout = Duration::from_secs(execution.timeout_secs.max(1));
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(request_timeout)
        .build()
        .context("failed to build http client")?;
    let base = scan_markets_base_url(execution);
    let url = format!(
        "{}/markets?limit=1000&closed=false&archived=false",
        base.trim_end_matches('/')
    );
    let resp = http.get(&url).send().await.context("markets request failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!("markets request failed with status {}", resp.status()));
    }
    let root: Value = resp.json().await.context("failed to parse markets json")?;
    let data = if let Some(arr) = root.as_array() {
        arr
    } else {
        root.get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing data array in markets response"))?
    };

    let clob_base = "https://clob.polymarket.com";
    let target_ids: HashSet<String> = market_ids.iter().cloned().collect();
    let mut found_ids: HashSet<String> = HashSet::new();
    let mut snapshots = Vec::new();

    for market in data {
        let market_id = extract_stable_market_id(market);
        if !target_ids.contains(&market_id) || found_ids.contains(&market_id) {
            continue;
        }
        let Some((token_a, token_b)) = extract_clob_token_ids(market) else {
            continue;
        };
        let Some((price_a, bid_a, price_b, bid_b)) =
            fetch_executable_quotes(&http, clob_base, &token_a, &token_b).await
        else {
            continue;
        };
        snapshots.push(crate::types::MarketSnapshot {
            market_id: market_id.clone(),
            yes_token_id: Some(token_a.clone()),
            no_token_id: Some(token_b.clone()),
            yes_best_bid: bid_a,
            yes_best_ask: price_a,
            no_best_bid: bid_b,
            no_best_ask: price_b,
            volume_24h: extract_volume_24h(market),
            timestamp: Utc::now(),
        });
        found_ids.insert(market_id);
        if found_ids.len() >= target_ids.len() {
            break;
        }
    }

    Ok(snapshots)
}

enum MarketScanOutcome {
    MissingTokenIds,
    TokenIdsResolved,
    PriceOkButNotArb,
    UnderSumOnly,
    Candidate(ArbCandidate),
}

async fn evaluate_market_for_arb(
    http: &reqwest::Client,
    clob_base: &str,
    market: &Value,
    max_sum: Decimal,
    min_edge: Decimal,
) -> MarketScanOutcome {
    let Some((token_a, token_b)) = extract_clob_token_ids(market) else {
        return MarketScanOutcome::MissingTokenIds;
    };

    let Some((price_a, bid_a, price_b, bid_b)) =
        fetch_executable_quotes(http, clob_base, &token_a, &token_b).await
    else {
        return MarketScanOutcome::TokenIdsResolved;
    };

    let sum = price_a + price_b;
    if sum >= max_sum {
        return MarketScanOutcome::PriceOkButNotArb;
    }

    let edge = Decimal::ONE - sum;
    if edge < min_edge {
        return MarketScanOutcome::UnderSumOnly;
    }

    MarketScanOutcome::Candidate(ArbCandidate {
        market_id: extract_stable_market_id(market),
        question: market
            .get("question")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        market_slug: market
            .get("market_slug")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        yes_token_id: token_a,
        no_token_id: token_b,
        bid_a,
        price_a,
        bid_b,
        price_b,
        volume_24h: extract_volume_24h(market),
        sum,
        edge,
    })
}

fn bool_by_keys(market: &Value, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(v) = market.get(key).and_then(|v| v.as_bool()) {
            return Some(v);
        }
    }
    None
}

fn is_market_tradeable(market: &Value) -> bool {
    // Basic lifecycle filter.
    let active = bool_by_keys(market, &["active"]).unwrap_or(false);
    let closed = bool_by_keys(market, &["closed"]).unwrap_or(true);
    let archived = bool_by_keys(market, &["archived"]).unwrap_or(true);
    if !(active && !closed && !archived) {
        return false;
    }

    // Additional status guards from different API payload variants.
    if bool_by_keys(market, &["resolved", "isResolved"]).unwrap_or(false) {
        return false;
    }
    if bool_by_keys(market, &["accepting_orders", "acceptingOrders", "enableOrderBook"])
        == Some(false)
    {
        return false;
    }

    true
}

fn market_end_time_ms(market: &Value) -> Option<i64> {
    for key in [
        "endDate",
        "end_date",
        "endTime",
        "end_time",
        "closeTime",
        "close_time",
    ] {
        let Some(s) = market.get(key).and_then(|v| v.as_str()) else {
            continue;
        };
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
    }
    None
}

fn scan_markets_base_url(execution: &ExecutionSettings) -> String {
    let configured = execution
        .polymarket_http_url
        .clone()
        .unwrap_or_else(|| "https://gamma-api.polymarket.com".to_string());
    if configured.contains("clob.polymarket.com") {
        "https://gamma-api.polymarket.com".to_string()
    } else {
        configured
    }
}

fn push_result(report: &mut HealthcheckReport, name: &str, res: Result<String>) {
    match res {
        Ok(detail) => report.checks.push(CheckItem {
            name: name.to_string(),
            ok: true,
            detail,
        }),
        Err(err) => report.checks.push(CheckItem {
            name: name.to_string(),
            ok: false,
            detail: err.to_string(),
        }),
    }
}

fn parse_decimal(v: Option<&Value>) -> Option<Decimal> {
    let raw = v?;
    if let Some(n) = raw.as_f64() {
        return Decimal::from_str(&format!("{n}")).ok();
    }
    if let Some(s) = raw.as_str() {
        return Decimal::from_str(s).ok();
    }
    None
}

fn extract_created_at_ms(market: &Value) -> i64 {
    let Some(s) = market.get("createdAt").and_then(|v| v.as_str()) else {
        return 0;
    };
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

/// Extract 24h volume from Gamma market payload using common key variants.
fn extract_volume_24h(market: &Value) -> Option<Decimal> {
    for key in ["volume24hr", "volume24h", "volume24Hr", "oneDayVolume", "volume"] {
        if let Some(v) = parse_decimal(market.get(key)) {
            return Some(v);
        }
    }
    None
}

/// Prefer stable API ids (conditionId / id) over slug/question for consistent position keying.
fn extract_stable_market_id(market: &Value) -> String {
    let try_str = |key: &str| {
        market
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    try_str("conditionId")
        .or_else(|| try_str("condition_id"))
        .or_else(|| try_str("id"))
        .or_else(|| try_str("market_slug"))
        .or_else(|| try_str("marketSlug"))
        .or_else(|| try_str("question"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn extract_clob_token_ids(market: &Value) -> Option<(String, String)> {
    let raw = market.get("clobTokenIds")?;
    let ids: Vec<String> = if let Some(arr) = raw.as_array() {
        arr.iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect()
    } else if let Some(s) = raw.as_str() {
        serde_json::from_str::<Vec<String>>(s).ok()?
    } else {
        return None;
    };
    if ids.len() != 2 {
        return None;
    }
    Some((ids[0].clone(), ids[1].clone()))
}

async fn fetch_executable_quotes(
    http: &reqwest::Client,
    clob_base: &str,
    token_a: &str,
    token_b: &str,
) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
    let ask_a = fetch_price(http, clob_base, token_a, "buy").await?;
    let ask_b = fetch_price(http, clob_base, token_b, "buy").await?;
    let bid_a = fetch_price(http, clob_base, token_a, "sell").await.unwrap_or(ask_a);
    let bid_b = fetch_price(http, clob_base, token_b, "sell").await.unwrap_or(ask_b);
    Some((ask_a, bid_a, ask_b, bid_b))
}

async fn fetch_price(
    http: &reqwest::Client,
    clob_base: &str,
    token_id: &str,
    side: &str,
) -> Option<Decimal> {
    let url = format!(
        "{}/price?token_id={}&side={}",
        clob_base.trim_end_matches('/'),
        token_id,
        side
    );
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let root: Value = resp.json().await.ok()?;
    parse_decimal(root.get("price"))
}

async fn check_clob_http(execution: &ExecutionSettings) -> Result<String> {
    let base = execution
        .polymarket_http_url
        .clone()
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let url = format!("{}/markets?limit=1", base.trim_end_matches('/'));
    let start = Instant::now();
    let resp = reqwest::get(&url).await.context("http request failed")?;
    let status = resp.status();
    let body = resp.bytes().await.context("failed reading body")?;
    if !status.is_success() {
        return Err(anyhow!("status={status}, body_len={}", body.len()));
    }
    Ok(format!(
        "status={status}, bytes={}, latency_ms={}",
        body.len(),
        start.elapsed().as_millis()
    ))
}

fn check_ws_tcp(execution: &ExecutionSettings) -> Result<String> {
    let ws = &execution.polymarket_ws_url;
    let url = Url::parse(ws).with_context(|| format!("invalid ws url: {ws}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("missing host in ws url"))?;
    let port = url.port_or_known_default().unwrap_or(443);

    let addr = resolve_first_addr(host, port)?;
    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    TcpStream::connect_timeout(&addr, timeout)
        .with_context(|| format!("tcp connect failed: {host}:{port}"))?;
    Ok(format!(
        "host={host}, port={port}, latency_ms={}",
        start.elapsed().as_millis()
    ))
}

fn resolve_first_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let mut addrs = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("dns resolve failed: {host}:{port}"))?;
    addrs.next().ok_or_else(|| anyhow!("no address resolved"))
}

async fn check_chain_id(rpc_url: &str) -> Result<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_chainId",
        "params": [],
        "id": 1
    });
    let resp = reqwest::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("rpc request failed")?
        .json::<JsonRpcResp>()
        .await
        .context("rpc parse failed")?;
    let chain = resp.result.ok_or_else(|| anyhow!("missing result"))?;
    Ok(format!("chain_id={chain}"))
}

async fn check_latest_block(rpc_url: &str) -> Result<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 2
    });
    let resp = reqwest::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("rpc request failed")?
        .json::<JsonRpcResp>()
        .await
        .context("rpc parse failed")?;
    let block_hex = resp.result.ok_or_else(|| anyhow!("missing result"))?;
    Ok(format!("latest_block={block_hex}"))
}

/// One attempt of an `eth_call` against a single RPC URL.
async fn eth_call_once(
    client: &reqwest::Client,
    rpc_url: &str,
    contract: &str,
    data: &str,
) -> Result<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": contract, "data": data}, "latest"],
        "id": 42
    });
    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("rpc request to {rpc_url} failed"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .with_context(|| format!("rpc body read from {rpc_url} failed"))?;
    if !status.is_success() {
        return Err(anyhow!(
            "rpc {rpc_url} http {status}: {}",
            text.chars().take(200).collect::<String>()
        ));
    }
    let parsed: JsonRpcResp = serde_json::from_str(&text).with_context(|| {
        format!(
            "rpc {rpc_url} parse failed, body={}",
            text.chars().take(200).collect::<String>()
        )
    })?;
    if let Some(err) = parsed.error {
        return Err(anyhow!(
            "rpc {rpc_url} jsonrpc error code={:?} msg={:?}",
            err.code,
            err.message
        ));
    }
    parsed.result.ok_or_else(|| {
        anyhow!(
            "rpc {rpc_url}: missing result (body={})",
            text.chars().take(200).collect::<String>()
        )
    })
}

/// Query ERC20 `balanceOf(wallet)` via `eth_call` and return the human-readable decimal value
/// (raw uint256 divided by 10^`decimals`).
///
/// `rpc_urls` is tried in order, with per-URL retries and exponential backoff. Returns the
/// first successful result; if all URLs fail, returns the last error seen.
pub async fn fetch_erc20_balance_multi(
    rpc_urls: &[&str],
    contract: &str,
    wallet: &str,
    decimals: u32,
) -> Result<Decimal> {
    if rpc_urls.is_empty() {
        return Err(anyhow!("no rpc url configured"));
    }
    let wallet_clean = wallet.trim().trim_start_matches("0x");
    let data = format!("0x70a08231{:0>64}", wallet_clean);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("http client build failed")?;

    // Collect the FINAL error from each URL (after per-URL retries exhausted) so the
    // caller can show the operator every failure mode at once — hiding all but the
    // last one makes it hard to tell e.g. "rpc A 429, rpc B auth, rpc C timeout".
    let mut per_url_errors: Vec<String> = Vec::new();
    const MAX_ATTEMPTS_PER_URL: u32 = 3;
    for url in rpc_urls {
        let mut last_err_for_url: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_ATTEMPTS_PER_URL {
            match eth_call_once(&client, url, contract, &data).await {
                Ok(hex) => {
                    let hex_clean = hex.trim().trim_start_matches("0x");
                    let hex_clean = if hex_clean.is_empty() { "0" } else { hex_clean };
                    let raw = u128::from_str_radix(hex_clean, 16).with_context(|| {
                        format!("erc20 balanceOf: invalid hex '{hex_clean}'")
                    })?;
                    let divisor = Decimal::from(10u64.pow(decimals));
                    return Ok(Decimal::from(raw) / divisor);
                }
                Err(e) => {
                    let is_last = attempt == MAX_ATTEMPTS_PER_URL;
                    if !is_last {
                        // Exponential backoff: 300ms, 900ms.
                        let backoff_ms = 300u64 * 3u64.pow(attempt - 1);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                    last_err_for_url = Some(e);
                }
            }
        }
        if let Some(e) = last_err_for_url {
            per_url_errors.push(format!("[{url}] {e}"));
        }
    }
    Err(anyhow!(
        "erc20 balanceOf failed on all {} rpc(s):\n  - {}",
        per_url_errors.len(),
        per_url_errors.join("\n  - ")
    ))
}

/// Single-URL variant retained for backward compatibility and healthcheck callers.
pub async fn fetch_erc20_balance(
    rpc_url: &str,
    contract: &str,
    wallet: &str,
    decimals: u32,
) -> Result<Decimal> {
    fetch_erc20_balance_multi(&[rpc_url], contract, wallet, decimals).await
}

/// Convenience wrapper: query USDC (6-decimal) balance for a wallet, with multi-RPC fallback.
pub async fn fetch_usdc_balance_multi(
    rpc_urls: &[&str],
    usdc_contract: &str,
    wallet: &str,
) -> Result<Decimal> {
    fetch_erc20_balance_multi(rpc_urls, usdc_contract, wallet, 6).await
}

/// Convenience wrapper: query USDC (6-decimal) balance for a wallet.
pub async fn fetch_usdc_balance(rpc_url: &str, usdc_contract: &str, wallet: &str) -> Result<Decimal> {
    fetch_erc20_balance(rpc_url, usdc_contract, wallet, 6).await
}

async fn check_contract_code(rpc_url: &str, contract: &str) -> Result<String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getCode",
        "params": [contract, "latest"],
        "id": 3
    });
    let resp = reqwest::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("rpc request failed")?
        .json::<JsonRpcResp>()
        .await
        .context("rpc parse failed")?;
    let code = resp.result.unwrap_or_default();
    let present = code.len() > 2;
    if !present {
        return Err(anyhow!("empty bytecode for contract={contract}"));
    }
    Ok(format!("contract={contract}, code_present=true"))
}

fn load_dotenv_map(path: &Path) -> Result<HashMap<String, String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read dotenv file: {}", path.display()))?;
    let mut out = HashMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        let key = k.trim().to_string();
        if key.is_empty() {
            continue;
        }
        let mut val = v.trim().to_string();
        if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            val = val[1..val.len() - 1].to_string();
        } else if let Some((head, _)) = val.split_once('#') {
            val = head.trim().to_string();
        }
        out.insert(key, val);
    }
    Ok(out)
}
