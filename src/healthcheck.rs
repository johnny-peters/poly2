use anyhow::{anyhow, Context, Result};
use reqwest::Url;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::config::ExecutionSettings;

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
}

#[derive(Clone, Debug)]
pub struct ArbCandidate {
    /// Stable id for dedup and position key (condition_id or API id preferred over slug/question).
    pub market_id: String,
    pub question: String,
    pub market_slug: String,
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
) -> Result<Vec<ArbCandidate>> {
    const LATEST_WINDOW: usize = 200;
    let clob_base = "https://clob.polymarket.com";
    let http = reqwest::Client::new();
    let base = scan_markets_base_url(execution);
    let url = format!(
        "{}/markets?limit=1000&closed=false&archived=false",
        base.trim_end_matches('/')
    );
    let resp = reqwest::get(&url).await.context("markets request failed")?;
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

    let mut active_open = Vec::new();
    for m in data {
        let active = m.get("active").and_then(|v| v.as_bool()).unwrap_or(false);
        let closed = m.get("closed").and_then(|v| v.as_bool()).unwrap_or(true);
        let archived = m.get("archived").and_then(|v| v.as_bool()).unwrap_or(true);
        if !(active && !closed && !archived) {
            continue;
        }
        active_open.push(m);
    }

    active_open.sort_by_key(|m| std::cmp::Reverse(extract_created_at_ms(m)));
    let window = &active_open[..active_open.len().min(LATEST_WINDOW)];

    let mut out = Vec::new();
    let active_open_count = active_open.len();
    let mut price_parsed_count = 0usize;
    let mut clob_price_ok_count = 0usize;
    let mut under_sum_count = 0usize;
    let mut edge_pass_count = 0usize;
    for m in window {
        let Some((token_a, token_b)) = extract_clob_token_ids(m) else {
            continue;
        };
        price_parsed_count += 1;
        let Some((price_a, bid_a, price_b, bid_b)) =
            fetch_executable_quotes(&http, clob_base, &token_a, &token_b).await
        else {
            continue;
        };
        clob_price_ok_count += 1;
        let sum = price_a + price_b;
        if sum >= max_sum {
            continue;
        }
        under_sum_count += 1;
        let edge = Decimal::ONE - sum;
        if edge < min_edge {
            continue;
        }
        edge_pass_count += 1;
        let candidate = ArbCandidate {
            market_id: extract_stable_market_id(m),
            question: m
                .get("question")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            market_slug: m
                .get("market_slug")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            bid_a,
            price_a,
            bid_b,
            price_b,
            volume_24h: extract_volume_24h(m),
            sum,
            edge,
        };
        out.push(candidate);
    }

    out.sort_by(|a, b| b.edge.cmp(&a.edge));
    if out.len() > top_n {
        out.truncate(top_n);
    }
    println!(
        "scan_filter_stats: total_raw={}, active_open={}, latest_window={}, token_ids_ok={}, clob_buy_price_ok={}, sum_below_threshold={}, edge_passed={}, min_edge={}, sorted_by=edge, top_returned={}",
        data.len(),
        active_open_count,
        window.len(),
        price_parsed_count,
        clob_price_ok_count,
        under_sum_count,
        edge_pass_count,
        min_edge,
        out.len()
    );
    Ok(out)
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
