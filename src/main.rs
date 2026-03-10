use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{self, Duration};

use poly2::{
    append_order_record, load_positions, run_healthcheck, save_positions, scan_arb_candidates,
    AppConfig, ArbitrageStrategy, EngineRunner, ExecutionClient, ExecutionReport, ExecutionSettings,
    MarketSnapshot, MockExecutionClient, PolymarketHttpExecutionClient, RetryingExecutionClient,
    RiskContext, RiskEngine, StrategyContext, StrategyId, StrategyRegistry, TodoStrategy,
    TradingEngine,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let app_config = AppConfig::load_from_path_with_env_overlay(
        "config/default.yaml",
        Some("src/.env"),
    )?;
    if args.get(1).map(|s| s.as_str()) == Some("healthcheck") {
        let report = run_healthcheck(&app_config.execution_settings(), Path::new("src/.env")).await;
        println!("healthcheck_passed={}", report.passed());
        for c in report.checks {
            println!(
                "- [{}] {} => {}",
                if c.ok { "OK" } else { "FAIL" },
                c.name,
                c.detail
            );
        }
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("scan-arb") {
        let mut top_n: usize = 20;
        let mut max_sum = read_scan_max_sum(Path::new("src/.env"), Decimal::from_str("1.005")?);
        let mut min_edge = read_scan_min_edge(Path::new("src/.env"), Decimal::from_str("0.005")?);
        let mut i = 2usize;
        while i < args.len() {
            match args[i].as_str() {
                "--top" if i + 1 < args.len() => {
                    top_n = args[i + 1].parse::<usize>()?;
                    i += 2;
                }
                "--max-sum" if i + 1 < args.len() => {
                    max_sum = Decimal::from_str(&args[i + 1])?;
                    i += 2;
                }
                "--min-edge" if i + 1 < args.len() => {
                    min_edge = Decimal::from_str(&args[i + 1])?;
                    i += 2;
                }
                _ => {
                    i += 1;
                }
            }
        }

        run_scan_arb_once(&app_config.execution_settings(), top_n, max_sum, min_edge).await;
        return Ok(());
    }
    let mut registry = StrategyRegistry::new();
    if app_config.strategies.arbitrage.enabled {
        registry.register(Arc::new(ArbitrageStrategy::new(app_config.arbitrage_config()?)));
    }

    registry.register(Arc::new(TodoStrategy::new(StrategyId::ProbabilityTrading)));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::MarketMaking)));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::LatencyArbitrage)));

    let risk_engine = RiskEngine::new(app_config.risk_config()?);
    let engine = TradingEngine::new(registry, risk_engine);

    let execution_settings = app_config.execution_settings();
    println!(
        "execution target={}, retries={}, timeout={}s",
        execution_settings.polymarket_ws_url,
        execution_settings.max_retries,
        execution_settings.timeout_secs
    );
    let fetch_interval_secs = read_fetch_interval_secs(Path::new("src/.env"), 60);
    let scan_max_sum = read_scan_max_sum(Path::new("src/.env"), Decimal::from_str("1.005")?);
    let scan_min_edge = read_scan_min_edge(Path::new("src/.env"), Decimal::from_str("0.005")?);
    println!("market scan interval={}s (from FETCH_INTERVAL)", fetch_interval_secs);
    println!("market scan max_sum={} (from SCAN_MAX_SUM)", scan_max_sum);
    println!("market scan min_edge={} (from SCAN_MIN_EDGE)", scan_min_edge);
    let context = StrategyContext {
        risk: RiskContext {
            total_capital: Decimal::from(100_000_i32),
            position_per_market: HashMap::new(),
            daily_pnl: Decimal::ZERO,
        },
        external_probability: None,
    };

    if let Some(http_url) = &execution_settings.polymarket_http_url {
        println!("execution mode=HTTP live, target={http_url}");
        let mut client = if let Some(env_key) = &execution_settings.api_key_env {
            PolymarketHttpExecutionClient::new(http_url.clone()).with_api_key_env(env_key)
        } else {
            PolymarketHttpExecutionClient::new(http_url.clone())
        };
        client = client.with_per_order_retry_policy(
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        client = client.with_status_polling(
            execution_settings.status_poll_attempts,
            execution_settings.status_poll_interval_ms,
        );
        if let Some(signature_env) = &execution_settings.signature_env {
            client = client.with_static_signature_env(signature_env);
        }
        let retrying = RetryingExecutionClient::new(
            client,
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        let trade_cooldown_secs = read_trade_cooldown_secs(Path::new("src/.env"), 300);
        let take_profit_pct = read_take_profit_pct(Path::new("src/.env"));
        let stop_loss_pct = read_stop_loss_pct(Path::new("src/.env"));
        println!("trade_cooldown_secs={} (same market skip re-trade)", trade_cooldown_secs);
        if let Some(p) = take_profit_pct {
            println!("take_profit_pct={} (close when unrealized PnL % >= this)", p);
        }
        if let Some(p) = stop_loss_pct {
            println!("stop_loss_pct={} (close when unrealized PnL % <= -this)", p);
        }
        let order_log_path = std::env::var("ORDER_LOG_PATH").ok().map(std::path::PathBuf::from);
        let positions_path = std::env::var("POSITIONS_PATH").ok().map(std::path::PathBuf::from);
        let positions_path_ref = positions_path.as_deref().unwrap_or(Path::new("data/positions.json"));
        let mut runner = EngineRunner::new(engine, retrying)
            .with_trade_cooldown_secs(trade_cooldown_secs)
            .with_take_profit_pct(take_profit_pct)
            .with_stop_loss_pct(stop_loss_pct)
            .with_position_manager(load_positions(positions_path_ref));
        run_live_market_loop(
            &execution_settings,
            fetch_interval_secs,
            1000,
            scan_max_sum,
            scan_min_edge,
            &context,
            &mut runner,
            order_log_path.as_deref(),
            positions_path_ref,
        )
        .await;
    } else {
        println!("execution mode=mock (polymarket_http_url is empty)");
        let retrying = RetryingExecutionClient::new(
            MockExecutionClient::default(),
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        let trade_cooldown_secs = read_trade_cooldown_secs(Path::new("src/.env"), 300);
        let positions_path = std::env::var("POSITIONS_PATH").ok().map(std::path::PathBuf::from);
        let positions_path_ref = positions_path.as_deref().unwrap_or(Path::new("data/positions.json"));
        let mut runner = EngineRunner::new(engine, retrying)
            .with_trade_cooldown_secs(trade_cooldown_secs)
            .with_position_manager(load_positions(positions_path_ref));
        run_live_market_loop(
            &execution_settings,
            fetch_interval_secs,
            20,
            scan_max_sum,
            scan_min_edge,
            &context,
            &mut runner,
            None,
            positions_path_ref,
        )
        .await;
    }

    Ok(())
}

async fn run_scan_arb_once(
    execution_settings: &ExecutionSettings,
    top_n: usize,
    max_sum: Decimal,
    min_edge: Decimal,
) {
    let started = Utc::now();
    println!("scan_started_at={}", started.to_rfc3339());
    match scan_arb_candidates(execution_settings, top_n, max_sum, min_edge).await {
        Ok(candidates) => {
            println!("arb_candidates_count={}", candidates.len());
            for (i, c) in candidates.iter().enumerate() {
                println!(
                    "#{:02} edge={} sum={} pA={} pB={} slug={} question={}",
                    i + 1,
                    c.edge,
                    c.sum,
                    c.price_a,
                    c.price_b,
                    c.market_slug,
                    c.question
                );
            }
        }
        Err(err) => {
            eprintln!("scan-arb failed: {err}");
        }
    }
}

async fn run_live_market_loop<C>(
    execution_settings: &ExecutionSettings,
    interval_secs: u64,
    top_n: usize,
    max_sum: Decimal,
    min_edge: Decimal,
    context: &StrategyContext,
    runner: &mut EngineRunner<C>,
    order_log_path: Option<&Path>,
    positions_path: &Path,
) where
    C: ExecutionClient,
{
    loop {
        println!("\n================ loop_at={} ================", Utc::now().to_rfc3339());
        match scan_arb_candidates(execution_settings, top_n, max_sum, min_edge).await {
            Ok(candidates) => {
                println!("scan_result_count={}", candidates.len());
                for (idx, c) in candidates.iter().enumerate() {
                    println!(
                        "scan #{:02}: edge={} sum={} pA={} pB={} slug={} question={}",
                        idx + 1,
                        c.edge,
                        c.sum,
                        c.price_a,
                        c.price_b,
                        c.market_slug,
                        c.question
                    );
                }

                let snapshots = to_market_snapshots(&candidates);
                if snapshots.is_empty() {
                    println!("no executable snapshots in this loop");
                } else {
                    let summary = runner
                        .run_snapshots_with_report_hook(snapshots, context, |report| {
                            print_execution_report(report);
                            if let Some(path) = order_log_path {
                                let _ = append_order_record(path, report);
                            }
                        })
                        .await;
                    match summary {
                        Ok(s) => {
                            println!(
                                "order_summary: processed_snapshots={}, reports={}, skipped_cooldown={}, realized_notional={}, fees={}, unrealized_pnl={}",
                                s.processed_snapshots,
                                s.execution_reports,
                                s.skipped_cooldown,
                                s.realized_notional,
                                s.total_fees,
                                s.unrealized_pnl
                            );
                            println!(
                                "tracked_markets={}",
                                runner.position_manager().positions().len()
                            );
                            if let Err(e) = save_positions(positions_path, runner.position_manager()) {
                                eprintln!("persist positions failed: {}", e);
                            }
                        }
                        Err(err) => {
                            eprintln!("engine execute failed: {err}");
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("market scan failed: {err}");
            }
        }
        time::sleep(Duration::from_secs(interval_secs.max(1))).await;
    }
}

fn to_market_snapshots(candidates: &[poly2::ArbCandidate]) -> Vec<MarketSnapshot> {
    candidates
        .iter()
        .map(|c| MarketSnapshot {
            market_id: c.market_id.clone(),
            yes_best_bid: c.bid_a,
            yes_best_ask: c.price_a,
            no_best_bid: c.bid_b,
            no_best_ask: c.price_b,
            volume_24h: c.volume_24h,
            timestamp: Utc::now(),
        })
        .collect()
}

fn print_execution_report(report: &ExecutionReport) {
    println!(
        "order_report: market_id={}, status={:?}, action_count={}, order_ids={:?}",
        report.market_id, report.status, report.action_count, report.order_ids
    );
    for (idx, fill) in report.fills.iter().enumerate() {
        println!(
            "  fill #{:02}: order_id={:?}, fill_id={:?}, side={:?}, price={}, size={}, fee={}",
            idx + 1,
            fill.order_id,
            fill.fill_id,
            fill.side,
            fill.price,
            fill.size,
            fill.fee
        );
    }
}

fn read_fetch_interval_secs(dotenv_path: &Path, default_secs: u64) -> u64 {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_secs;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "FETCH_INTERVAL" {
            continue;
        }
        if let Ok(sec) = v.trim().parse::<u64>() {
            return sec.max(1);
        }
    }
    default_secs
}

fn read_scan_max_sum(dotenv_path: &Path, default_max_sum: Decimal) -> Decimal {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_max_sum;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_MAX_SUM" {
            continue;
        }
        if let Ok(val) = Decimal::from_str(v.trim()) {
            return val;
        }
    }
    default_max_sum
}

fn read_scan_min_edge(dotenv_path: &Path, default_min_edge: Decimal) -> Decimal {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_min_edge;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_MIN_EDGE" {
            continue;
        }
        if let Ok(val) = Decimal::from_str(v.trim()) {
            return val;
        }
    }
    default_min_edge
}

fn read_trade_cooldown_secs(dotenv_path: &Path, default_secs: u64) -> u64 {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_secs;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "TRADE_COOLDOWN_SECS" {
            continue;
        }
        if let Ok(sec) = v.trim().parse::<u64>() {
            return sec;
        }
    }
    default_secs
}

fn read_take_profit_pct(dotenv_path: &Path) -> Option<Decimal> {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return None;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "TAKE_PROFIT_PCT" {
            continue;
        }
        if let Ok(p) = Decimal::from_str(v.trim()) {
            return Some(p);
        }
    }
    None
}

fn read_stop_loss_pct(dotenv_path: &Path) -> Option<Decimal> {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return None;
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "STOP_LOSS_PCT" {
            continue;
        }
        if let Ok(p) = Decimal::from_str(v.trim()) {
            return Some(p);
        }
    }
    None
}
