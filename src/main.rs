use anyhow::{anyhow, Context};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::process::Command;
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
    let dotenv_path = Path::new("src/.env");
    let app_config = AppConfig::load_from_path_with_env_overlay(
        "config/default.yaml",
        Some(dotenv_path),
    )?;
    if args.get(1).map(|s| s.as_str()) == Some("healthcheck") {
        let report = run_healthcheck(&app_config.execution_settings(), dotenv_path).await;
        let healthcheck_passed = report.passed();
        println!("healthcheck_passed={healthcheck_passed}");
        for c in report.checks {
            println!(
                "- [{}] {} => {}",
                if c.ok { "OK" } else { "FAIL" },
                c.name,
                c.detail
            );
        }
        let strict = args.iter().any(|a| a == "--strict");
        if strict {
            println!("healthcheck_strict=true");
            if !healthcheck_passed {
                return Err(anyhow!("strict healthcheck failed: connectivity checks not passed"));
            }
            let preflight = run_startup_preflight(&app_config, dotenv_path)?;
            print_preflight_report(&preflight);
            if preflight.live_mode && preflight.trading_enabled && preflight.total_capital.is_none() {
                return Err(anyhow!(
                    "strict preflight failed: TOTAL_CAPITAL_USD or TOTAL_CAPITAL_CMD is required"
                ));
            }
        }
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("scan-arb") {
        let execution_settings = app_config.execution_settings();
        let mut top_n: usize = 20;
        let mut max_sum = read_scan_max_sum(Path::new("src/.env"), Decimal::from_str("1.005")?);
        let mut min_edge = read_scan_min_edge(Path::new("src/.env"), Decimal::from_str("0.005")?);
        let mut settle_guard_minutes = execution_settings.scan_min_minutes_to_settle;
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
                "--settle-guard-minutes" if i + 1 < args.len() => {
                    settle_guard_minutes = args[i + 1].parse::<u64>()?;
                    i += 2;
                }
                _ => {
                    i += 1;
                }
            }
        }

        run_scan_arb_once(
            &execution_settings,
            top_n,
            max_sum,
            min_edge,
            settle_guard_minutes,
        )
        .await;
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
    let scan_settle_guard_minutes = execution_settings.scan_min_minutes_to_settle;
    println!("market scan interval={}s (from FETCH_INTERVAL)", fetch_interval_secs);
    println!("market scan max_sum={} (from SCAN_MAX_SUM)", scan_max_sum);
    println!("market scan min_edge={} (from SCAN_MIN_EDGE)", scan_min_edge);
    println!(
        "market scan settle_guard_minutes={} (from SCAN_MIN_MINUTES_TO_SETTLE)",
        scan_settle_guard_minutes
    );
    let preflight = run_startup_preflight(&app_config, dotenv_path)?;
    print_preflight_report(&preflight);
    let context = StrategyContext {
        risk: RiskContext {
            total_capital: preflight
                .total_capital
                .unwrap_or_else(|| Decimal::from(100_000_i32)),
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
            scan_settle_guard_minutes,
            &context,
            &mut runner,
            order_log_path.as_deref(),
            positions_path_ref,
            dotenv_path,
            &preflight,
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
            scan_settle_guard_minutes,
            &context,
            &mut runner,
            None,
            positions_path_ref,
            dotenv_path,
            &preflight,
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
    settle_guard_minutes: u64,
) {
    let started = Utc::now();
    println!("scan_started_at={}", started.to_rfc3339());
    match scan_arb_candidates(
        execution_settings,
        top_n,
        max_sum,
        min_edge,
        settle_guard_minutes,
    )
    .await
    {
        Ok(candidates) => {
            println!("arb_candidates_count={}", candidates.len());
            for (i, c) in candidates.iter().enumerate() {
                let volume_24h = c
                    .volume_24h
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string());
                println!(
                    "#{:02} edge={} sum={} pA={} pB={} vol24h={} slug={} question={}",
                    i + 1,
                    c.edge,
                    c.sum,
                    c.price_a,
                    c.price_b,
                    volume_24h,
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
    settle_guard_minutes: u64,
    context: &StrategyContext,
    runner: &mut EngineRunner<C>,
    order_log_path: Option<&Path>,
    positions_path: &Path,
    dotenv_path: &Path,
    preflight: &PreflightReport,
) where
    C: ExecutionClient,
{
    let mut loop_idx: u64 = 0;
    let refresh_every = read_capital_refresh_loops(dotenv_path, 10);
    let mut cached_capital = preflight.total_capital;
    let mut cached_source = preflight.capital_source.clone();
    let mut failure_breaker =
        FailureCircuitBreaker::new(FailureCircuitBreakerConfig::from_env(dotenv_path));
    loop {
        loop_idx += 1;
        println!("\n================ loop_at={} ================", Utc::now().to_rfc3339());
        let trading_enabled = read_trading_enabled(dotenv_path, preflight.trading_enabled);
        if !trading_enabled {
            failure_breaker.reset();
        }
        let breaker_open = trading_enabled && failure_breaker.is_open(loop_idx);
        let mut loop_context = context.clone();
        let mut loop_outcome = LoopOutcome::Neutral;
        if preflight.live_mode && trading_enabled {
            let should_refresh = cached_capital.is_none() || loop_idx % refresh_every == 1;
            if should_refresh {
                match resolve_total_capital(dotenv_path) {
                    Ok((capital, source)) => {
                        cached_capital = Some(capital);
                        cached_source = source;
                    }
                    Err(err) => {
                        eprintln!(
                            "capital refresh failed (loop={}): {}; trading skipped for safety",
                            loop_idx, err
                        );
                    }
                }
            }
            if let Some(capital) = cached_capital {
                loop_context.risk.total_capital = capital;
                println!("risk_total_capital={} source={}", capital, cached_source);
            }
        }
        match scan_arb_candidates(
            execution_settings,
            top_n,
            max_sum,
            min_edge,
            settle_guard_minutes,
        )
        .await
        {
            Ok(candidates) => {
                println!("scan_result_count={}", candidates.len());
                for (idx, c) in candidates.iter().enumerate() {
                    let volume_24h = c
                        .volume_24h
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "n/a".to_string());
                    println!(
                        "scan #{:02}: edge={} sum={} pA={} pB={} vol24h={} slug={} question={}",
                        idx + 1,
                        c.edge,
                        c.sum,
                        c.price_a,
                        c.price_b,
                        volume_24h,
                        c.market_slug,
                        c.question
                    );
                }

                let snapshots = to_market_snapshots(&candidates);
                if snapshots.is_empty() {
                    println!("no executable snapshots in this loop");
                } else if !trading_enabled {
                    println!("trading_enabled=false: skip order execution, scan only");
                } else if breaker_open {
                    eprintln!(
                        "ALERT circuit breaker open: skip execution at loop={}; close_at_loop={}",
                        loop_idx,
                        failure_breaker.open_until_loop().unwrap_or(loop_idx)
                    );
                } else {
                    let summary = runner
                        .run_snapshots_with_report_hook(snapshots, &loop_context, |report| {
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
                                loop_outcome = LoopOutcome::Failure;
                            } else {
                                loop_outcome = LoopOutcome::Success;
                            }
                        }
                        Err(err) => {
                            eprintln!("engine execute failed: {err}");
                            loop_outcome = LoopOutcome::Failure;
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("market scan failed: {err}");
                loop_outcome = LoopOutcome::Failure;
            }
        }
        if trading_enabled {
            if let Some(alert) = failure_breaker.observe(loop_outcome, loop_idx) {
                eprintln!("ALERT {}", alert);
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

#[derive(Clone, Debug)]
struct FailureCircuitBreakerConfig {
    consecutive_failure_limit: u64,
    window_loops: usize,
    max_failure_rate: f64,
    cooldown_loops: u64,
}

impl FailureCircuitBreakerConfig {
    fn from_env(dotenv_path: &Path) -> Self {
        let consecutive_failure_limit = resolve_runtime_var("CB_CONSECUTIVE_FAILURE_LIMIT", dotenv_path)
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(3)
            .max(1);
        let window_loops = resolve_runtime_var("CB_WINDOW_LOOPS", dotenv_path)
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(20)
            .max(1);
        let max_failure_rate = resolve_runtime_var("CB_MAX_FAILURE_RATE", dotenv_path)
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map(|v| v.clamp(0.0, 1.0))
            .unwrap_or(0.5);
        let cooldown_loops = resolve_runtime_var("CB_COOLDOWN_LOOPS", dotenv_path)
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5)
            .max(1);
        Self {
            consecutive_failure_limit,
            window_loops,
            max_failure_rate,
            cooldown_loops,
        }
    }
}

#[derive(Clone, Debug)]
enum LoopOutcome {
    Success,
    Failure,
    Neutral,
}

#[derive(Clone, Debug)]
struct FailureCircuitBreaker {
    config: FailureCircuitBreakerConfig,
    consecutive_failures: u64,
    recent_failures: VecDeque<bool>,
    open_until_loop: Option<u64>,
}

impl FailureCircuitBreaker {
    fn new(config: FailureCircuitBreakerConfig) -> Self {
        Self {
            config,
            consecutive_failures: 0,
            recent_failures: VecDeque::new(),
            open_until_loop: None,
        }
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.recent_failures.clear();
        self.open_until_loop = None;
    }

    fn is_open(&self, loop_idx: u64) -> bool {
        self.open_until_loop.is_some_and(|until| loop_idx <= until)
    }

    fn open_until_loop(&self) -> Option<u64> {
        self.open_until_loop
    }

    fn observe(&mut self, outcome: LoopOutcome, loop_idx: u64) -> Option<String> {
        match outcome {
            LoopOutcome::Neutral => return None,
            LoopOutcome::Success => {
                self.consecutive_failures = 0;
                self.record_failure_flag(false);
                if self.open_until_loop.is_some() {
                    self.open_until_loop = None;
                    return Some("circuit breaker closed after successful loop".to_string());
                }
                return None;
            }
            LoopOutcome::Failure => {
                self.consecutive_failures += 1;
                self.record_failure_flag(true);
            }
        }

        if self.open_until_loop.is_some_and(|until| loop_idx <= until) {
            return None;
        }

        if self.consecutive_failures >= self.config.consecutive_failure_limit {
            let until = loop_idx + self.config.cooldown_loops;
            self.open_until_loop = Some(until);
            return Some(format!(
                "circuit breaker opened: consecutive_failures={} threshold={} cooldown_loops={} close_at_loop={}",
                self.consecutive_failures,
                self.config.consecutive_failure_limit,
                self.config.cooldown_loops,
                until
            ));
        }

        let window_size = self.recent_failures.len();
        if window_size >= self.config.window_loops {
            let failures = self.recent_failures.iter().filter(|v| **v).count();
            let failure_rate = failures as f64 / window_size as f64;
            if failure_rate >= self.config.max_failure_rate {
                let until = loop_idx + self.config.cooldown_loops;
                self.open_until_loop = Some(until);
                return Some(format!(
                    "circuit breaker opened: failure_rate={:.2} threshold={:.2} window={} cooldown_loops={} close_at_loop={}",
                    failure_rate,
                    self.config.max_failure_rate,
                    window_size,
                    self.config.cooldown_loops,
                    until
                ));
            }
        }

        None
    }

    fn record_failure_flag(&mut self, is_failure: bool) {
        self.recent_failures.push_back(is_failure);
        while self.recent_failures.len() > self.config.window_loops {
            self.recent_failures.pop_front();
        }
    }
}

#[derive(Clone, Debug)]
struct PreflightReport {
    live_mode: bool,
    trading_enabled: bool,
    total_capital: Option<Decimal>,
    capital_source: String,
}

fn run_startup_preflight(app_config: &AppConfig, dotenv_path: &Path) -> anyhow::Result<PreflightReport> {
    let live_mode = app_config.execution.polymarket_http_url.is_some();
    let trading_enabled = read_trading_enabled(dotenv_path, true);
    let mut total_capital = None;
    let mut capital_source = "n/a".to_string();

    if live_mode && trading_enabled {
        if let Some(api_key_env) = &app_config.execution.api_key_env {
            let val = resolve_runtime_var(api_key_env, dotenv_path);
            if val.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(anyhow!(
                    "preflight failed: required api key env '{}' is missing/empty",
                    api_key_env
                ));
            }
        }
        if let Some(signature_env) = &app_config.execution.signature_env {
            let val = resolve_runtime_var(signature_env, dotenv_path);
            if val.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(anyhow!(
                    "preflight failed: required signature env '{}' is missing/empty",
                    signature_env
                ));
            }
        }

        let (capital, source) = resolve_total_capital(dotenv_path)?;
        total_capital = Some(capital);
        capital_source = source;

        let min_total_capital = read_decimal_var(dotenv_path, "MIN_TOTAL_CAPITAL_USD")
            .unwrap_or_else(|| Decimal::ONE);
        if capital < min_total_capital {
            return Err(anyhow!(
                "preflight failed: capital={} below MIN_TOTAL_CAPITAL_USD={}",
                capital,
                min_total_capital
            ));
        }

        if app_config.strategies.arbitrage.enabled {
            let arb = app_config.arbitrage_config()?;
            let min_order_size = read_decimal_var(dotenv_path, "MIN_ORDER_SIZE_USD")
                .unwrap_or_else(|| Decimal::ONE);
            if arb.order_size < min_order_size {
                return Err(anyhow!(
                    "preflight failed: arbitrage order_size={} < MIN_ORDER_SIZE_USD={}",
                    arb.order_size,
                    min_order_size
                ));
            }
        }
    }

    Ok(PreflightReport {
        live_mode,
        trading_enabled,
        total_capital,
        capital_source,
    })
}

fn print_preflight_report(preflight: &PreflightReport) {
    println!(
        "preflight: live_mode={}, trading_enabled={}",
        preflight.live_mode, preflight.trading_enabled
    );
    if let Some(capital) = preflight.total_capital {
        println!(
            "preflight: total_capital={} source={}",
            capital, preflight.capital_source
        );
    }
}

fn resolve_total_capital(dotenv_path: &Path) -> anyhow::Result<(Decimal, String)> {
    if let Some(raw) = resolve_runtime_var("TOTAL_CAPITAL_USD", dotenv_path) {
        let parsed = Decimal::from_str(raw.trim())
            .with_context(|| format!("invalid TOTAL_CAPITAL_USD value: {}", raw.trim()))?;
        if parsed <= Decimal::ZERO {
            return Err(anyhow!("TOTAL_CAPITAL_USD must be > 0"));
        }
        return Ok((parsed, "TOTAL_CAPITAL_USD".to_string()));
    }
    if let Some(cmd) = resolve_runtime_var("TOTAL_CAPITAL_CMD", dotenv_path) {
        let output = if cfg!(target_os = "windows") {
            Command::new("powershell")
                .args(["-NoProfile", "-Command", cmd.trim()])
                .output()
        } else {
            Command::new("sh").args(["-lc", cmd.trim()]).output()
        }
        .context("failed to execute TOTAL_CAPITAL_CMD")?;
        if !output.status.success() {
            return Err(anyhow!(
                "TOTAL_CAPITAL_CMD failed with status {}",
                output.status
            ));
        }
        let stdout = String::from_utf8(output.stdout).context("TOTAL_CAPITAL_CMD stdout is not utf-8")?;
        let line = stdout.lines().next().unwrap_or("").trim();
        if line.is_empty() {
            return Err(anyhow!("TOTAL_CAPITAL_CMD returned empty output"));
        }
        let parsed = Decimal::from_str(line)
            .with_context(|| format!("TOTAL_CAPITAL_CMD output is not decimal: '{line}'"))?;
        if parsed <= Decimal::ZERO {
            return Err(anyhow!("TOTAL_CAPITAL_CMD must output a value > 0"));
        }
        return Ok((parsed, "TOTAL_CAPITAL_CMD".to_string()));
    }
    Err(anyhow!(
        "missing capital source: set TOTAL_CAPITAL_USD or TOTAL_CAPITAL_CMD"
    ))
}

fn read_capital_refresh_loops(dotenv_path: &Path, default_loops: u64) -> u64 {
    resolve_runtime_var("CAPITAL_REFRESH_LOOPS", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_loops)
        .max(1)
}

fn read_trading_enabled(dotenv_path: &Path, default_value: bool) -> bool {
    let Some(raw) = resolve_runtime_var("TRADING_ENABLED", dotenv_path) else {
        return default_value;
    };
    parse_bool_flag(raw.trim()).unwrap_or(default_value)
}

fn parse_bool_flag(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn read_decimal_var(dotenv_path: &Path, key: &str) -> Option<Decimal> {
    resolve_runtime_var(key, dotenv_path).and_then(|v| Decimal::from_str(v.trim()).ok())
}

fn resolve_runtime_var(key: &str, dotenv_path: &Path) -> Option<String> {
    std::env::var(key)
        .ok()
        .or_else(|| load_dotenv_map(dotenv_path).get(key).cloned())
}

fn load_dotenv_map(path: &Path) -> HashMap<String, String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
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
    out
}
