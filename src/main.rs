use anyhow::{anyhow, Context};
use chrono::Utc;
use polymarket_client_sdk::auth::ExposeSecret as _;
use polymarket_client_sdk::auth::{LocalSigner as PmLocalSigner, Signer as _};
use polymarket_client_sdk::clob::types::{Side as PmSide, SignatureType as PmSignatureType};
use polymarket_client_sdk::clob::{Client as PmClobClient, Config as PmClobConfig};
use polymarket_client_sdk::types::{Address as PmAddress, Decimal as PmDecimal, U256 as PmU256};
use polymarket_client_sdk::{
    derive_proxy_wallet, derive_safe_wallet, POLYGON as PM_POLYGON, PRIVATE_KEY_VAR,
};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{self, Duration};

use poly2::{
    append_order_record, fetch_market_snapshots_by_ids, fetch_usdc_balance, load_positions,
    run_healthcheck, save_positions, scan_arb_candidates, AppConfig, ArbitrageStrategy,
    EngineRunner, ExecutionClient, ExecutionReport, ExecutionSettings, MarketSnapshot,
    MockExecutionClient, RetryingExecutionClient, RiskContext, RiskEngine,
    PolymarketSdkExecutionClient, StrategyContext, StrategyId, StrategyRegistry, TodoStrategy,
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
    if args.get(1).map(|s| s.as_str()) == Some("sdk-order") {
        run_sdk_limit_order(dotenv_path).await?;
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("sdk-auth-check") {
        run_sdk_auth_check(dotenv_path).await?;
        return Ok(());
    }
    if args.get(1).map(|s| s.as_str()) == Some("sdk-auth-export") {
        run_sdk_auth_export(dotenv_path).await?;
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
    let scan_top_n = read_scan_top_n(Path::new("src/.env"), 5);
    let scan_settle_guard_minutes = execution_settings.scan_min_minutes_to_settle;
    println!("market scan interval={}s (from FETCH_INTERVAL)", fetch_interval_secs);
    println!("market scan top_n={} (from SCAN_TOP_N)", scan_top_n);
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
        let pm_chain_id = read_pm_chain_id(dotenv_path, PM_POLYGON);
        let pm_signature_type = resolve_pm_signature_type(dotenv_path)?;
        let pm_funder = resolve_pm_funder_address(dotenv_path, pm_signature_type, pm_chain_id)?;
        let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
            .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
            .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;
        let signer = PmLocalSigner::from_str(private_key.trim())
            .context("invalid private key format")?
            .with_chain_id(Some(pm_chain_id));
        let mut auth_builder = PmClobClient::new(http_url, PmClobConfig::default())?
            .authentication_builder(&signer);
        if let Some(sig) = pm_signature_type {
            println!("pm_signature_type={}", pm_signature_type_to_name(sig));
            auth_builder = auth_builder.signature_type(sig);
        }
        if let Some(funder) = pm_funder {
            println!("pm_funder={funder}");
            auth_builder = auth_builder.funder(funder);
        }
        let sdk_client = auth_builder.authenticate().await?;
        let retrying = RetryingExecutionClient::new(
            PolymarketSdkExecutionClient::new(sdk_client, private_key, pm_chain_id),
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
            scan_top_n,
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
            scan_top_n,
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

async fn run_sdk_limit_order(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;
    let token_id_raw = resolve_runtime_var("PM_TOKEN_ID", dotenv_path)
        .ok_or_else(|| anyhow!("missing PM_TOKEN_ID in env/.env"))?;
    let price_raw = resolve_runtime_var("PM_ORDER_PRICE", dotenv_path)
        .unwrap_or_else(|| "0.50".to_string());
    let size_raw = resolve_runtime_var("PM_ORDER_SIZE", dotenv_path)
        .unwrap_or_else(|| "10".to_string());
    let side_raw = resolve_runtime_var("PM_ORDER_SIDE", dotenv_path)
        .unwrap_or_else(|| "buy".to_string());

    let token_id = PmU256::from_str(token_id_raw.trim())
        .with_context(|| format!("invalid PM_TOKEN_ID: {}", token_id_raw.trim()))?;
    let price = PmDecimal::from_str(price_raw.trim())
        .with_context(|| format!("invalid PM_ORDER_PRICE: {}", price_raw.trim()))?;
    let size = PmDecimal::from_str(size_raw.trim())
        .with_context(|| format!("invalid PM_ORDER_SIZE: {}", size_raw.trim()))?;
    let side = parse_pm_side(side_raw.trim())
        .ok_or_else(|| anyhow!("invalid PM_ORDER_SIDE: {} (expected buy/sell)", side_raw.trim()))?;
    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let pm_signature_type = resolve_pm_signature_type(dotenv_path)?;
    let pm_funder = resolve_pm_funder_address(dotenv_path, pm_signature_type, chain_id)?;
    let pm_funder_display = pm_funder
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "(auto)".to_string());
    let mut auth_builder = PmClobClient::new(&host, PmClobConfig::default())?
        .authentication_builder(&signer);
    if let Some(sig) = pm_signature_type {
        auth_builder = auth_builder.signature_type(sig);
    }
    if let Some(funder) = pm_funder {
        auth_builder = auth_builder.funder(funder);
    }
    let client = auth_builder.authenticate().await?;

    println!(
        "sdk-order: host={}, chain_id={}, token_id={}, side={:?}, price={}, size={}, pm_signature_type={}, pm_funder={}",
        host,
        chain_id,
        token_id,
        side,
        price,
        size,
        pm_signature_type
            .map(pm_signature_type_to_name)
            .unwrap_or("eoa"),
        pm_funder_display
    );

    let order = client
        .limit_order()
        .token_id(token_id)
        .price(price)
        .size(size)
        .side(side)
        .build()
        .await?;
    let signed_order = client.sign(&signer, order).await?;
    let response = client.post_order(signed_order).await?;

    println!("sdk-order: order_id={}", response.order_id);
    println!(
        "sdk-order: status={:?}, success={}, error_msg={:?}",
        response.status, response.success, response.error_msg
    );

    Ok(())
}

async fn run_sdk_auth_check(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;

    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let client = PmClobClient::new(&host, PmClobConfig::default())?;
    let credentials = client.create_or_derive_api_key(&signer, None).await?;
    let derived_key = credentials.key().to_string();
    let configured_key = resolve_runtime_var("POLYMARKET_API_KEY", dotenv_path)
        .or_else(|| resolve_runtime_var("API_KEY", dotenv_path));

    println!("sdk-auth-check: host={host}, chain_id={chain_id}");
    println!("sdk-auth-check: derived_api_key={derived_key}");
    if let Some(configured) = configured_key {
        let matches = configured.trim().eq_ignore_ascii_case(derived_key.as_str());
        println!("sdk-auth-check: configured_api_key={}", configured.trim());
        println!("sdk-auth-check: api_key_match={matches}");
        if !matches {
            return Err(anyhow!(
                "configured POLYMARKET_API_KEY does not match sdk-derived key"
            ));
        }
    } else {
        println!("sdk-auth-check: configured_api_key=(missing)");
    }
    Ok(())
}

async fn run_sdk_auth_export(dotenv_path: &Path) -> anyhow::Result<()> {
    let host = resolve_runtime_var("PM_HOST", dotenv_path)
        .unwrap_or_else(|| "https://clob.polymarket.com".to_string());
    let chain_id = resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(PM_POLYGON);
    let private_key = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
        .ok_or_else(|| anyhow!("missing private key: set {PRIVATE_KEY_VAR} or PRIVATE_KEY"))?;

    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format")?
        .with_chain_id(Some(chain_id));
    let client = PmClobClient::new(&host, PmClobConfig::default())?;
    let credentials = client.create_or_derive_api_key(&signer, None).await?;

    println!("# copy to src/.env");
    println!("POLYMARKET_API_KEY='{}'", credentials.key());
    println!(
        "POLYMARKET_SIGNATURE='{}'",
        credentials.secret().expose_secret()
    );
    println!(
        "POLYMARKET_PASSPHRASE='{}'",
        credentials.passphrase().expose_secret()
    );
    Ok(())
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
    let balance_check_wallet = resolve_runtime_var("PM_FUNDER", dotenv_path)
        .or_else(|| resolve_runtime_var("PROXY_WALLET", dotenv_path));
    let balance_check_rpc = resolve_runtime_var("RPC_URL", dotenv_path)
        .unwrap_or_else(|| "https://poly.api.pocket.network".to_string());
    let balance_check_usdc = resolve_runtime_var("USDC_CONTRACT_ADDRESS", dotenv_path)
        .unwrap_or_else(|| "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string());
    let min_balance_usd = read_decimal_var(dotenv_path, "MIN_ORDER_SIZE_USD")
        .unwrap_or_else(|| Decimal::ONE);
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
        let mut balance_too_low = false;
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
                let mut effective_capital = capital;
                if let Some(wallet) = &balance_check_wallet {
                    match fetch_usdc_balance(&balance_check_rpc, &balance_check_usdc, wallet).await {
                        Ok(onchain_balance) => {
                            println!("onchain_usdc_balance={} wallet={}", onchain_balance, wallet);
                            if onchain_balance < min_balance_usd {
                                eprintln!(
                                    "WARN insufficient USDC balance: {} < min {} — skipping execution",
                                    onchain_balance, min_balance_usd
                                );
                                balance_too_low = true;
                            }
                            effective_capital = capital.min(onchain_balance);
                        }
                        Err(err) => {
                            eprintln!("balance check failed: {err} — using configured capital");
                        }
                    }
                }
                loop_context.risk.total_capital = effective_capital;
                println!("risk_total_capital={} source={}", effective_capital, cached_source);
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
                } else if balance_too_low {
                    eprintln!("SKIP execution: USDC balance too low to place orders");
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
                            let mut forced_exit_error = false;
                            let open_market_ids = runner.position_manager().open_market_ids();
                            if !open_market_ids.is_empty() {
                                match fetch_market_snapshots_by_ids(execution_settings, &open_market_ids).await {
                                    Ok(forced_snapshots) => {
                                        if !forced_snapshots.is_empty() {
                                            println!(
                                                "forced_quote_poll: held_markets={}, quoted_markets={}",
                                                open_market_ids.len(),
                                                forced_snapshots.len()
                                            );
                                            let forced_marks: HashMap<String, (Decimal, Decimal, Decimal, Decimal)> =
                                                forced_snapshots
                                                    .iter()
                                                    .map(|s| {
                                                        (
                                                            s.market_id.clone(),
                                                            (
                                                                s.yes_best_bid,
                                                                s.yes_best_ask,
                                                                s.no_best_bid,
                                                                s.no_best_ask,
                                                            ),
                                                        )
                                                    })
                                                    .collect();
                                            match runner
                                                .run_forced_exit_checks_with_report_hook(&forced_marks, |report| {
                                                    print_execution_report(report);
                                                    if let Some(path) = order_log_path {
                                                        let _ = append_order_record(path, report);
                                                    }
                                                })
                                                .await
                                            {
                                                Ok(forced_exit_reports) => {
                                                    if forced_exit_reports > 0 {
                                                        println!(
                                                            "forced_exit_reports={} (held-market quote poll)",
                                                            forced_exit_reports
                                                        );
                                                    }
                                                }
                                                Err(err) => {
                                                    eprintln!("forced exit checks failed: {err}");
                                                    forced_exit_error = true;
                                                }
                                            }
                                        } else {
                                            println!(
                                                "forced_quote_poll: held_markets={} but no executable quotes found",
                                                open_market_ids.len()
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        eprintln!("forced quote poll failed: {err}");
                                    }
                                }
                            }
                            if let Err(e) = save_positions(positions_path, runner.position_manager()) {
                                eprintln!("persist positions failed: {}", e);
                                loop_outcome = LoopOutcome::Failure;
                            } else {
                                loop_outcome = if forced_exit_error {
                                    LoopOutcome::Failure
                                } else {
                                    LoopOutcome::Success
                                };
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
            yes_token_id: Some(c.yes_token_id.clone()),
            no_token_id: Some(c.no_token_id.clone()),
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

fn read_scan_top_n(dotenv_path: &Path, default_top_n: usize) -> usize {
    let Ok(raw) = std::fs::read_to_string(dotenv_path) else {
        return default_top_n.max(1);
    };
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        if k.trim() != "SCAN_TOP_N" {
            continue;
        }
        if let Ok(n) = v.trim().parse::<usize>() {
            return n.max(1);
        }
    }
    default_top_n.max(1)
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

fn parse_pm_side(raw: &str) -> Option<PmSide> {
    match raw.to_ascii_lowercase().as_str() {
        "buy" | "bid" | "b" => Some(PmSide::Buy),
        "sell" | "ask" | "s" => Some(PmSide::Sell),
        _ => None,
    }
}

fn read_pm_chain_id(dotenv_path: &Path, default_chain_id: u64) -> u64 {
    resolve_runtime_var("PM_CHAIN_ID", dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_chain_id)
}

fn resolve_pm_signature_type(dotenv_path: &Path) -> anyhow::Result<Option<PmSignatureType>> {
    let Some(raw) = resolve_runtime_var("PM_SIGNATURE_TYPE", dotenv_path) else {
        return Ok(None);
    };
    let normalized = raw.trim().to_ascii_lowercase();
    let value = match normalized.as_str() {
        "" => return Ok(None),
        "0" | "eoa" => PmSignatureType::Eoa,
        "1" | "proxy" => PmSignatureType::Proxy,
        "2" | "gnosis_safe" | "gnosissafe" | "safe" => PmSignatureType::GnosisSafe,
        _ => {
            return Err(anyhow!(
                "invalid PM_SIGNATURE_TYPE='{}' (supported: eoa|proxy|gnosis_safe or 0|1|2)",
                raw.trim()
            ));
        }
    };
    Ok(Some(value))
}

fn pm_signature_type_to_name(signature_type: PmSignatureType) -> &'static str {
    match signature_type {
        PmSignatureType::Eoa => "eoa",
        PmSignatureType::Proxy => "proxy",
        PmSignatureType::GnosisSafe => "gnosis_safe",
        _ => "unknown",
    }
}

fn resolve_pm_funder_address(
    dotenv_path: &Path,
    signature_type: Option<PmSignatureType>,
    chain_id: u64,
) -> anyhow::Result<Option<PmAddress>> {
    if let Some(raw) = resolve_runtime_var("PM_FUNDER", dotenv_path) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let parsed = PmAddress::from_str(trimmed)
                .with_context(|| format!("invalid PM_FUNDER address: {trimmed}"))?;
            return Ok(Some(parsed));
        }
    }
    let derived = match signature_type {
        Some(PmSignatureType::Proxy) => derive_pm_funder_from_private_key(dotenv_path, chain_id, true)?,
        Some(PmSignatureType::GnosisSafe) => {
            derive_pm_funder_from_private_key(dotenv_path, chain_id, false)?
        }
        _ => None,
    };
    Ok(derived)
}

fn derive_pm_funder_from_private_key(
    dotenv_path: &Path,
    chain_id: u64,
    is_proxy: bool,
) -> anyhow::Result<Option<PmAddress>> {
    let Some(private_key) = resolve_runtime_var(PRIVATE_KEY_VAR, dotenv_path)
        .or_else(|| resolve_runtime_var("PRIVATE_KEY", dotenv_path))
    else {
        return Ok(None);
    };
    let signer = PmLocalSigner::from_str(private_key.trim())
        .context("invalid private key format for PM_FUNDER derivation")?
        .with_chain_id(Some(chain_id));
    let derived = if is_proxy {
        derive_proxy_wallet(signer.address(), chain_id)
    } else {
        derive_safe_wallet(signer.address(), chain_id)
    };
    Ok(derived)
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
