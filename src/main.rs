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
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{self, Duration};

use poly2::{
    run_healthcheck, scan_arb_candidates, AppConfig, ArbitrageStrategy,
    ExecutionClient, ExecutionReport, ExecutionSettings,
    MockExecutionClient, RetryingExecutionClient, RiskContext, RiskEngine,
    PolymarketSdkExecutionClient, StrategyContext, StrategyId, StrategyRegistry, TodoStrategy,
    TradingEngine, discover_btc_candle_market,
};

async fn fetch_clob_price(token_id: &str, side: &str) -> anyhow::Result<Decimal> {
    let url = format!(
        "https://clob.polymarket.com/price?token_id={}&side={}",
        token_id.trim(),
        side
    );
    let resp = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()?
        .get(url)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow::anyhow!("clob price failed: status={}", resp.status()));
    }
    let root: serde_json::Value = resp.json().await?;
    let raw = root
        .get("price")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("clob price missing 'price' field"))?;
    Ok(Decimal::from_str(raw.trim())?)
}

async fn fetch_clob_best_ask(token_id: &str) -> anyhow::Result<Decimal> {
    fetch_clob_price(token_id, "buy").await
}

async fn fetch_clob_best_bid(token_id: &str) -> anyhow::Result<Decimal> {
    fetch_clob_price(token_id, "sell").await
}

fn read_decimal_env_or(dotenv_path: &Path, key: &str, default_value: Decimal) -> Decimal {
    resolve_runtime_var(key, dotenv_path)
        .and_then(|v| Decimal::from_str(v.trim()).ok())
        .unwrap_or(default_value)
}

fn read_u64_env_or(dotenv_path: &Path, key: &str, default_value: u64) -> u64 {
    resolve_runtime_var(key, dotenv_path)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

async fn run_btc_candle_loop<C>(
    _interval_secs: u64,
    context: &StrategyContext,
    execution: &C,
    dotenv_path: &Path,
) where
    C: ExecutionClient,
{
    let order_usd = read_decimal_env_or(dotenv_path, "BTC_CANDLE_ORDER_USD", Decimal::from(2_u32));
    let entry_threshold = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_ENTRY_THRESHOLD",
        Decimal::from_str("0.72").unwrap_or(Decimal::from(72_u32) / Decimal::from(100_u32)),
    );
    let stop_loss_price = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_STOP_LOSS",
        Decimal::from_str("0.50").unwrap_or(Decimal::from(1_u32) / Decimal::from(2_u32)),
    );
    let take_profit_price = read_decimal_env_or(
        dotenv_path,
        "BTC_CANDLE_TAKE_PROFIT",
        Decimal::from_str("0.84").unwrap_or(Decimal::from(84_u32) / Decimal::from(100_u32)),
    );
    let poll_ms = read_u64_env_or(dotenv_path, "BTC_CANDLE_POLL_MS", 2000);

    println!(
        "btc_candle: order_usd={}, entry_threshold={}, stop_loss={}, take_profit={}, poll_ms={}",
        order_usd, entry_threshold, stop_loss_price, take_profit_price, poll_ms
    );

    let _ = context;
    let mut loop_idx: u64 = 0;
    loop {
        loop_idx += 1;

        // --- Phase 1: sleep until the NEXT 5-min window boundary ---
        let now_secs = Utc::now().timestamp();
        let current_window = now_secs - (now_secs.rem_euclid(300));
        let next_window = current_window + 300;
        let sleep_to_boundary = (next_window - now_secs).max(0) as u64;
        println!(
            "\n================ btc5m round #{} waiting for window ================\n\
             now={} next_window_ts={} sleeping={}s",
            loop_idx,
            Utc::now().to_rfc3339(),
            next_window,
            sleep_to_boundary
        );
        if sleep_to_boundary > 0 {
            time::sleep(Duration::from_secs(sleep_to_boundary)).await;
        }

        let window_ts = next_window;
        let close_ts = window_ts + 300;

        // --- Phase 2: discover the market for this window ---
        let market = match discover_btc_candle_market(window_ts).await {
            Ok(m) => m,
            Err(err) => {
                eprintln!("btc_candle: market discovery failed: {err}");
                time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        println!(
            "btc_candle: market_id={} slug={} up_token={} down_token={}",
            market.market_id, market.market_slug, market.up_token_id, market.down_token_id
        );

        // --- Phase 3: poll ask prices until one exceeds entry_threshold ---
        let mut bought_side: Option<poly2::Side> = None;
        let mut bought_token_id = String::new();
        let mut bought_size = Decimal::ZERO;

        'entry: loop {
            let now = Utc::now().timestamp();
            if now >= close_ts - 10 {
                println!("btc_candle: window closing soon, no entry this round");
                break 'entry;
            }

            let (up_ask, down_ask) = tokio::join!(
                fetch_clob_best_ask(&market.up_token_id),
                fetch_clob_best_ask(&market.down_token_id)
            );
            let up_val = up_ask.unwrap_or(Decimal::ZERO);
            let down_val = down_ask.unwrap_or(Decimal::ZERO);

            println!(
                "btc_candle poll: up_ask={} down_ask={} threshold={} remaining={}s",
                up_val, down_val, entry_threshold, close_ts - now
            );

            let (trigger_side, trigger_price, trigger_token) =
                if up_val >= entry_threshold && up_val >= down_val {
                    (poly2::Side::Yes, up_val, market.up_token_id.clone())
                } else if down_val >= entry_threshold {
                    (poly2::Side::No, down_val, market.down_token_id.clone())
                } else {
                    time::sleep(Duration::from_millis(poll_ms)).await;
                    continue 'entry;
                };

            if trigger_price <= Decimal::ZERO {
                time::sleep(Duration::from_millis(poll_ms)).await;
                continue 'entry;
            }
            let size = (order_usd / trigger_price).round_dp(2);
            println!(
                "btc_candle: ENTRY triggered! side={:?} price={} size={} (order_usd={})",
                trigger_side, trigger_price, size, order_usd
            );

            let signal = poly2::StrategySignal {
                strategy_id: StrategyId::ProbabilityTrading,
                market_id: market.market_id.clone(),
                yes_token_id: Some(market.up_token_id.clone()),
                no_token_id: Some(market.down_token_id.clone()),
                actions: vec![poly2::OrderIntent {
                    side: trigger_side.clone(),
                    price: trigger_price,
                    size,
                    sell: false,
                }],
                state: poly2::StrategyState::Implemented,
            };

            match execution.submit(&signal).await {
                Ok(report) => {
                    print_execution_report(&report);
                    bought_side = Some(trigger_side);
                    bought_token_id = trigger_token;
                    bought_size = size;
                }
                Err(err) => eprintln!("btc_candle: buy execution failed: {err}"),
            }
            break 'entry;
        }

        // --- Phase 4: if we bought, monitor for stop-loss / take-profit ---
        if let Some(ref exit_side) = bought_side {
            println!(
                "btc_candle: holding {:?}, monitoring stop_loss<={} / take_profit>={}",
                exit_side, stop_loss_price, take_profit_price
            );

            'exit: loop {
                let now = Utc::now().timestamp();
                if now >= close_ts - 5 {
                    println!("btc_candle: window closing, letting market resolve");
                    break 'exit;
                }

                let (up_ask_res, down_ask_res) = tokio::join!(
                    fetch_clob_best_ask(&market.up_token_id),
                    fetch_clob_best_ask(&market.down_token_id)
                );
                let up_val = up_ask_res.unwrap_or(Decimal::ZERO);
                let down_val = down_ask_res.unwrap_or(Decimal::ZERO);
                let current_ask = if bought_token_id == market.up_token_id {
                    up_val
                } else {
                    down_val
                };

                println!(
                    "btc_candle exit: up_ask={} down_ask={} held_price={} SL={} TP={} remaining={}s",
                    up_val, down_val, current_ask, stop_loss_price, take_profit_price, close_ts - now
                );

                let trigger = if current_ask <= stop_loss_price {
                    println!("btc_candle: STOP LOSS triggered at {}", current_ask);
                    true
                } else if current_ask >= take_profit_price {
                    println!("btc_candle: TAKE PROFIT triggered at {}", current_ask);
                    true
                } else {
                    false
                };

                if trigger {
                    let sell_price = fetch_clob_best_bid(&bought_token_id)
                        .await
                        .unwrap_or(current_ask);
                    let sell_price = if sell_price > Decimal::ZERO {
                        sell_price
                    } else {
                        current_ask
                    };

                    println!(
                        "btc_candle: SELL side={:?} price={} size={}",
                        exit_side, sell_price, bought_size
                    );

                    let signal = poly2::StrategySignal {
                        strategy_id: StrategyId::ProbabilityTrading,
                        market_id: market.market_id.clone(),
                        yes_token_id: Some(market.up_token_id.clone()),
                        no_token_id: Some(market.down_token_id.clone()),
                        actions: vec![poly2::OrderIntent {
                            side: exit_side.clone(),
                            price: sell_price,
                            size: bought_size,
                            sell: true,
                        }],
                        state: poly2::StrategyState::Implemented,
                    };

                    match execution.submit(&signal).await {
                        Ok(report) => print_execution_report(&report),
                        Err(err) => eprintln!("btc_candle: sell execution failed: {err}"),
                    }
                    break 'exit;
                }

                time::sleep(Duration::from_millis(poll_ms)).await;
            }
        }

        println!("btc_candle: round #{} complete", loop_idx);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::ring::default_provider(),
    );
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

    let _risk_engine = RiskEngine::new(app_config.risk_config()?);
    let _engine = TradingEngine::new(registry, _risk_engine);

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
        run_btc_candle_loop(fetch_interval_secs, &context, &retrying, dotenv_path).await;
    } else {
        println!("execution mode=mock (polymarket_http_url is empty)");
        let retrying = RetryingExecutionClient::new(
            MockExecutionClient::default(),
            execution_settings.max_retries,
            execution_settings.timeout_secs * 100,
        );
        run_btc_candle_loop(fetch_interval_secs, &context, &retrying, dotenv_path).await;
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
