use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use poly2::{
    ArbitrageConfig, ArbitrageStrategy, EngineRunner, FlakyExecutionClient, MarketSnapshot,
    RiskConfig, RiskContext, RiskEngine, RetryingExecutionClient, StrategyContext, StrategyRegistry,
    TradingEngine,
};

fn build_snapshot() -> MarketSnapshot {
    MarketSnapshot {
        market_id: "runner-m1".to_string(),
        yes_token_id: Some("runner-m1-yes".to_string()),
        no_token_id: Some("runner-m1-no".to_string()),
        yes_best_bid: Decimal::from_str("0.49").expect("invalid decimal"),
        yes_best_ask: Decimal::from_str("0.48").expect("invalid decimal"),
        no_best_bid: Decimal::from_str("0.50").expect("invalid decimal"),
        no_best_ask: Decimal::from_str("0.50").expect("invalid decimal"),
        volume_24h: Some(Decimal::from_str("1000").expect("invalid decimal")),
        timestamp: Utc::now(),
    }
}

fn build_context() -> StrategyContext {
    StrategyContext {
        risk: RiskContext {
            total_capital: Decimal::from_str("10000").expect("invalid decimal"),
            position_per_market: HashMap::new(),
            daily_pnl: Decimal::ZERO,
        },
        external_probability: None,
    }
}

fn build_engine() -> TradingEngine {
    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(ArbitrageStrategy::new(ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.001").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("10").expect("invalid decimal"),
    ))));

    TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02").expect("invalid decimal"),
            max_single_trade_pct: Decimal::from_str("0.03").expect("invalid decimal"),
            daily_loss_limit_pct: Decimal::from_str("0.05").expect("invalid decimal"),
        }),
    )
}

#[tokio::test]
async fn retrying_execution_client_retries_and_succeeds() {
    let flaky = FlakyExecutionClient::new(2);
    let retrying = RetryingExecutionClient::new(flaky.clone(), 3, 1);
    let mut runner = EngineRunner::new(build_engine(), retrying);

    let summary = runner
        .run_snapshots(vec![build_snapshot()], &build_context())
        .await
        .expect("runner should succeed");

    assert_eq!(summary.processed_snapshots, 1);
    assert_eq!(summary.execution_reports, 1);
    assert!(summary.realized_notional > Decimal::ZERO);
    assert_eq!(summary.total_fees, Decimal::ZERO);
    assert_eq!(flaky.attempts(), 3);
}

#[tokio::test]
async fn runner_updates_position_manager_after_execution() {
    let flaky = FlakyExecutionClient::new(0);
    let retrying = RetryingExecutionClient::new(flaky, 0, 1);
    let mut runner = EngineRunner::new(build_engine(), retrying);

    let _ = runner
        .run_snapshots(vec![build_snapshot()], &build_context())
        .await
        .expect("runner should succeed");

    let pos = runner
        .position_manager()
        .positions()
        .get("runner-m1")
        .expect("position should exist");
    assert!(pos.qty_yes > Decimal::ZERO);
    assert!(pos.qty_no > Decimal::ZERO);
}

#[tokio::test]
async fn runner_report_hook_receives_execution_reports() {
    let flaky = FlakyExecutionClient::new(0);
    let retrying = RetryingExecutionClient::new(flaky, 0, 1);
    let mut runner = EngineRunner::new(build_engine(), retrying);

    let mut seen_reports = 0usize;
    let summary = runner
        .run_snapshots_with_report_hook(vec![build_snapshot()], &build_context(), |_report| {
            seen_reports += 1;
        })
        .await
        .expect("runner should succeed");

    assert_eq!(seen_reports, 1);
    assert_eq!(summary.execution_reports, 1);
}

#[tokio::test]
async fn runner_error_hook_receives_engine_error() {
    let flaky = FlakyExecutionClient::new(1);
    let retrying = RetryingExecutionClient::new(flaky, 0, 1);
    let mut runner = EngineRunner::new(build_engine(), retrying);

    let mut seen_reports = 0usize;
    let mut seen_errors = 0usize;
    let result = runner
        .run_snapshots_with_hooks(
            vec![build_snapshot()],
            &build_context(),
            |_report| {
                seen_reports += 1;
            },
            |_err| {
                seen_errors += 1;
            },
        )
        .await;

    assert!(result.is_err());
    assert_eq!(seen_reports, 0);
    assert_eq!(seen_errors, 1);
}

#[tokio::test]
async fn runner_replay_queue_retries_failed_snapshot() {
    let flaky = FlakyExecutionClient::new(1);
    let retrying = RetryingExecutionClient::new(flaky, 0, 1);
    let mut runner = EngineRunner::new(build_engine(), retrying);

    let mut seen_reports = 0usize;
    let mut seen_errors = 0usize;
    let summary = runner
        .run_snapshots_with_replay(
            vec![build_snapshot()],
            &build_context(),
            1,
            |_report| {
                seen_reports += 1;
            },
            |_err, _replay_count| {
                seen_errors += 1;
            },
        )
        .await
        .expect("replay should recover after one failure");

    assert_eq!(seen_errors, 1);
    assert_eq!(seen_reports, 1);
    assert_eq!(summary.execution_reports, 1);
}
