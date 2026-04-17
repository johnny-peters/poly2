use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use poly2::{
    ArbitrageConfig, ArbitrageStrategy, MarketSnapshot, MockExecutionClient, Position, RiskConfig,
    RiskContext, RiskEngine, StrategyContext, StrategyId, StrategyRegistry, TodoStrategy,
    TradingEngine, ExecutionStatus, PolymarketHttpExecutionClient, ExecutionClient, Strategy,
};

fn snapshot(yes_ask: &str, no_ask: &str, volume_24h: &str) -> MarketSnapshot {
    MarketSnapshot {
        market_id: "m1".to_string(),
        yes_token_id: Some("m1-yes".to_string()),
        no_token_id: Some("m1-no".to_string()),
        yes_best_bid: Decimal::from_str("0.49").expect("invalid decimal"),
        yes_best_ask: Decimal::from_str(yes_ask).expect("invalid decimal"),
        no_best_bid: Decimal::from_str("0.49").expect("invalid decimal"),
        no_best_ask: Decimal::from_str(no_ask).expect("invalid decimal"),
        volume_24h: Some(Decimal::from_str(volume_24h).expect("invalid decimal")),
        timestamp: Utc::now(),
    }
}

fn context(total_capital: &str, daily_pnl: &str) -> StrategyContext {
    StrategyContext {
        risk: RiskContext {
            total_capital: Decimal::from_str(total_capital).expect("invalid decimal"),
            position_per_market: HashMap::new(),
            daily_pnl: Decimal::from_str(daily_pnl).expect("invalid decimal"),
        },
        external_probability: None,
    }
}

#[tokio::test]
async fn arbitrage_triggers_when_effective_sum_below_threshold() {
    let cfg = ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.002").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("10").expect("invalid decimal"),
    );
    let strategy = ArbitrageStrategy::new(cfg);

    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(strategy));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::MarketMaking)));

    let engine = TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02").expect("invalid decimal"),
            max_single_trade_pct: Decimal::from_str("0.03").expect("invalid decimal"),
            daily_loss_limit_pct: Decimal::from_str("0.05").expect("invalid decimal"),
        }),
    );

    let accepted = engine
        .process_snapshot(
            &snapshot("0.48", "0.50", "1000"),
            &context("10000", "0"),
        )
        .await
        .expect("engine should not fail");

    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].actions.len(), 2);
}

#[tokio::test]
async fn todo_strategies_are_not_dispatched() {
    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(TodoStrategy::new(StrategyId::ProbabilityTrading)));
    registry.register(Arc::new(TodoStrategy::new(StrategyId::MarketMaking)));

    let signals = registry
        .active_signals(
            &snapshot("0.40", "0.40", "1000"),
            &context("10000", "0"),
        )
        .await;

    assert!(signals.is_empty());
}

#[tokio::test]
async fn risk_engine_blocks_large_notional_signal() {
    let cfg = ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.001").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("1000").expect("invalid decimal"),
    );

    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(ArbitrageStrategy::new(cfg)));

    let engine = TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02").expect("invalid decimal"),
            max_single_trade_pct: Decimal::from_str("0.005").expect("invalid decimal"),
            daily_loss_limit_pct: Decimal::from_str("0.05").expect("invalid decimal"),
        }),
    );

    let accepted = engine
        .process_snapshot(
            &snapshot("0.45", "0.45", "1000"),
            &context("10000", "0"),
        )
        .await
        .expect("engine should not fail");

    assert!(accepted.is_empty());
}

#[tokio::test]
async fn risk_engine_blocks_when_market_position_hits_max_position_pct() {
    let cfg = ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.001").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("10").expect("invalid decimal"),
    );

    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(ArbitrageStrategy::new(cfg)));

    let engine = TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02").expect("invalid decimal"),
            max_single_trade_pct: Decimal::from_str("0.03").expect("invalid decimal"),
            daily_loss_limit_pct: Decimal::from_str("0.05").expect("invalid decimal"),
        }),
    );

    let mut positions = HashMap::new();
    positions.insert(
        "m1".to_string(),
        Position {
            market_id: "m1".to_string(),
            yes_token_id: None,
            no_token_id: None,
            qty_yes: Decimal::from_str("390").expect("invalid decimal"),
            qty_no: Decimal::ZERO,
            avg_entry_yes: Decimal::from_str("0.50").expect("invalid decimal"),
            avg_entry_no: Decimal::ZERO,
        },
    );
    let ctx = StrategyContext {
        risk: RiskContext {
            total_capital: Decimal::from_str("10000").expect("invalid decimal"),
            position_per_market: positions,
            daily_pnl: Decimal::ZERO,
        },
        external_probability: None,
    };

    let accepted = engine
        .process_snapshot(&snapshot("0.48", "0.50", "1000"), &ctx)
        .await
        .expect("engine should not fail");

    assert!(accepted.is_empty());
}

#[tokio::test]
async fn process_and_execute_submits_to_execution_client() {
    let cfg = ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.001").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("10").expect("invalid decimal"),
    );
    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(ArbitrageStrategy::new(cfg)));

    let engine = TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02").expect("invalid decimal"),
            max_single_trade_pct: Decimal::from_str("0.03").expect("invalid decimal"),
            daily_loss_limit_pct: Decimal::from_str("0.05").expect("invalid decimal"),
        }),
    );
    let exec = MockExecutionClient::default();

    let reports = engine
        .process_and_execute(
            &snapshot("0.48", "0.50", "1000"),
            &context("10000", "0"),
            &exec,
        )
        .await
        .expect("execution should succeed");

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].order_ids.len(), 0);
    assert_eq!(reports[0].status, ExecutionStatus::Filled);
    assert_eq!(reports[0].fills.len(), 2);
    assert_eq!(exec.submitted().len(), 1);
}

#[tokio::test]
async fn polymarket_http_client_returns_network_error_without_endpoint() {
    let client = PolymarketHttpExecutionClient::new("http://127.0.0.1:9")
        .with_per_order_retry_policy(1, 1)
        .with_signature_provider(|ctx| {
            Some(format!(
                "sig:{}:{}:{}:{}:{}",
                ctx.market_id, ctx.side, ctx.timestamp, ctx.nonce, ctx.strategy_id
            ))
        });
    let cfg = ArbitrageConfig::simple(
        Decimal::from_str("0.005").expect("invalid decimal"),
        Decimal::from_str("0.001").expect("invalid decimal"),
        Decimal::from_str("100").expect("invalid decimal"),
        Decimal::from_str("10").expect("invalid decimal"),
    );
    let signal = ArbitrageStrategy::new(cfg)
        .generate_signal(&snapshot("0.48", "0.50", "1000"), &context("10000", "0"))
        .await
        .expect("strategy should not fail")
        .expect("signal should exist");

    let err = client
        .submit(&signal)
        .await
        .expect_err("http client should fail without exchange endpoint");
    let msg = err.to_string();
    assert!(
        msg.contains("http request failed") || msg.contains("exchange rejected order"),
        "unexpected error message: {msg}"
    );
}
