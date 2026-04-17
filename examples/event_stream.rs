use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use poly2::{
    ArbitrageConfig, ArbitrageStrategy, EngineRunner, EventingExecutionClient, MarketSnapshot,
    MockExecutionClient, RiskConfig, RiskContext, RiskEngine, StrategyContext, StrategyRegistry,
    TradingEngine,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut registry = StrategyRegistry::new();
    registry.register(Arc::new(ArbitrageStrategy::new(ArbitrageConfig::simple(
        Decimal::from_str("0.005")?,
        Decimal::from_str("0.001")?,
        Decimal::from_str("100")?,
        Decimal::from_str("10")?,
    ))));

    let engine = TradingEngine::new(
        registry,
        RiskEngine::new(RiskConfig {
            max_position_pct: Decimal::from_str("0.02")?,
            max_single_trade_pct: Decimal::from_str("0.03")?,
            daily_loss_limit_pct: Decimal::from_str("0.05")?,
        }),
    );

    let eventing_exec = EventingExecutionClient::new(MockExecutionClient::default(), 64);
    let mut rx = eventing_exec.subscribe();

    let watcher = tokio::spawn(async move {
        for _ in 0..3 {
            if let Ok(evt) = rx.recv().await {
                println!(
                    "[event#{} @{}] type={:?} market={} order_id={:?} status={:?} fill={} error={:?}",
                    evt.event_id,
                    evt.occurred_at,
                    evt.event_type,
                    evt.market_id,
                    evt.order_id,
                    evt.status,
                    evt.fill.is_some(),
                    evt.error
                );
            }
        }
    });

    let snapshot = MarketSnapshot {
        market_id: "event-market".to_string(),
        yes_token_id: Some("event-market-yes".to_string()),
        no_token_id: Some("event-market-no".to_string()),
        yes_best_bid: Decimal::from_str("0.49")?,
        yes_best_ask: Decimal::from_str("0.48")?,
        no_best_bid: Decimal::from_str("0.50")?,
        no_best_ask: Decimal::from_str("0.50")?,
        volume_24h: Some(Decimal::from_str("1000")?),
        timestamp: Utc::now(),
    };
    let context = StrategyContext {
        risk: RiskContext {
            total_capital: Decimal::from_str("10000")?,
            position_per_market: HashMap::new(),
            daily_pnl: Decimal::ZERO,
        },
        external_probability: None,
    };

    let mut runner = EngineRunner::new(engine, eventing_exec);
    let summary = runner.run_snapshots(vec![snapshot], &context).await?;
    println!(
        "summary: processed={} reports={} realized={} fees={} unrealized={}",
        summary.processed_snapshots,
        summary.execution_reports,
        summary.realized_notional,
        summary.total_fees,
        summary.unrealized_pnl
    );

    watcher.await?;
    Ok(())
}
