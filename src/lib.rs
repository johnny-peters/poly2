pub mod config;
pub mod engine;
pub mod execution;
pub mod healthcheck;
pub mod persistence;
pub mod position;
pub mod risk;
pub mod runner;
pub mod strategy;
pub mod types;

pub use config::{AppConfig, ExecutionSettings};
pub use engine::{EngineError, TradingEngine};
pub use execution::{
    EventingExecutionClient, ExecutionClient, ExecutionError, ExecutionEvent, ExecutionEventType,
    ExecutionFill, ExecutionReport, ExecutionStatus, FlakyExecutionClient, MockExecutionClient,
    OrderSignContext, PolymarketHttpExecutionClient, RetryingExecutionClient,
};
pub use persistence::{append_order_record, load_positions, save_positions, OrderRecord, PositionSnapshot};
pub use position::{PositionManager, PositionPnl};
pub use healthcheck::{
    fetch_market_snapshots_by_ids, run_healthcheck, scan_arb_candidates, ArbCandidate, CheckItem,
    HealthcheckReport,
};
pub use risk::{RiskConfig, RiskEngine};
pub use runner::{EngineRunner, RunSummary, RunnerConfig};
pub use strategy::{
    ArbitrageConfig, ArbitrageStrategy, Strategy, StrategyId, StrategyRegistry, StrategyStatus,
    TodoStrategy,
};
pub use types::{
    MarketSnapshot, OrderIntent, Position, RiskContext, Side, StrategyContext, StrategySignal,
    StrategyState,
};
