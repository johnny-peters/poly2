mod arbitrage;
mod registry;
mod todo;

pub use arbitrage::{ArbitrageConfig, ArbitrageStrategy};
pub use registry::StrategyRegistry;
pub use todo::TodoStrategy;

use async_trait::async_trait;
use thiserror::Error;

use crate::types::{MarketSnapshot, StrategyContext, StrategySignal};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum StrategyId {
    Arbitrage,
    ProbabilityTrading,
    MarketMaking,
    LatencyArbitrage,
    /// Exit from position (take-profit / stop-loss).
    PositionExit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyStatus {
    Implemented,
    Todo,
}

#[derive(Debug, Error)]
pub enum StrategyError {
    #[error("invalid strategy parameters: {0}")]
    InvalidParameters(String),
    #[error("runtime strategy failure: {0}")]
    Runtime(String),
}

#[async_trait]
pub trait Strategy: Send + Sync {
    fn id(&self) -> StrategyId;
    fn status(&self) -> StrategyStatus;
    fn validate_params(&self) -> Result<(), StrategyError>;

    async fn generate_signal(
        &self,
        snapshot: &MarketSnapshot,
        context: &StrategyContext,
    ) -> Result<Option<StrategySignal>, StrategyError>;
}
