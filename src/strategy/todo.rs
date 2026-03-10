use async_trait::async_trait;

use crate::strategy::{Strategy, StrategyError, StrategyId, StrategyStatus};
use crate::types::{MarketSnapshot, StrategyContext, StrategySignal};

pub struct TodoStrategy {
    id: StrategyId,
}

impl TodoStrategy {
    pub fn new(id: StrategyId) -> Self {
        Self { id }
    }
}

#[async_trait]
impl Strategy for TodoStrategy {
    fn id(&self) -> StrategyId {
        self.id.clone()
    }

    fn status(&self) -> StrategyStatus {
        StrategyStatus::Todo
    }

    fn validate_params(&self) -> Result<(), StrategyError> {
        Ok(())
    }

    async fn generate_signal(
        &self,
        _snapshot: &MarketSnapshot,
        _context: &StrategyContext,
    ) -> Result<Option<StrategySignal>, StrategyError> {
        Ok(None)
    }
}
