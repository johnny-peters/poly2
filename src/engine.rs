use crate::execution::{ExecutionClient, ExecutionError, ExecutionReport};
use crate::risk::RiskEngine;
use crate::strategy::{StrategyError, StrategyRegistry};
use crate::types::{MarketSnapshot, StrategyContext, StrategySignal};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Strategy(#[from] StrategyError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

pub struct TradingEngine {
    registry: StrategyRegistry,
    risk_engine: RiskEngine,
}

impl TradingEngine {
    pub fn new(registry: StrategyRegistry, risk_engine: RiskEngine) -> Self {
        Self {
            registry,
            risk_engine,
        }
    }

    pub async fn process_snapshot(
        &self,
        snapshot: &MarketSnapshot,
        context: &StrategyContext,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let signals = self.registry.active_signals(snapshot, context).await;
        let mut accepted = Vec::new();
        for signal in signals {
            let signal = signal?;
            if self.risk_engine.allow(&signal, context) {
                accepted.push(signal);
            }
        }
        Ok(accepted)
    }

    pub async fn process_and_execute(
        &self,
        snapshot: &MarketSnapshot,
        context: &StrategyContext,
        execution: &dyn ExecutionClient,
    ) -> Result<Vec<ExecutionReport>, EngineError> {
        let accepted = self.process_snapshot(snapshot, context).await?;
        let mut reports = Vec::with_capacity(accepted.len());
        for signal in accepted {
            reports.push(execution.submit(&signal).await?);
        }
        Ok(reports)
    }
}
