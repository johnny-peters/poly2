use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;

use crate::strategy::{Strategy, StrategyError, StrategyId, StrategyStatus};
use crate::types::{MarketSnapshot, StrategyContext, StrategySignal};

#[derive(Default)]
pub struct StrategyRegistry {
    strategies: HashMap<StrategyId, Arc<dyn Strategy>>,
}

impl StrategyRegistry {
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
        }
    }

    pub fn register(&mut self, strategy: Arc<dyn Strategy>) {
        self.strategies.insert(strategy.id(), strategy);
    }

    pub fn statuses(&self) -> HashMap<StrategyId, StrategyStatus> {
        self.strategies
            .iter()
            .map(|(k, v)| (k.clone(), v.status()))
            .collect()
    }

    pub async fn active_signals(
        &self,
        snapshot: &MarketSnapshot,
        ctx: &StrategyContext,
    ) -> Vec<Result<StrategySignal, StrategyError>> {
        let mut futures = self
            .strategies
            .values()
            .filter(|s| matches!(s.status(), StrategyStatus::Implemented))
            .map(|s| s.generate_signal(snapshot, ctx))
            .collect::<FuturesUnordered<_>>();

        let mut out = Vec::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok(Some(signal)) => out.push(Ok(signal)),
                Ok(None) => {}
                Err(err) => out.push(Err(err)),
            }
        }
        out
    }
}
