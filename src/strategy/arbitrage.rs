use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::strategy::{Strategy, StrategyError, StrategyId, StrategyStatus};
use crate::types::{OrderIntent, Side, StrategyContext, StrategySignal, StrategyState};
use crate::MarketSnapshot;

#[derive(Clone, Debug)]
pub struct ArbitrageConfig {
    pub min_profit_threshold: Decimal,
    pub total_cost_buffer: Decimal,
    pub min_liquidity: Decimal,
    pub order_size: Decimal,
}

impl ArbitrageConfig {
    pub fn simple(
        min_profit_threshold: Decimal,
        total_cost_buffer: Decimal,
        min_liquidity: Decimal,
        order_size: Decimal,
    ) -> Self {
        Self {
            min_profit_threshold,
            total_cost_buffer,
            min_liquidity,
            order_size,
        }
    }
}

pub struct ArbitrageStrategy {
    pub config: ArbitrageConfig,
}

impl ArbitrageStrategy {
    pub fn new(config: ArbitrageConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Strategy for ArbitrageStrategy {
    fn id(&self) -> StrategyId {
        StrategyId::Arbitrage
    }

    fn status(&self) -> StrategyStatus {
        StrategyStatus::Implemented
    }

    fn validate_params(&self) -> Result<(), StrategyError> {
        if self.config.min_profit_threshold < Decimal::ZERO {
            return Err(StrategyError::InvalidParameters(
                "min_profit_threshold must be >= 0".to_string(),
            ));
        }
        if self.config.total_cost_buffer < Decimal::ZERO {
            return Err(StrategyError::InvalidParameters(
                "total_cost_buffer must be >= 0".to_string(),
            ));
        }
        if self.config.order_size <= Decimal::ZERO {
            return Err(StrategyError::InvalidParameters(
                "order_size must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    async fn generate_signal(
        &self,
        snapshot: &MarketSnapshot,
        _context: &StrategyContext,
    ) -> Result<Option<StrategySignal>, StrategyError> {
        self.validate_params()?;

        if let Some(v) = snapshot.volume_24h {
            if v < self.config.min_liquidity {
                return Ok(None);
            }
        }

        let effective_sum = snapshot.yes_best_ask + snapshot.no_best_ask + self.config.total_cost_buffer;
        let trigger_threshold = Decimal::ONE - self.config.min_profit_threshold;

        if effective_sum >= trigger_threshold {
            return Ok(None);
        }

        let actions = vec![
            OrderIntent {
                side: Side::Yes,
                price: snapshot.yes_best_ask,
                size: self.config.order_size,
                sell: false,
            },
            OrderIntent {
                side: Side::No,
                price: snapshot.no_best_ask,
                size: self.config.order_size,
                sell: false,
            },
        ];

        Ok(Some(StrategySignal {
            strategy_id: StrategyId::Arbitrage,
            market_id: snapshot.market_id.clone(),
            actions,
            state: StrategyState::Implemented,
        }))
    }
}
