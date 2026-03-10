use rust_decimal::Decimal;

use crate::types::{StrategyContext, StrategySignal};

#[derive(Clone, Debug)]
pub struct RiskConfig {
    pub max_position_pct: Decimal,
    pub max_single_trade_pct: Decimal,
    pub daily_loss_limit_pct: Decimal,
}

pub struct RiskEngine {
    config: RiskConfig,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Self {
        Self { config }
    }

    pub fn allow(&self, signal: &StrategySignal, context: &StrategyContext) -> bool {
        if context.risk.total_capital <= Decimal::ZERO {
            return false;
        }

        let loss_limit_abs = context.risk.total_capital * self.config.daily_loss_limit_pct;
        if context.risk.daily_pnl <= -loss_limit_abs {
            return false;
        }

        let max_single_trade_notional = context.risk.total_capital * self.config.max_single_trade_pct;
        let signal_notional = signal
            .actions
            .iter()
            .map(|a| a.price * a.size)
            .sum::<Decimal>();
        if signal_notional > max_single_trade_notional {
            return false;
        }

        let max_position_notional = context.risk.total_capital * self.config.max_position_pct;
        let current_market_position = context
            .risk
            .position_per_market
            .get(&signal.market_id)
            .map(|p| (p.qty_yes.abs() * p.avg_entry_yes) + (p.qty_no.abs() * p.avg_entry_no))
            .unwrap_or(Decimal::ZERO);

        current_market_position + signal_notional <= max_position_notional
    }

    pub fn config(&self) -> &RiskConfig {
        &self.config
    }
}
