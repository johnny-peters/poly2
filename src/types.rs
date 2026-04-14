use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Side {
    Yes,
    No,
}

#[derive(Clone, Debug)]
pub struct OrderIntent {
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    /// True = sell (close) order; false = buy (open) order.
    #[allow(clippy::struct_field_names)]
    pub sell: bool,
}

#[derive(Clone, Debug)]
pub enum StrategyState {
    Implemented,
    Todo,
}

#[derive(Clone, Debug)]
pub struct StrategySignal {
    pub strategy_id: crate::strategy::StrategyId,
    pub market_id: String,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub actions: Vec<OrderIntent>,
    pub state: StrategyState,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    pub market_id: String,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub yes_best_bid: Decimal,
    pub yes_best_ask: Decimal,
    pub no_best_bid: Decimal,
    pub no_best_ask: Decimal,
    pub volume_24h: Option<Decimal>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub market_id: String,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub qty_yes: Decimal,
    pub qty_no: Decimal,
    pub avg_entry_yes: Decimal,
    pub avg_entry_no: Decimal,
}

#[derive(Clone, Debug)]
pub struct RiskContext {
    pub total_capital: Decimal,
    pub position_per_market: HashMap<String, Position>,
    pub daily_pnl: Decimal,
}

#[derive(Clone, Debug)]
pub struct StrategyContext {
    pub risk: RiskContext,
    pub external_probability: Option<Decimal>,
}
