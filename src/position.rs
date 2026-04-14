use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::execution::{ExecutionReport, ExecutionStatus};
use crate::types::{Position, Side};

#[derive(Clone, Debug, Default)]
pub struct PositionPnl {
    pub realized_pnl: Decimal,
    pub realized_notional: Decimal,
    pub total_fees: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Clone, Debug, Default)]
pub struct PositionManager {
    positions: HashMap<String, Position>,
    realized_pnl: Decimal,
    realized_notional: Decimal,
    total_fees: Decimal,
}

impl PositionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore from explicit state (e.g. loaded from persistence).
    pub fn from_state(
        positions: HashMap<String, Position>,
        realized_pnl: Decimal,
        realized_notional: Decimal,
        total_fees: Decimal,
    ) -> Self {
        Self {
            positions,
            realized_pnl,
            realized_notional,
            total_fees,
        }
    }

    pub fn apply_report(&mut self, report: &ExecutionReport) {
        if matches!(report.status, ExecutionStatus::Rejected | ExecutionStatus::Pending) {
            return;
        }

        let entry = self
            .positions
            .entry(report.market_id.clone())
            .or_insert_with(|| Position {
                market_id: report.market_id.clone(),
                qty_yes: Decimal::ZERO,
                qty_no: Decimal::ZERO,
                avg_entry_yes: Decimal::ZERO,
                avg_entry_no: Decimal::ZERO,
            });

        for fill in &report.fills {
            if fill.sell {
                match fill.side {
                    Side::Yes => {
                        let close_qty = entry.qty_yes.min(fill.size).max(Decimal::ZERO);
                        if close_qty > Decimal::ZERO {
                            let fee_per_unit = if fill.size > Decimal::ZERO {
                                fill.fee / fill.size
                            } else {
                                Decimal::ZERO
                            };
                            self.realized_pnl +=
                                (fill.price - fee_per_unit - entry.avg_entry_yes) * close_qty;
                        }
                        entry.qty_yes = (entry.qty_yes - fill.size).max(Decimal::ZERO);
                        if entry.qty_yes <= Decimal::ZERO {
                            entry.avg_entry_yes = Decimal::ZERO;
                        }
                    }
                    Side::No => {
                        let close_qty = entry.qty_no.min(fill.size).max(Decimal::ZERO);
                        if close_qty > Decimal::ZERO {
                            let fee_per_unit = if fill.size > Decimal::ZERO {
                                fill.fee / fill.size
                            } else {
                                Decimal::ZERO
                            };
                            self.realized_pnl +=
                                (fill.price - fee_per_unit - entry.avg_entry_no) * close_qty;
                        }
                        entry.qty_no = (entry.qty_no - fill.size).max(Decimal::ZERO);
                        if entry.qty_no <= Decimal::ZERO {
                            entry.avg_entry_no = Decimal::ZERO;
                        }
                    }
                }
            } else {
                match fill.side {
                    Side::Yes => {
                        let fee_per_unit = if fill.size > Decimal::ZERO {
                            fill.fee / fill.size
                        } else {
                            Decimal::ZERO
                        };
                        entry.avg_entry_yes = weighted_avg(
                            entry.qty_yes,
                            entry.avg_entry_yes,
                            fill.size,
                            fill.price + fee_per_unit,
                        );
                        entry.qty_yes += fill.size;
                    }
                    Side::No => {
                        let fee_per_unit = if fill.size > Decimal::ZERO {
                            fill.fee / fill.size
                        } else {
                            Decimal::ZERO
                        };
                        entry.avg_entry_no = weighted_avg(
                            entry.qty_no,
                            entry.avg_entry_no,
                            fill.size,
                            fill.price + fee_per_unit,
                        );
                        entry.qty_no += fill.size;
                    }
                }
            }
            self.realized_notional += fill.price * fill.size;
            self.total_fees += fill.fee;
        }
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    /// Return market ids that still have open quantity on either side.
    pub fn open_market_ids(&self) -> Vec<String> {
        self.positions
            .iter()
            .filter_map(|(market_id, p)| {
                if p.qty_yes > Decimal::ZERO || p.qty_no > Decimal::ZERO {
                    Some(market_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn realized_notional(&self) -> Decimal {
        self.realized_notional
    }

    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    pub fn total_fees(&self) -> Decimal {
        self.total_fees
    }

    pub fn unrealized_pnl(&self, marks: &HashMap<String, (Decimal, Decimal)>) -> Decimal {
        self.positions
            .iter()
            .map(|(market_id, p)| {
                marks.get(market_id).map_or(Decimal::ZERO, |(yes_mark, no_mark)| {
                    (yes_mark - p.avg_entry_yes) * p.qty_yes + (no_mark - p.avg_entry_no) * p.qty_no
                })
            })
            .sum()
    }

    pub fn pnl_snapshot(&self, marks: &HashMap<String, (Decimal, Decimal)>) -> PositionPnl {
        PositionPnl {
            realized_pnl: self.realized_pnl,
            realized_notional: self.realized_notional,
            total_fees: self.total_fees,
            unrealized_pnl: self.unrealized_pnl(marks),
        }
    }
}

fn weighted_avg(old_qty: Decimal, old_avg: Decimal, add_qty: Decimal, add_price: Decimal) -> Decimal {
    if add_qty <= Decimal::ZERO {
        return old_avg;
    }
    let total_qty = old_qty + add_qty;
    if total_qty <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    ((old_avg * old_qty) + (add_price * add_qty)) / total_qty
}
