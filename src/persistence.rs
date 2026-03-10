//! Order and position persistence: append-only order log and position snapshot load/save.

use crate::execution::{ExecutionReport, ExecutionStatus};
use crate::position::PositionManager;
use crate::types::{Position, Side};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

/// One-line record for order log (JSONL).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderRecord {
    pub occurred_at: String,
    pub market_id: String,
    pub order_ids: Vec<String>,
    pub status: String,
    pub fills: Vec<FillRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FillRecord {
    pub order_id: Option<String>,
    pub fill_id: Option<String>,
    pub side: String,
    pub price: String,
    pub size: String,
    pub fee: String,
    pub sell: bool,
}

impl OrderRecord {
    pub fn from_report(report: &ExecutionReport) -> Self {
        Self {
            occurred_at: Utc::now().to_rfc3339(),
            market_id: report.market_id.clone(),
            order_ids: report.order_ids.clone(),
            status: status_to_string(&report.status),
            fills: report
                .fills
                .iter()
                .map(|f| FillRecord {
                    order_id: f.order_id.clone(),
                    fill_id: f.fill_id.clone(),
                    side: side_to_string(&f.side),
                    price: f.price.to_string(),
                    size: f.size.to_string(),
                    fee: f.fee.to_string(),
                    sell: f.sell,
                })
                .collect(),
        }
    }
}

fn status_to_string(s: &ExecutionStatus) -> String {
    match s {
        ExecutionStatus::Pending => "Pending",
        ExecutionStatus::PartiallyFilled => "PartiallyFilled",
        ExecutionStatus::Filled => "Filled",
        ExecutionStatus::Rejected => "Rejected",
    }
    .to_string()
}

fn side_to_string(s: &Side) -> String {
    match s {
        Side::Yes => "Yes",
        Side::No => "No",
    }
    .to_string()
}

/// Append one order record to the order log file (one JSON object per line).
pub fn append_order_record(log_path: &Path, report: &ExecutionReport) -> std::io::Result<()> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let record = OrderRecord::from_report(report);
    let line = serde_json::to_string(&record).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    writeln!(f, "{}", line)?;
    f.sync_all()?;
    Ok(())
}

/// Position snapshot for persistence (decimal as string for portability).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub positions: HashMap<String, PositionRow>,
    #[serde(default)]
    pub realized_pnl: String,
    pub realized_notional: String,
    pub total_fees: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PositionRow {
    pub market_id: String,
    pub qty_yes: String,
    pub qty_no: String,
    pub avg_entry_yes: String,
    pub avg_entry_no: String,
}

impl PositionSnapshot {
    pub fn from_manager(manager: &PositionManager) -> Self {
        let positions = manager
            .positions()
            .iter()
            .map(|(k, p)| {
                (
                    k.clone(),
                    PositionRow {
                        market_id: p.market_id.clone(),
                        qty_yes: p.qty_yes.to_string(),
                        qty_no: p.qty_no.to_string(),
                        avg_entry_yes: p.avg_entry_yes.to_string(),
                        avg_entry_no: p.avg_entry_no.to_string(),
                    },
                )
            })
            .collect();
        Self {
            positions,
            realized_pnl: manager.realized_pnl().to_string(),
            realized_notional: manager.realized_notional().to_string(),
            total_fees: manager.total_fees().to_string(),
        }
    }

    pub fn into_manager(self) -> Result<PositionManager, Box<dyn std::error::Error + Send + Sync>> {
        let mut positions = HashMap::new();
        for (_, row) in self.positions {
            positions.insert(
                row.market_id.clone(),
                Position {
                    market_id: row.market_id,
                    qty_yes: Decimal::from_str(&row.qty_yes)?,
                    qty_no: Decimal::from_str(&row.qty_no)?,
                    avg_entry_yes: Decimal::from_str(&row.avg_entry_yes)?,
                    avg_entry_no: Decimal::from_str(&row.avg_entry_no)?,
                },
            );
        }
        let realized_pnl = Decimal::from_str(&self.realized_pnl).unwrap_or(Decimal::ZERO);
        let realized_notional = Decimal::from_str(&self.realized_notional).unwrap_or(Decimal::ZERO);
        let total_fees = Decimal::from_str(&self.total_fees).unwrap_or(Decimal::ZERO);
        Ok(PositionManager::from_state(
            positions,
            realized_pnl,
            realized_notional,
            total_fees,
        ))
    }
}

/// Save position snapshot to a JSON file.
pub fn save_positions(path: &Path, manager: &PositionManager) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let snap = PositionSnapshot::from_manager(manager);
    let json = serde_json::to_string_pretty(&snap).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Load position snapshot from a JSON file; returns default PositionManager if file missing or invalid.
pub fn load_positions(path: &Path) -> PositionManager {
    let Ok(data) = std::fs::read_to_string(path) else {
        return PositionManager::new();
    };
    let Ok(snap) = serde_json::from_str::<PositionSnapshot>(&data) else {
        return PositionManager::new();
    };
    snap.into_manager().unwrap_or_else(|_| PositionManager::new())
}
