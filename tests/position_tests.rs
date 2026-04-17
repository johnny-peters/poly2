use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

use poly2::{
    ExecutionFill, ExecutionReport, ExecutionStatus, PositionManager, Side, StrategyId,
    StrategySignal, StrategyState,
};

fn sample_signal() -> StrategySignal {
    StrategySignal {
        strategy_id: StrategyId::Arbitrage,
        market_id: "pm-1".to_string(),
        yes_token_id: None,
        no_token_id: None,
        actions: vec![],
        state: StrategyState::Implemented,
    }
}

#[test]
fn position_manager_updates_from_fills() {
    let mut pm = PositionManager::new();
    let report = ExecutionReport {
        market_id: "pm-1".to_string(),
        action_count: 2,
        order_ids: vec!["oid-1".to_string(), "oid-2".to_string()],
        status: ExecutionStatus::Filled,
        fills: vec![
            ExecutionFill {
                order_id: Some("oid-1".to_string()),
                fill_id: Some("fid-1".to_string()),
                side: Side::Yes,
                price: Decimal::from_str("0.48").expect("invalid decimal"),
                size: Decimal::from_str("10").expect("invalid decimal"),
                fee: Decimal::from_str("0.01").expect("invalid decimal"),
                sell: false,
            },
            ExecutionFill {
                order_id: Some("oid-2".to_string()),
                fill_id: Some("fid-2".to_string()),
                side: Side::No,
                price: Decimal::from_str("0.50").expect("invalid decimal"),
                size: Decimal::from_str("10").expect("invalid decimal"),
                fee: Decimal::from_str("0.01").expect("invalid decimal"),
                sell: false,
            },
        ],
        signal: sample_signal(),
    };

    pm.apply_report(&report);
    let p = pm
        .positions()
        .get("pm-1")
        .expect("position for pm-1 should exist");
    assert_eq!(p.qty_yes, Decimal::from_str("10").expect("invalid decimal"));
    assert_eq!(p.qty_no, Decimal::from_str("10").expect("invalid decimal"));
    assert_eq!(
        pm.realized_notional(),
        Decimal::from_str("9.8").expect("invalid decimal")
    );
    assert_eq!(
        pm.total_fees(),
        Decimal::from_str("0.02").expect("invalid decimal")
    );
    assert!(p.avg_entry_yes > Decimal::from_str("0.48").expect("invalid decimal"));
}

#[test]
fn pnl_snapshot_contains_unrealized_component() {
    let mut pm = PositionManager::new();
    let report = ExecutionReport {
        market_id: "pm-3".to_string(),
        action_count: 2,
        order_ids: vec!["oid-3".to_string(), "oid-4".to_string()],
        status: ExecutionStatus::Filled,
        fills: vec![
            ExecutionFill {
                order_id: Some("oid-3".to_string()),
                fill_id: Some("fid-3".to_string()),
                side: Side::Yes,
                price: Decimal::from_str("0.40").expect("invalid decimal"),
                size: Decimal::from_str("5").expect("invalid decimal"),
                fee: Decimal::ZERO,
                sell: false,
            },
            ExecutionFill {
                order_id: Some("oid-4".to_string()),
                fill_id: Some("fid-4".to_string()),
                side: Side::No,
                price: Decimal::from_str("0.60").expect("invalid decimal"),
                size: Decimal::from_str("5").expect("invalid decimal"),
                fee: Decimal::ZERO,
                sell: false,
            },
        ],
        signal: sample_signal(),
    };
    pm.apply_report(&report);

    let mut marks = HashMap::new();
    marks.insert(
        "pm-3".to_string(),
        (
            Decimal::from_str("0.45").expect("invalid decimal"),
            Decimal::from_str("0.65").expect("invalid decimal"),
        ),
    );
    let snap = pm.pnl_snapshot(&marks);
    assert!(snap.unrealized_pnl > Decimal::ZERO);
}

#[test]
fn pending_report_does_not_change_positions() {
    let mut pm = PositionManager::new();
    let report = ExecutionReport {
        market_id: "pm-2".to_string(),
        action_count: 1,
        order_ids: vec!["oid-5".to_string()],
        status: ExecutionStatus::Pending,
        fills: vec![ExecutionFill {
            order_id: Some("oid-5".to_string()),
            fill_id: Some("fid-5".to_string()),
            side: Side::Yes,
            price: Decimal::from_str("0.40").expect("invalid decimal"),
            size: Decimal::from_str("5").expect("invalid decimal"),
            fee: Decimal::ZERO,
            sell: false,
        }],
        signal: sample_signal(),
    };

    pm.apply_report(&report);
    assert!(pm.positions().is_empty());
}
