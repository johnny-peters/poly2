use crate::engine::{EngineError, TradingEngine};
use crate::execution::{ExecutionClient, ExecutionReport, ExecutionStatus};
use crate::position::PositionManager;
use crate::strategy::StrategyId;
use crate::types::{MarketSnapshot, OrderIntent, StrategyContext, StrategySignal, StrategyState};
use crate::types::Side;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

/// (yes_bid, yes_ask, no_bid, no_ask) for a market.
pub type MarkQuad = (Decimal, Decimal, Decimal, Decimal);

/// Config for runner behaviour (dedup, cooldown, exit).
#[derive(Clone, Debug)]
pub struct RunnerConfig {
    /// Seconds to skip re-trading the same market after an order was sent (0 = disabled).
    pub trade_cooldown_secs: u64,
    /// Take-profit: close position when unrealized PnL % >= this (None = disabled).
    pub take_profit_pct: Option<Decimal>,
    /// Stop-loss: close position when unrealized PnL % <= -this (None = disabled).
    pub stop_loss_pct: Option<Decimal>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            trade_cooldown_secs: 300,
            take_profit_pct: None,
            stop_loss_pct: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RunSummary {
    pub processed_snapshots: usize,
    pub execution_reports: usize,
    pub realized_notional: Decimal,
    pub total_fees: Decimal,
    pub unrealized_pnl: Decimal,
    /// Number of snapshots skipped due to trade cooldown (same market recently traded).
    pub skipped_cooldown: usize,
}

pub struct EngineRunner<C> {
    engine: TradingEngine,
    execution: C,
    position_manager: PositionManager,
    config: RunnerConfig,
    /// market_id -> instant when we last sent an order (for cooldown dedup).
    recently_traded: HashMap<String, Instant>,
}

impl<C> EngineRunner<C>
where
    C: ExecutionClient,
{
    pub fn new(engine: TradingEngine, execution: C) -> Self {
        Self {
            engine,
            execution,
            position_manager: PositionManager::new(),
            config: RunnerConfig::default(),
            recently_traded: HashMap::new(),
        }
    }

    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.config = config;
        self
    }

    /// Use a pre-loaded position manager (e.g. from persistence).
    pub fn with_position_manager(mut self, position_manager: PositionManager) -> Self {
        self.position_manager = position_manager;
        self
    }

    /// Set trade cooldown in seconds (0 = disabled). Same market will not be traded again within this window.
    pub fn with_trade_cooldown_secs(mut self, secs: u64) -> Self {
        self.config.trade_cooldown_secs = secs;
        self
    }

    /// Set take-profit: close when unrealized PnL % >= this (None = disabled).
    pub fn with_take_profit_pct(mut self, pct: Option<Decimal>) -> Self {
        self.config.take_profit_pct = pct;
        self
    }

    /// Set stop-loss: close when unrealized PnL % <= -this (None = disabled).
    pub fn with_stop_loss_pct(mut self, pct: Option<Decimal>) -> Self {
        self.config.stop_loss_pct = pct;
        self
    }

    pub fn position_manager(&self) -> &PositionManager {
        &self.position_manager
    }

    fn should_skip_cooldown(&mut self, market_id: &str) -> bool {
        if self.config.trade_cooldown_secs == 0 {
            return false;
        }
        let now = Instant::now();
        let cooldown = std::time::Duration::from_secs(self.config.trade_cooldown_secs);
        if let Some(&t) = self.recently_traded.get(market_id) {
            if now.duration_since(t) < cooldown {
                return true;
            }
            self.recently_traded.remove(market_id);
        }
        false
    }

    fn record_traded(&mut self, report: &ExecutionReport) {
        if self.config.trade_cooldown_secs == 0 {
            return;
        }
        let traded = matches!(
            report.status,
            ExecutionStatus::Filled | ExecutionStatus::PartiallyFilled
        );
        if traded && !report.fills.is_empty() {
            self.recently_traded
                .insert(report.market_id.clone(), Instant::now());
        }
    }

    /// Build close (sell) signal for a position; submit and return reports if TP/SL triggered.
    async fn run_exit_checks(
        &mut self,
        marks: &HashMap<String, MarkQuad>,
    ) -> Result<Vec<ExecutionReport>, EngineError> {
        let take = self.config.take_profit_pct;
        let stop = self.config.stop_loss_pct;
        if take.is_none() && stop.is_none() {
            return Ok(Vec::new());
        }
        let mut reports = Vec::new();
        for (market_id, position) in self.position_manager.positions().clone() {
            let Some(&(yes_bid, _yes_ask, no_bid, _no_ask)) = marks.get(&market_id) else {
                continue;
            };
            let cost_basis = position.avg_entry_yes * position.qty_yes + position.avg_entry_no * position.qty_no;
            if cost_basis <= Decimal::ZERO {
                continue;
            }
            let unrealized = (yes_bid - position.avg_entry_yes) * position.qty_yes
                + (no_bid - position.avg_entry_no) * position.qty_no;
            let pnl_pct = unrealized / cost_basis;
            let trigger_tp = take.map_or(false, |t| pnl_pct >= t);
            let trigger_sl = stop.map_or(false, |s| pnl_pct <= -s);
            if !trigger_tp && !trigger_sl {
                continue;
            }
            let mut actions = Vec::new();
            if position.qty_yes > Decimal::ZERO {
                actions.push(OrderIntent {
                    side: Side::Yes,
                    price: yes_bid,
                    size: position.qty_yes,
                    sell: true,
                });
            }
            if position.qty_no > Decimal::ZERO {
                actions.push(OrderIntent {
                    side: Side::No,
                    price: no_bid,
                    size: position.qty_no,
                    sell: true,
                });
            }
            if actions.is_empty() {
                continue;
            }
            let signal = StrategySignal {
                strategy_id: StrategyId::PositionExit,
                market_id: market_id.clone(),
                actions,
                state: StrategyState::Implemented,
            };
            let report = self.execution.submit(&signal).await?;
            reports.push(report);
        }
        Ok(reports)
    }

    /// Run take-profit / stop-loss checks against provided marks and emit reports.
    pub async fn run_forced_exit_checks_with_report_hook<F>(
        &mut self,
        marks: &HashMap<String, MarkQuad>,
        mut on_report: F,
    ) -> Result<usize, EngineError>
    where
        F: FnMut(&ExecutionReport),
    {
        let reports = self.run_exit_checks(marks).await?;
        for report in &reports {
            on_report(report);
            self.record_traded(report);
            self.position_manager.apply_report(report);
        }
        Ok(reports.len())
    }

    fn marks_for_pnl(marks: &HashMap<String, MarkQuad>) -> HashMap<String, (Decimal, Decimal)> {
        marks
            .iter()
            .map(|(k, &(yb, _, nb, _))| (k.clone(), (yb, nb)))
            .collect()
    }

    pub async fn run_snapshots<I>(
        &mut self,
        snapshots: I,
        base_context: &StrategyContext,
    ) -> Result<RunSummary, EngineError>
    where
        I: IntoIterator<Item = MarketSnapshot>,
    {
        let mut summary = RunSummary::default();
        let mut latest_marks: HashMap<String, MarkQuad> = HashMap::new();
        for snapshot in snapshots {
            summary.processed_snapshots += 1;
            latest_marks.insert(
                snapshot.market_id.clone(),
                (
                    snapshot.yes_best_bid,
                    snapshot.yes_best_ask,
                    snapshot.no_best_bid,
                    snapshot.no_best_ask,
                ),
            );

            if self.should_skip_cooldown(&snapshot.market_id) {
                summary.skipped_cooldown += 1;
                continue;
            }

            let mut context = base_context.clone();
            context.risk.position_per_market = self.position_manager.positions().clone();
            let marks_for_pnl = Self::marks_for_pnl(&latest_marks);
            let pnl = self.position_manager.pnl_snapshot(&marks_for_pnl);
            context.risk.daily_pnl = pnl.realized_pnl + pnl.unrealized_pnl;

            let reports = self
                .engine
                .process_and_execute(&snapshot, &context, &self.execution)
                .await?;

            for report in &reports {
                self.record_traded(report);
                self.position_manager.apply_report(report);
            }
            summary.execution_reports += reports.len();
        }
        let pnl = self.position_manager.pnl_snapshot(&Self::marks_for_pnl(&latest_marks));
        summary.realized_notional = pnl.realized_notional;
        summary.total_fees = pnl.total_fees;
        summary.unrealized_pnl = pnl.unrealized_pnl;
        Ok(summary)
    }

    pub async fn run_snapshots_with_report_hook<I, F>(
        &mut self,
        snapshots: I,
        base_context: &StrategyContext,
        mut on_report: F,
    ) -> Result<RunSummary, EngineError>
    where
        I: IntoIterator<Item = MarketSnapshot>,
        F: FnMut(&ExecutionReport),
    {
        let mut summary = RunSummary::default();
        let mut latest_marks: HashMap<String, MarkQuad> = HashMap::new();
        for snapshot in snapshots {
            summary.processed_snapshots += 1;
            latest_marks.insert(
                snapshot.market_id.clone(),
                (
                    snapshot.yes_best_bid,
                    snapshot.yes_best_ask,
                    snapshot.no_best_bid,
                    snapshot.no_best_ask,
                ),
            );

            if self.should_skip_cooldown(&snapshot.market_id) {
                summary.skipped_cooldown += 1;
                continue;
            }

            let mut context = base_context.clone();
            context.risk.position_per_market = self.position_manager.positions().clone();
            let marks_for_pnl = Self::marks_for_pnl(&latest_marks);
            let pnl = self.position_manager.pnl_snapshot(&marks_for_pnl);
            context.risk.daily_pnl = pnl.realized_pnl + pnl.unrealized_pnl;

            let reports = self
                .engine
                .process_and_execute(&snapshot, &context, &self.execution)
                .await?;

            for report in &reports {
                on_report(report);
                self.record_traded(report);
                self.position_manager.apply_report(report);
            }
            summary.execution_reports += reports.len();
        }

        if let Ok(exit_report_count) = self
            .run_forced_exit_checks_with_report_hook(&latest_marks, |report| on_report(report))
            .await
        {
            summary.execution_reports += exit_report_count;
        }

        let pnl = self.position_manager.pnl_snapshot(&Self::marks_for_pnl(&latest_marks));
        summary.realized_notional = pnl.realized_notional;
        summary.total_fees = pnl.total_fees;
        summary.unrealized_pnl = pnl.unrealized_pnl;
        Ok(summary)
    }

    pub async fn run_snapshots_with_hooks<I, FR, FE>(
        &mut self,
        snapshots: I,
        base_context: &StrategyContext,
        mut on_report: FR,
        mut on_error: FE,
    ) -> Result<RunSummary, EngineError>
    where
        I: IntoIterator<Item = MarketSnapshot>,
        FR: FnMut(&ExecutionReport),
        FE: FnMut(&EngineError),
    {
        let mut summary = RunSummary::default();
        let mut latest_marks: HashMap<String, MarkQuad> = HashMap::new();
        for snapshot in snapshots {
            summary.processed_snapshots += 1;
            latest_marks.insert(
                snapshot.market_id.clone(),
                (
                    snapshot.yes_best_bid,
                    snapshot.yes_best_ask,
                    snapshot.no_best_bid,
                    snapshot.no_best_ask,
                ),
            );

            if self.should_skip_cooldown(&snapshot.market_id) {
                summary.skipped_cooldown += 1;
                continue;
            }

            let mut context = base_context.clone();
            context.risk.position_per_market = self.position_manager.positions().clone();
            let marks_for_pnl = Self::marks_for_pnl(&latest_marks);
            let pnl = self.position_manager.pnl_snapshot(&marks_for_pnl);
            context.risk.daily_pnl = pnl.realized_pnl + pnl.unrealized_pnl;

            let reports = match self
                .engine
                .process_and_execute(&snapshot, &context, &self.execution)
                .await
            {
                Ok(reports) => reports,
                Err(err) => {
                    on_error(&err);
                    return Err(err);
                }
            };

            for report in &reports {
                on_report(report);
                self.record_traded(report);
                self.position_manager.apply_report(report);
            }
            summary.execution_reports += reports.len();
        }
        let pnl = self.position_manager.pnl_snapshot(&Self::marks_for_pnl(&latest_marks));
        summary.realized_notional = pnl.realized_notional;
        summary.total_fees = pnl.total_fees;
        summary.unrealized_pnl = pnl.unrealized_pnl;
        Ok(summary)
    }

    pub async fn run_snapshots_with_replay<I, FR, FE>(
        &mut self,
        snapshots: I,
        base_context: &StrategyContext,
        max_replays: u32,
        mut on_report: FR,
        mut on_error: FE,
    ) -> Result<RunSummary, EngineError>
    where
        I: IntoIterator<Item = MarketSnapshot>,
        FR: FnMut(&ExecutionReport),
        FE: FnMut(&EngineError, u32),
    {
        let mut summary = RunSummary::default();
        let mut latest_marks: HashMap<String, MarkQuad> = HashMap::new();
        let mut queue: VecDeque<(MarketSnapshot, u32)> =
            snapshots.into_iter().map(|s| (s, 0)).collect();

        while let Some((snapshot, replay_count)) = queue.pop_front() {
            summary.processed_snapshots += 1;
            latest_marks.insert(
                snapshot.market_id.clone(),
                (
                    snapshot.yes_best_bid,
                    snapshot.yes_best_ask,
                    snapshot.no_best_bid,
                    snapshot.no_best_ask,
                ),
            );

            if self.should_skip_cooldown(&snapshot.market_id) {
                summary.skipped_cooldown += 1;
                continue;
            }

            let mut context = base_context.clone();
            context.risk.position_per_market = self.position_manager.positions().clone();
            let marks_for_pnl = Self::marks_for_pnl(&latest_marks);
            let pnl = self.position_manager.pnl_snapshot(&marks_for_pnl);
            context.risk.daily_pnl = pnl.realized_pnl + pnl.unrealized_pnl;

            match self
                .engine
                .process_and_execute(&snapshot, &context, &self.execution)
                .await
            {
                Ok(reports) => {
                    for report in &reports {
                        on_report(report);
                        self.record_traded(report);
                        self.position_manager.apply_report(report);
                    }
                    summary.execution_reports += reports.len();
                }
                Err(err) => {
                    on_error(&err, replay_count);
                    if replay_count < max_replays {
                        queue.push_back((snapshot, replay_count + 1));
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        let pnl = self.position_manager.pnl_snapshot(&Self::marks_for_pnl(&latest_marks));
        summary.realized_notional = pnl.realized_notional;
        summary.total_fees = pnl.total_fees;
        summary.unrealized_pnl = pnl.unrealized_pnl;
        Ok(summary)
    }
}

pub fn report_markets(reports: &[ExecutionReport]) -> Vec<String> {
    reports.iter().map(|r| r.market_id.clone()).collect()
}
