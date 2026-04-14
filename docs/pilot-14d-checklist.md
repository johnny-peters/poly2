# poly2 14 天小额实盘验证清单

目标：用统一口径验证 `poly2` 在真实摩擦成本下是否具备持续正边际。

## Day 0（启动前）

- [ ] `cargo run -- healthcheck --strict` 通过。
- [ ] `TRADING_ENABLED=true`。
- [ ] 设置 `ORDER_LOG_PATH`，确保订单 JSONL 持续落盘。
- [ ] 设置 `POSITIONS_PATH`，确保持仓快照可恢复。
- [ ] 仅启用 `arbitrage`，其余策略保持关闭。
- [ ] 校准保守风险参数：
  - [ ] `max_position_pct`
  - [ ] `max_single_trade_pct`
  - [ ] `daily_loss_limit_pct`

## Day 1 ~ Day 14（日常执行）

- [ ] 启动交易：`cargo run`
- [ ] 记录当天关键运行日志：
  - [ ] `order_report`
  - [ ] `order_summary`
  - [ ] 失败事件（scan / execute / forced_exit）
- [ ] 日终填写 `docs/templates/daily-profit-report.csv`：
  - [ ] `gross_pnl`
  - [ ] `fees`
  - [ ] `slippage_cost`
  - [ ] `failure_cost`
  - [ ] `net_pnl`
  - [ ] `daily_net_return`
  - [ ] `max_drawdown_to_date`
  - [ ] `report_count` / `filled_or_partial_count`

可选半自动命令（建议）：

```bash
python scripts/generate_daily_profit_report.py
```

默认值：

- `--order-log-path data/orders.jsonl`
- `--positions-path data/positions.json`
- `--start-capital 100`

如需覆盖可继续传参，例如：

```bash
python scripts/generate_daily_profit_report.py --start-capital 300 --date 2026-04-14
```

## 风险刹车（任一命中即执行）

- [ ] 连续 3 天 `daily_net_return <= 0`：暂停并复盘参数/执行质量。
- [ ] 7 天滚动中位日收益 `<= 0`：判定策略边际不足。
- [ ] 执行成功率异常走低：优先修复执行链路而非继续放量。

## Day 14 结论模板

- [ ] Go：
  - [ ] 14 天中至少 10 天净收益为正。
  - [ ] 7 天滚动中位日收益 > 0。
  - [ ] 最大回撤处于可承受区间。
- [ ] No-Go：
  - [ ] 净收益不稳定，或成本持续吞噬边际。
  - [ ] 执行失败率高、成交质量不足。

## 结果归档

- [ ] 保存 14 天完整日报（CSV）。
- [ ] 保存关键日志与配置快照。
- [ ] 输出最终审查结论（是否继续投入/是否扩展策略）。
