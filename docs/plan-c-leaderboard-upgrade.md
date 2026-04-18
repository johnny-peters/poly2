# Plan C Leaderboard 筛选机制升级计划

本文件为已批准实施计划的副本，用于仓库内文档归档。实现以代码与 `docs/plan-c-review-after.md` 自评为准。

## 一、目标

把快速打分表各维度抬升到 **B+ 或以上**，覆盖：候选池获取、分数函数、bot 过滤、样本量控制、刷新节奏与下单规模适配。

## 二、总体流程

1. 拉榜：WEEK PNL + WEEK VOLUME + MONTH PNL（可配置）
2. 取交集/并集、去重、限额
3. 硬门槛：pnl / vol / realized / 集中度
4. 并行 trades + 分析（含 DD、Sharpe、notional CV 等）
5. FIFO（含回撤、可选 neg-risk 处理路径）
6. Bot 过滤 v2
7. Wilson LCB + PF shrinkage + min_closed
8. Score v2（含 DD 惩罚）
9. 同 `condition_id` 去相关
10. 事件驱动 refresh + 弹性 `order_usd`

## 三、环境变量

见 `.env` 样例与 `run_plan_c_loop` 启动日志；计划中的默认值见实现代码中的 `read_*_env_or(..., default)`。

## 四、验收

- `cargo test` 通过（含 `plan_c` 单元测试）
- 见 [plan-c-review-after.md](./plan-c-review-after.md)
