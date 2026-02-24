# CSI500 量化策略项目

## 策略设计依据
- 编写代码时以 `策略Opus4.6.docx` 作为策略逻辑的唯一设计依据
- 该文档基于 GitHub 仓库 `JerryZ8889/CSI500-Quant-Live` 的 app.py 代码逻辑生成
- 原始参考文档为 `策略逻辑 .docx`，但存在两处与代码不一致的地方

## 已确认的逻辑选择（2026-02-24）

### 差异 1：FirstNeg 退出条件 → 采用代码逻辑
`is_below_ma AND (is_1d OR is_5d)`：跌破 MA_Filter 是前提，再叠加当日下跌或5日滞涨才触发卖出。

### 差异 2：entry_high 记录方式 → 采用代码逻辑
仅在入场当日记录一次 `high` 值，持仓期间不滚动更新。

## 策略核心参数速查
| 参数 | 默认值 |
|------|--------|
| MA_Filter | 30日 |
| MA_Trend | 10日 |
| MA_Support | 5日 |
| Heat_Z 窗口 | 20日 |
| 广度冰点 | < 16% |
| 广度过热 | > 79% |
| Heat_Z 过热 | > 1.5σ |
| 连涨要求 | ≥ 3天 |
| ETF换手率下限 | 1.0% |
| 时间止损 | 5天 |
| 交易成本 | 0.1% |
| 执行模型 | T+1 |

## 数据文件
- `stocks_data/` — 用户准备的股票数据
- `strategy_data.csv` — 策略数据
- `build_strategy_data.py` — 构建策略数据的脚本
- `csi500_components_schedule.csv` — 成分股调度表
