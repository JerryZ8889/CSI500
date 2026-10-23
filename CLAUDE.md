# CSI500 量化策略项目

## 设计依据

- 策略逻辑、设计取舍、踩过的坑，以本地项目文件夹里的 `design.md` 为唯一设计依据（它不随公开仓库上传）
- 买卖规则以代码为准：主观策略在 `strategy_engine.py`，量化策略在 `tools/strategy_meta_switch_research.py`（参数 `BASE_HYBRID`）和 `tools/strategy_research_advanced.py`
- 怎么运行、部署看 `README.md`
- 代码仓库：GitHub `JerryZ8889/CSI500`（早期叫 `CSI500-Quant-Live`）；线上看板 https://csi500.streamlit.app/

## 已确认的逻辑选择（2026-02-24）

### FirstNeg 退出条件
`is_below_ma AND (is_1d OR is_5d)`：跌破 MA30 是前提，再叠加当日收跌或 5 日滞涨才卖出。

### entry_high 记录方式
只在入场当日记录一次 `high`，持仓期间不滚动更新。

## 主观策略参数速查

门槛数字定义在 `strategy_engine.py` 顶部的常量里，看板文字和图表都引用它们；要改数字只改那里。

| 参数 | 值 | 用在哪 |
|------|----|--------|
| 广度冰点 | < 16% | Composite 买入 |
| 广度过热 | > 79% | 通用卖出（要同时满足下一行） |
| heat_z 放量线 | < 1.5σ | 通用卖出：广度过热且 heat_z 低于此线才卖 |
| MA30 | 30日 | FirstNeg 入场要求在其上方；FirstNeg 卖出以跌破它为前提 |
| MA10 / MA5 | 10日 / 5日 | FirstNeg 入场要求收盘在其上方 |
| 连涨要求 | ≥ 3天 | FirstNeg 入场 |
| ETF 换手率下限 | > 1.0% | FirstNeg 入场 |
| 时间止损 | 5天 | FirstNeg：持满 5 天收盘仍未高过入场当日最高价 |
| heat_z 窗口 | 20日 | 中证500成交额的 20 日 Z 分数 |
| 交易成本 | 0.1% | 买卖各一次 |
| 执行模型 | T+1 | 信号日次日开盘成交 |

广度 = 成分股中收盘价站上自己 20 日均线的比例。

## 量化策略参数速查（`BASE_HYBRID`）

| 层 | 开仓 | 平仓 |
|----|------|------|
| 抄底层 | 站上20日线占比 < 12% | 站上20日线占比 > 79% 且 heat_z < 2.0σ |
| 趋势层 | 收盘 > MA30，且站上60日线占比 ≥ 55%、站上20日线占比 ≥ 70% | 收盘 < MA30，或站上60日线占比 < 30%，或站上20日线占比 < 45% |

任一层开着就持仓。

## 数据文件
- `strategy_data.csv` — 策略每日输入总表
- `csi500_components_schedule.csv` — 每天的成分股名单
- `stocks_data/`、`stocks_archive/` — 现任 / 已调出成分股日线
- `daily_update.py` — 每日增量更新；`build_strategy_data.py` — 全量重建
