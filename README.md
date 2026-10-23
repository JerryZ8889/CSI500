# CSI500 量化决策看板

中证500 择时策略的数据更新、回测和 Streamlit 看板。策略规则、每个设计为什么这样定、踩过的坑，写在项目设计文档 `design.md` 里（只放在本地项目文件夹，不随公开仓库上传）。本文件只讲怎么跑起来。

线上看板：https://csi500.streamlit.app/

## 环境

- Python 3.11（GitHub Actions 和 Streamlit Cloud 用的版本；本地 3.9 以上也能跑看板）
- Tushare 账号的 token：只有更新数据、全量重建时需要，看板和回测不需要

## 本地运行（macOS）

在项目文件夹里执行。

建虚拟环境并装依赖：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

启动看板（浏览器打开 http://localhost:3000）：

```bash
.venv/bin/python -m streamlit run dashboard.py --server.port 3000
```

跑回测：

```bash
.venv/bin/python backtest.py
```

更新某一天的数据（不写日期就是北京时间今天）：

```bash
TUSHARE_TOKEN=你的token .venv/bin/python daily_update.py --date 20261022
```

从头重建 `strategy_data.csv`：

```bash
TUSHARE_TOKEN=你的token .venv/bin/python build_strategy_data.py
```

## 文件

| 文件 | 作用 |
|---|---|
| `dashboard.py` | 看板 |
| `strategy_engine.py` | 主观策略的买卖规则和门槛数字，看板和回测共用 |
| `tools/strategy_research_advanced.py`、`tools/strategy_meta_switch_research.py` | 量化策略（看板里的第二套策略）的规则和参数 |
| `backtest.py` | 回测，输出 K 线图和净值曲线 |
| `daily_update.py` | 每日增量更新 |
| `build_strategy_data.py` | 全量重建 |
| `tools/wake_streamlit.py` | 打开线上看板、睡着就叫醒 |
| `strategy_data.csv` | 策略每日输入总表 |
| `csi500_components_schedule.csv` | 每天的成分股名单 |
| `adj_factor_base.csv` | 复权基准表 |
| `update_status.json` | 最近一次数据更新的结果，看板顶部会显示 |
| `stocks_data/`、`stocks_archive/` | 现任 / 已调出成分股的日线 |
| `.streamlit/config.toml` | 看板固定浅色主题 |

## 安全检查工具

改策略相关代码时，用行为基线确认买卖结果没被意外改动。改代码之前先拍快照，改完再核对：

```bash
.venv/bin/python tools/behavior_baseline.py snapshot
.venv/bin/python tools/behavior_baseline.py verify
```

注意：基线连数据一起记录。两步之间如果数据更新过，`verify` 一定会报差异，所以两步要在同一份数据上做。

归档已调出成分股的文件（先 `plan` 看计划，`apply` 执行，`restore` 撤回）：

```bash
.venv/bin/python tools/archive_old_component_files.py plan
.venv/bin/python tools/archive_old_component_files.py apply
.venv/bin/python tools/archive_old_component_files.py restore
```

## 部署和自动任务

- **看板**：部署在 Streamlit Community Cloud，连着本仓库 `main` 分支，推送后一两分钟自动更新。如果改了 `strategy_engine.py` 或 `tools/` 下的文件，推送后要在看板右下角 Manage app → ⋮ → Reboot app 重启一次，否则线上会报 `ImportError`。
- **每日数据更新**：GitHub Actions `daily_update.yml`，工作日北京时间 17:30 定时（GitHub 常会推迟几个小时）。每次从数据表最后一天往后、按交易日历把没补的交易日全部补齐；当天行情还没发布就留到下次。需要在仓库 Settings → Secrets 里配置 `TUSHARE_TOKEN`。
- **补回表中间漏掉的日子**：GitHub Actions `backfill_missing_days.yml`，在 Actions 页面手动运行。不勾“写入”只预演，报告在运行页面的 Summary 里，也可以下载 `backfill-report`；确认后勾上“写入”再跑一次才会提交。看板顶部出现“数据有缺口”的黄色提示时用它。
- **看板保活**：GitHub Actions `streamlit_keepalive.yml`，每 6 小时用浏览器打开一次看板。看板睡着时也可以在 Actions 页面手动运行它。
- **打不开看板、提示 `source IP address not allowed`**：是代理节点的 IP 被 Streamlit 拦了，让 `streamlit.app` 和 `streamlit.io` 走直连即可。
