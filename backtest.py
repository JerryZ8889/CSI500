#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest.py — CSI500 量化策略回测
基于 策略Opus4.6.docx 确认的代码逻辑实现

输出：
  1. 510500 ETF 日K线图 + 买卖标记
  2. 策略净值 vs 基准净值对比曲线
  3. 总涨幅、最大回撤等统计
"""

import os
import subprocess
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd

from strategy_engine import compute_strategy_frame, compute_trade_summary, max_drawdown

# ── 配置 ──────────────────────────────────────────────────────────────────────
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(DATA_DIR, 'strategy_data.csv')
START_DATE = '20190101'
END_DATE = str(pd.read_csv(DATA_FILE, usecols=['trade_date'])['trade_date'].iloc[-1])
COST = 0.001  # 单次交易成本 0.1%

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1：加载数据 & 计算策略结果
# ═══════════════════════════════════════════════════════════════════════════════
df = pd.read_csv(DATA_FILE)
df['trade_date'] = df['trade_date'].astype(str)
df = df[(df['trade_date'] >= START_DATE) & (df['trade_date'] <= END_DATE)].copy()
df = compute_strategy_frame(df, cost=COST)

n = len(df)
signals = df['signal'].to_numpy()

print(f"数据加载完成：{len(df)} 行，{df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2：统计
# ═══════════════════════════════════════════════════════════════════════════════
trade_summary = compute_trade_summary(df)
n_trades = trade_summary['n_trades']
win_rate = trade_summary['win_rate']

strat_total = (df['strat_nav'].iloc[-1] - 1) * 100
bench_total = (df['bench_nav'].iloc[-1] - 1) * 100
strat_mdd = max_drawdown(df['strat_nav']) * 100
bench_mdd = max_drawdown(df['bench_nav']) * 100

print("\n" + "=" * 60)
print("CSI500 策略回测结果")
print("=" * 60)
print(f"回测区间：{df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")
print(f"交易天数：{len(df)}")
print(f"交易次数：{n_trades} 次（买入）")
print(f"胜    率：{win_rate:.1f}%")
print("-" * 60)
print(f"{'指标':<16} {'基准(买入持有)':<18} {'策略':<18}")
print("-" * 60)
print(f"{'总涨幅':<16} {bench_total:>+14.2f}%    {strat_total:>+14.2f}%")
print(f"{'最大回撤':<14} {bench_mdd:>+14.2f}%    {strat_mdd:>+14.2f}%")
print("=" * 60)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3：可视化
# ═══════════════════════════════════════════════════════════════════════════════
# 准备 mplfinance 数据格式
df_plot = df.set_index('date')[['open', 'high', 'low', 'close', 'volume']].copy()
df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

# 买卖标记（标注在 T+1 执行日）
buy_exec_dates = []
sell_exec_dates = []
for i in range(n):
    if signals[i] == 1 and i + 1 < n:
        buy_exec_dates.append(df['date'].iloc[i + 1])
    elif signals[i] == -1 and i + 1 < n:
        sell_exec_dates.append(df['date'].iloc[i + 1])

# 为 mplfinance 构建标记序列
buy_markers = pd.Series(np.nan, index=df_plot.index)
sell_markers = pd.Series(np.nan, index=df_plot.index)
for d in buy_exec_dates:
    if d in buy_markers.index:
        buy_markers[d] = df_plot.loc[d, 'Low'] * 0.985
for d in sell_exec_dates:
    if d in sell_markers.index:
        sell_markers[d] = df_plot.loc[d, 'High'] * 1.015

# 自定义 mplfinance 样式
mc = mpf.make_marketcolors(
    up='#ef5350', down='#26a69a',  # 中国习惯：红涨绿跌
    edge='inherit', wick='inherit',
    volume={'up': '#ef5350', 'down': '#26a69a'}
)
style = mpf.make_mpf_style(
    marketcolors=mc, gridstyle=':', gridcolor='#e0e0e0',
    rc={'font.sans-serif': ['SimHei'], 'axes.unicode_minus': False}
)

# 构建附加图（仅 K线 + 买卖标记 + 成交量，不含净值曲线）
add_plots = [
    mpf.make_addplot(buy_markers, type='scatter', marker='^',
                     markersize=60, color='#ef5350', panel=0),
    mpf.make_addplot(sell_markers, type='scatter', marker='v',
                     markersize=60, color='#26a69a', panel=0),
]

# 绘制 K线图
fig_kline, _ = mpf.plot(
    df_plot,
    type='candle',
    style=style,
    addplot=add_plots,
    volume=True,
    panel_ratios=(5, 1.5),
    figsize=(36, 14),
    title='\nCSI500 K线 & 策略回测',
    returnfig=True,
    tight_layout=True,
    warn_too_much_data=2000,
)

# 保存 K线图
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
kline_path = os.path.join(DATA_DIR, f'backtest_kline_{ts}.png')
fig_kline.savefig(kline_path, dpi=120, bbox_inches='tight')
plt.close(fig_kline)
print(f"\nK线图已保存至：{kline_path}")
subprocess.Popen(['start', '', kline_path], shell=True)

# ── 净值对比曲线（独立 matplotlib 窗口，共享同一 Y 轴）──────────────────────
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

stats_text = (
    f"回测区间: {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}\n"
    f"交易次数: {n_trades}次  |  胜率: {win_rate:.1f}%\n"
    f"策略总涨幅: {strat_total:+.2f}%  |  最大回撤: {strat_mdd:.2f}%\n"
    f"基准总涨幅: {bench_total:+.2f}%  |  最大回撤: {bench_mdd:.2f}%"
)

fig_nav, ax = plt.subplots(figsize=(20, 8))
ax.plot(df['date'], df['strat_nav'], color='#ff6f00', linewidth=1.5, label='策略净值')
ax.plot(df['date'], df['bench_nav'], color='#1565c0', linewidth=1.0, linestyle='--', label='基准净值(买入持有)')
ax.axhline(y=1.0, color='gray', linestyle=':', alpha=0.5)
ax.set_ylabel('净值', fontsize=12)
ax.set_title('策略净值 vs 基准净值', fontsize=14)
ax.legend(loc='upper left', fontsize=12)
ax.text(0.98, 0.95, stats_text, transform=ax.transAxes, fontsize=10,
        verticalalignment='top', horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig_nav.autofmt_xdate(rotation=45)
plt.tight_layout()
print(f"strat_nav 起点: {df['strat_nav'].iloc[0]:.4f}，bench_nav 起点: {df['bench_nav'].iloc[0]:.4f}")
plt.show()
