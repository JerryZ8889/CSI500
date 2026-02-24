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

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import os

# ── 配置 ─────────────────────────────────────────────────────────────────────
DATA_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_FILE   = os.path.join(DATA_DIR, 'strategy_data.csv')
START_DATE  = '20190101'
END_DATE    = str(pd.read_csv(DATA_FILE, usecols=['trade_date'])['trade_date'].iloc[-1])
COST        = 0.001        # 单次交易成本 0.1%

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1：加载数据 & 计算派生字段
# ═══════════════════════════════════════════════════════════════════════════════
df = pd.read_csv(DATA_FILE)
df['trade_date'] = df['trade_date'].astype(str)
df = df[(df['trade_date'] >= START_DATE) & (df['trade_date'] <= END_DATE)].copy()
df = df.sort_values('trade_date').reset_index(drop=True)
df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

# 连续上涨天数 (Consec_Gains)
df['is_up'] = (df['close'] > df['close'].shift(1)).astype(int)
# 分组标记：每次涨跌切换时组号+1
df['streak_group'] = (df['is_up'] != df['is_up'].shift(1)).cumsum()
df['streak'] = df.groupby('streak_group').cumcount() + 1
df['consec_gains'] = np.where(df['is_up'] == 1, df['streak'], 0)


print(f"数据加载完成：{len(df)} 行，{df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2：策略信号生成
# ═══════════════════════════════════════════════════════════════════════════════
n = len(df)
signals    = np.zeros(n, dtype=int)       # 0=无信号, 1=买入, -1=卖出
position   = 0                             # 0=空仓, 1=持仓
logic_state = None                         # None / 'Composite' / 'FirstNeg'
entry_idx  = 0
entry_high = 0.0

for i in range(1, n):
    curr_c = df['close'].iloc[i]
    prev_c = df['close'].iloc[i - 1]
    ma_f   = df['ma_30'].iloc[i]       # MA_Filter
    ma_t   = df['ma_10'].iloc[i]       # MA_Trend
    ma_s   = df['ma_5'].iloc[i]        # MA_Support
    brd    = df['breadth'].iloc[i]
    hz     = df['heat_z'].iloc[i]
    turn   = df['etf_turnover'].iloc[i]
    prev_cg = df['consec_gains'].iloc[i - 1]  # 昨日连涨天数

    if position == 0:
        # ── 买入检测 ──
        # 场景 A：Composite（左侧极值抄底）
        if brd < 16:
            signals[i] = 1
            logic_state = 'Composite'
            entry_idx = i
            entry_high = df['high'].iloc[i]
            position = 1
        # 场景 B：FirstNeg（右侧首阴低吸）
        elif (curr_c > ma_t and              # 收盘 > MA10
              prev_cg >= 3 and               # 昨日连涨 >= 3天
              curr_c < prev_c and            # 今日首阴
              turn > 1.0 and                 # 换手率 > 1%
              curr_c > ma_s and              # 收盘 > MA5
              curr_c > ma_f):                # 收盘 > MA30
            signals[i] = 1
            logic_state = 'FirstNeg'
            entry_idx = i
            entry_high = df['high'].iloc[i]
            position = 1
    else:
        # ── 持仓中：状态升级检测 ──
        if logic_state == 'FirstNeg' and brd < 16:
            logic_state = 'Composite'

        # ── 卖出检测 ──
        # 通用退出：广度过热 + 资金降温
        cond_common = (brd > 79) and (hz < 1.5)

        if cond_common:
            signals[i] = -1
            position = 0
            logic_state = None
        elif logic_state == 'FirstNeg':
            is_1d = (curr_c < prev_c)
            is_below_ma = (curr_c < ma_f)
            # 持仓天数 >= 5 且期间 close 从未超过 entry_high
            held_days = i - entry_idx
            if held_days >= 5:
                closes_in_period = df['close'].iloc[entry_idx:i + 1]
                is_5d = not (closes_in_period > entry_high).any()
            else:
                is_5d = False

            if is_below_ma and (is_1d or is_5d):
                signals[i] = -1
                position = 0
                logic_state = None

df['signal'] = signals

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3：T+1 执行 & 收益计算
# ═══════════════════════════════════════════════════════════════════════════════
# signal[i] 在第 i 天收盘时产生，第 i+1 天 open 执行
# actual_pos[i] = 第 i 天收盘时是否持仓（考虑 T+1 延迟）
actual_pos = np.zeros(n, dtype=int)
pos = 0
for i in range(n):
    if i > 0 and signals[i - 1] == 1:
        pos = 1
    elif i > 0 and signals[i - 1] == -1:
        pos = 0
    actual_pos[i] = pos

df['actual_pos'] = actual_pos

# 每日策略收益
strat_ret = np.zeros(n)
for i in range(1, n):
    if actual_pos[i] == 1 and actual_pos[i - 1] == 0:
        # 买入执行日：以 open 买入，持有到 close
        strat_ret[i] = (df['close'].iloc[i] / df['open'].iloc[i] - 1) - COST
    elif actual_pos[i] == 0 and actual_pos[i - 1] == 1:
        # 卖出执行日：以前日 close 持有到今日 open 卖出
        strat_ret[i] = (df['open'].iloc[i] / df['close'].iloc[i - 1] - 1) - COST
    elif actual_pos[i] == 1 and actual_pos[i - 1] == 1:
        # 正常持仓日
        strat_ret[i] = df['close'].iloc[i] / df['close'].iloc[i - 1] - 1
    # else: 空仓，收益 = 0

df['strat_ret'] = strat_ret
df['strat_nav'] = (1 + df['strat_ret']).cumprod()
df['strat_nav'] /= df['strat_nav'].iloc[0]   # 确保从 1.0 出发

# 基准收益（买入持有）
df['bench_ret'] = df['close'].pct_change().fillna(0)
df['bench_nav'] = (1 + df['bench_ret']).cumprod()
df['bench_nav'] /= df['bench_nav'].iloc[0]   # 确保从 1.0 出发

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4：统计
# ═══════════════════════════════════════════════════════════════════════════════
def max_drawdown(nav_series):
    peak = nav_series.cummax()
    dd = (nav_series - peak) / peak
    return dd.min()

# 交易记录
buy_signals  = df[df['signal'] == 1].copy()
sell_signals = df[df['signal'] == -1].copy()
n_trades = len(buy_signals)

# 胜率（每笔交易的盈亏）
trade_returns = []
buy_dates  = df.index[df['signal'] == 1].tolist()
sell_dates = df.index[df['signal'] == -1].tolist()
for bi, si in zip(buy_dates, sell_dates[:len(buy_dates)]):
    # 从信号日+1（执行日）到卖出信号日+1（执行日）的累计收益
    exec_buy  = min(bi + 1, n - 1)
    exec_sell = min(si + 1, n - 1)
    trade_nav = (1 + df['strat_ret'].iloc[exec_buy:exec_sell + 1]).prod() - 1
    trade_returns.append(trade_nav)

win_rate = np.mean([r > 0 for r in trade_returns]) * 100 if trade_returns else 0

strat_total  = (df['strat_nav'].iloc[-1] - 1) * 100
bench_total  = (df['bench_nav'].iloc[-1] - 1) * 100
strat_mdd    = max_drawdown(df['strat_nav']) * 100
bench_mdd    = max_drawdown(df['bench_nav']) * 100

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
# STEP 5：可视化
# ═══════════════════════════════════════════════════════════════════════════════
# 准备 mplfinance 数据格式
df_plot = df.set_index('date')[['open', 'high', 'low', 'close', 'volume']].copy()
df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

# 买卖标记（标注在 T+1 执行日）
buy_exec_dates  = []
sell_exec_dates = []
for i in range(n):
    if signals[i] == 1 and i + 1 < n:
        buy_exec_dates.append(df['date'].iloc[i + 1])
    elif signals[i] == -1 and i + 1 < n:
        sell_exec_dates.append(df['date'].iloc[i + 1])

# 为 mplfinance 构建标记序列
buy_markers  = pd.Series(np.nan, index=df_plot.index)
sell_markers = pd.Series(np.nan, index=df_plot.index)
for d in buy_exec_dates:
    if d in buy_markers.index:
        buy_markers[d] = df_plot.loc[d, 'Low'] * 0.985
for d in sell_exec_dates:
    if d in sell_markers.index:
        sell_markers[d] = df_plot.loc[d, 'High'] * 1.015

# 自定义 mplfinance 样式
mc = mpf.make_marketcolors(
    up='#ef5350', down='#26a69a',       # 中国习惯：红涨绿跌
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
import subprocess
from datetime import datetime
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
