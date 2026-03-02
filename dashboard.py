#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dashboard.py — CSI500 量化实战决策中心 (Streamlit)
基于 strategy_data.csv + backtest.py 策略引擎
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import json

# ═══════════════════════════════════════════════════════════════════════════════
# 页面配置
# ═══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="CSI500 量化决策中心",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS 样式 ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* 隐藏侧边栏 & 全局 */
    [data-testid="collapsedControl"] { display: none; }
    .main { background-color: #f0f2f6; }
    .block-container { max-width: 1200px; }

    /* 页面顶部彩条 */
    .top-bar {
        background: linear-gradient(90deg, #1e3a5f 0%, #2d5a87 100%);
        color: white; padding: 18px 24px; border-radius: 12px;
        margin-bottom: 20px;
    }
    .top-bar h1 { margin: 0 0 4px 0; font-size: 1.6rem; }
    .top-bar p  { margin: 0; font-size: 0.85rem; opacity: 0.9; }

    /* KPI 卡片 */
    div[data-testid="metric-container"] {
        background: #ffffff;
        border: none;
        padding: 18px 16px;
        border-radius: 12px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    div[data-testid="metric-container"] label {
        font-size: 0.82rem !important; color: #6b7280 !important;
    }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 1.5rem !important; font-weight: 700 !important;
    }

    /* 分割线 */
    hr { margin: 1.8rem 0; border-color: #e5e7eb; }

    /* Section 标题 */
    .section-head {
        border-left: 4px solid #1e3a5f; padding-left: 12px;
        font-size: 1.15rem; font-weight: 700; margin: 24px 0 12px 0;
    }

    /* 状态卡片 */
    .status-card {
        color: white; padding: 20px 14px; border-radius: 12px;
        text-align: center; font-size: 1.25rem; font-weight: 700;
        box-shadow: 0 2px 8px rgba(0,0,0,0.12);
    }
    .status-desc {
        text-align: center; color: #6b7280; font-size: 0.85rem;
        margin: 6px 0 14px 0;
    }

    /* 理由/风险区块 */
    .reason-block {
        background: #ffffff; border-radius: 10px; padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05); margin: 10px 0;
    }

    /* Expander 美化 */
    .streamlit-expanderHeader {
        font-weight: 600 !important; font-size: 0.95rem !important;
    }

    /* 自定义 metric 网格：PC 四列，手机两列 */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 12px; margin: 8px 0 16px 0;
    }
    .metric-item {
        background: #ffffff; border-radius: 12px; padding: 16px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06); text-align: center;
    }
    .metric-item .label {
        font-size: 0.8rem; color: #6b7280; margin-bottom: 4px;
    }
    .metric-item .value {
        font-size: 1.4rem; font-weight: 700; color: #111827;
    }

    /* 参考提示卡片（虚拟仓位/加仓建议） */
    .ref-tip-card {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        color: #92400e;
        padding: 14px 14px;
        border-radius: 10px;
        text-align: center;
        font-size: 1.05rem;
        font-weight: 600;
        border: 1px dashed #f59e0b;
        margin-top: 10px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .ref-tip-exit-card {
        background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
        color: #991b1b;
        padding: 14px 14px;
        border-radius: 10px;
        text-align: center;
        font-size: 1.05rem;
        font-weight: 600;
        border: 1px dashed #ef4444;
        margin-top: 10px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .ref-tip-desc {
        text-align: center;
        color: #78716c;
        font-size: 0.78rem;
        margin: 4px 0 0 0;
        opacity: 0.85;
    }
    .ref-tip-disclaimer {
        text-align: center;
        color: #b45309;
        font-size: 0.7rem;
        margin: 2px 0 10px 0;
        font-style: italic;
    }

    /* 移动端 */
    @media (max-width: 768px) {
        .block-container { padding: 0.5rem 0.8rem !important; }
        .top-bar { padding: 14px 16px; border-radius: 8px; }
        .top-bar h1 { font-size: 1.2rem; }
        .status-card { font-size: 1rem; padding: 14px 10px; }
        h1 { font-size: 1.3rem !important; }
        h3 { font-size: 1rem !important; }

        /* metric 网格：手机变 2x2 */
        .metric-grid { grid-template-columns: repeat(2, 1fr); gap: 8px; }
        .metric-item { padding: 12px 8px; }
        .metric-item .value { font-size: 1.1rem; }
        .metric-item .label { font-size: 0.75rem; }
        .ref-tip-card, .ref-tip-exit-card { font-size: 0.9rem; padding: 10px 8px; }
        .ref-tip-desc { font-size: 0.72rem; }
        .ref-tip-disclaimer { font-size: 0.65rem; }
    }
</style>
""", unsafe_allow_html=True)

# ── 全局 matplotlib 中文字体 ──────────────────────────────────────────────────
import matplotlib.font_manager as _fm
_available = {f.name for f in _fm.fontManager.ttflist}
for _font in ['SimHei', 'WenQuanYi Zen Hei', 'Microsoft YaHei']:
    if _font in _available:
        plt.rcParams['font.sans-serif'] = [_font]
        break
plt.rcParams['axes.unicode_minus'] = False

# ═══════════════════════════════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════════════════════════════
DATA_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_FILE  = os.path.join(DATA_DIR, 'strategy_data.csv')
START_DATE = '20240101'
END_DATE   = str(pd.read_csv(DATA_FILE, usecols=['trade_date'])['trade_date'].iloc[-1])
COST       = 0.001

# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载 & 策略引擎
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def load_and_compute(end_date):
    df = pd.read_csv(DATA_FILE)
    df['trade_date'] = df['trade_date'].astype(str)
    df = df[(df['trade_date'] >= START_DATE) & (df['trade_date'] <= end_date)].copy()
    df = df.sort_values('trade_date').reset_index(drop=True)
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

    # ── 连续上涨天数 ──
    df['is_up'] = (df['close'] > df['close'].shift(1)).astype(int)
    df['streak_group'] = (df['is_up'] != df['is_up'].shift(1)).cumsum()
    df['streak'] = df.groupby('streak_group').cumcount() + 1
    df['consec_gains'] = np.where(df['is_up'] == 1, df['streak'], 0)

    n = len(df)

    # ── 策略信号生成 ──
    signals     = np.zeros(n, dtype=int)
    position    = 0
    logic_state = None
    entry_idx   = 0
    entry_high  = 0.0
    logic_states = [None] * n   # 记录每日逻辑状态
    exit_reasons = [None] * n   # 'overheating' | 'trend_break' | 'time_stop'
    entry_highs  = [np.nan] * n
    entry_idxs   = [np.nan] * n

    for i in range(1, n):
        curr_c  = df['close'].iloc[i]
        prev_c  = df['close'].iloc[i - 1]
        ma_f    = df['ma_30'].iloc[i]
        ma_t    = df['ma_10'].iloc[i]
        ma_s    = df['ma_5'].iloc[i]
        brd     = df['breadth'].iloc[i]
        hz      = df['heat_z'].iloc[i]
        turn    = df['etf_turnover'].iloc[i]
        prev_cg = df['consec_gains'].iloc[i - 1]

        if position == 0:
            if brd < 16:
                signals[i] = 1
                logic_state = 'Composite'
                entry_idx = i
                entry_high = df['high'].iloc[i]
                position = 1
            elif (curr_c > ma_t and prev_cg >= 3 and curr_c < prev_c and
                  turn > 1.0 and curr_c > ma_s and curr_c > ma_f):
                signals[i] = 1
                logic_state = 'FirstNeg'
                entry_idx = i
                entry_high = df['high'].iloc[i]
                position = 1
        else:
            if logic_state == 'FirstNeg' and brd < 16:
                logic_state = 'Composite'

            cond_common = (brd > 79) and (hz < 1.5)
            if cond_common:
                signals[i] = -1
                exit_reasons[i] = 'overheating'
                position = 0
                logic_state = None
            elif logic_state == 'FirstNeg':
                is_1d = (curr_c < prev_c)
                is_below_ma = (curr_c < ma_f)
                held_days = i - entry_idx
                if held_days >= 5:
                    closes_in_period = df['close'].iloc[entry_idx:i + 1]
                    is_5d = not (closes_in_period > entry_high).any()
                else:
                    is_5d = False
                if is_below_ma and (is_1d or is_5d):
                    signals[i] = -1
                    exit_reasons[i] = 'trend_break' if is_1d else 'time_stop'
                    position = 0
                    logic_state = None

        logic_states[i] = logic_state
        if position == 1:
            entry_highs[i] = entry_high
            entry_idxs[i]  = entry_idx

    df['signal'] = signals
    df['logic_state'] = logic_states
    df['exit_reason'] = exit_reasons
    df['entry_high_val'] = entry_highs
    df['entry_idx_val'] = entry_idxs

    # ── T+1 执行 ──
    actual_pos = np.zeros(n, dtype=int)
    pos = 0
    for i in range(n):
        if i > 0 and signals[i - 1] == 1:
            pos = 1
        elif i > 0 and signals[i - 1] == -1:
            pos = 0
        actual_pos[i] = pos
    df['actual_pos'] = actual_pos

    # ── 每日收益 ──
    strat_ret = np.zeros(n)
    for i in range(1, n):
        if actual_pos[i] == 1 and actual_pos[i - 1] == 0:
            strat_ret[i] = (df['close'].iloc[i] / df['open'].iloc[i] - 1) - COST
        elif actual_pos[i] == 0 and actual_pos[i - 1] == 1:
            strat_ret[i] = (df['open'].iloc[i] / df['close'].iloc[i - 1] - 1) - COST
        elif actual_pos[i] == 1 and actual_pos[i - 1] == 1:
            strat_ret[i] = df['close'].iloc[i] / df['close'].iloc[i - 1] - 1

    df['strat_ret'] = strat_ret
    df['strat_nav'] = (1 + df['strat_ret']).cumprod()
    df['strat_nav'] /= df['strat_nav'].iloc[0]

    df['bench_ret'] = df['close'].pct_change().fillna(0)
    df['bench_nav'] = (1 + df['bench_ret']).cumprod()
    df['bench_nav'] /= df['bench_nav'].iloc[0]

    return df

df = load_and_compute(END_DATE)
n = len(df)


# ── 虚拟首阴仓位计算（仅供战术指令板参考提示使用）──
def compute_virtual_firstneg(df):
    """Composite 持仓期间，追踪虚拟 FirstNeg 仓位的生命周期。"""
    empty = {'active': False, 'entry_date': None, 'entry_high': None,
             'held_days': 0, 'exit_signal': None}
    n = len(df)
    if n < 2:
        return empty
    last_idx = n - 1

    # 仅当前持仓中且为 Composite 逻辑时才有意义
    if df['actual_pos'].iloc[last_idx] != 1:
        return empty
    if df['logic_state'].iloc[last_idx] != 'Composite':
        return empty

    # 找到当前持仓周期起点
    scan_start = last_idx
    while scan_start > 0 and df['actual_pos'].iloc[scan_start - 1] == 1:
        scan_start -= 1

    # 向前扫描，追踪虚拟仓位
    v_active = False
    v_entry_idx = 0
    v_entry_high = 0.0
    v_exit_signal = None

    for i in range(max(scan_start, 1), n):
        curr_c  = df['close'].iloc[i]
        prev_c  = df['close'].iloc[i - 1]
        ma_f    = df['ma_30'].iloc[i]
        ma_t    = df['ma_10'].iloc[i]
        ma_s    = df['ma_5'].iloc[i]
        turn    = df['etf_turnover'].iloc[i]
        prev_cg = int(df['consec_gains'].iloc[i - 1])
        is_down = curr_c < prev_c

        if df['actual_pos'].iloc[i] != 1:
            v_active = False
            v_exit_signal = None
            continue

        if df['logic_state'].iloc[i] != 'Composite':
            v_active = False
            v_exit_signal = None
            continue

        if v_active:
            # 检查虚拟退出条件
            is_below_ma = curr_c < ma_f
            v_held_days = i - v_entry_idx

            # 短线平仓：close < MA30 + 当日下跌
            if is_below_ma and is_down:
                v_exit_signal = 'trend_break'
                v_active = False
                continue

            # 时间止损：close < MA30 + 持仓≥5天 + 未回 entry_high
            if v_held_days >= 5 and is_below_ma:
                closes_in_period = df['close'].iloc[v_entry_idx:i + 1]
                if not (closes_in_period > v_entry_high).any():
                    v_exit_signal = 'time_stop'
                    v_active = False
                    continue

            # 安全过渡：持仓>5天无退出触发 → 静默销毁
            if v_held_days > 5:
                v_active = False
                v_exit_signal = None
                continue

            # 仍存续，清除前次退出信号
            v_exit_signal = None
        else:
            # 检查是否创建虚拟仓位
            v_exit_signal = None
            conds_met = (
                curr_c > ma_t and       # close > MA10
                prev_cg >= 3 and        # 昨日连涨 >= 3天
                is_down and             # 今日首阴
                turn > 1.0 and          # ETF 换手率 > 1%
                curr_c > ma_s and       # close > MA5
                curr_c > ma_f           # close > MA30
            )
            if conds_met:
                v_active = True
                v_entry_idx = i
                v_entry_high = df['high'].iloc[i]
                v_exit_signal = None

    # 构造返回值
    result = empty.copy()
    if v_active:
        result.update({
            'active': True,
            'entry_date': df['trade_date'].iloc[v_entry_idx],
            'entry_high': v_entry_high,
            'held_days': last_idx - v_entry_idx,
            'exit_signal': None,
        })
    elif v_exit_signal is not None:
        # 虚拟仓位刚在最后一天退出（v_entry_idx 已在创建时赋值，此处必然有效）
        result.update({
            'active': False,
            'entry_date': df['trade_date'].iloc[v_entry_idx],
            'entry_high': v_entry_high,
            'held_days': last_idx - v_entry_idx,
            'exit_signal': v_exit_signal,
        })
    return result


vfn = compute_virtual_firstneg(df)

# ── 统计指标 ──
def max_drawdown(nav):
    peak = nav.cummax()
    return ((nav - peak) / peak).min()

strat_total = (df['strat_nav'].iloc[-1] - 1) * 100
bench_total = (df['bench_nav'].iloc[-1] - 1) * 100
strat_mdd   = max_drawdown(df['strat_nav']) * 100
bench_mdd   = max_drawdown(df['bench_nav']) * 100
excess      = strat_total - bench_total

buy_dates  = df.index[df['signal'] == 1].tolist()
sell_dates = df.index[df['signal'] == -1].tolist()
n_trades   = len(buy_dates)

trade_returns = []
for bi, si in zip(buy_dates, sell_dates[:len(buy_dates)]):
    eb = min(bi + 1, n - 1)
    es = min(si + 1, n - 1)
    tr = (1 + df['strat_ret'].iloc[eb:es + 1]).prod() - 1
    trade_returns.append(tr)
win_rate = np.mean([r > 0 for r in trade_returns]) * 100 if trade_returns else 0

# 最新一行数据
last = df.iloc[-1]
prev = df.iloc[-2]

# ═══════════════════════════════════════════════════════════════════════════════
# 页面标题
# ═══════════════════════════════════════════════════════════════════════════════
last_date_fmt = pd.to_datetime(df['trade_date'].iloc[-1], format='%Y%m%d').strftime('%Y-%m-%d')
start_date_fmt = pd.to_datetime(df['trade_date'].iloc[0], format='%Y%m%d').strftime('%Y-%m-%d')
st.markdown(
    f'<div class="top-bar">'
    f'<h1>🛡️ 中证500量化实战决策中心</h1>'
    f'<p>数据起始：{start_date_fmt}　|　'
    f'最后同步：{last_date_fmt}　|　'
    f'累计交易：{n_trades} 次　|　胜率：{win_rate:.1f}%</p>'
    f'</div>',
    unsafe_allow_html=True,
)

# ── 数据同步状态 ──
_status_file = os.path.join(DATA_DIR, 'update_status.json')
if os.path.exists(_status_file):
    with open(_status_file, 'r', encoding='utf-8') as _f:
        _sync = json.load(_f)
    _st = _sync.get('status', 'unknown')
    _tm = _sync.get('last_update_time', '未知')
    if _st == 'success':
        st.caption(f"🟢 数据同步正常 | 最后更新: {_tm}")
    elif _st == 'retrying':
        _rc = _sync.get('retry_count', 0)
        _err = _sync.get('error_message', '')
        st.caption(f"🟡 数据同步重试中 (第{_rc}次) | {_err}")
    elif _st == 'failed':
        _err = _sync.get('error_message', '')
        st.caption(f"🔴 数据同步失败 | {_err}")
    elif _st == 'running':
        st.caption(f"🔵 数据同步进行中...")
    else:
        st.caption(f"⚪ 最后更新: {_tm}")

# ═══════════════════════════════════════════════════════════════════════════════
# Section A: 核心绩效看板
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-head">📊 核心绩效看板</div>', unsafe_allow_html=True)

st.markdown(f'''<div class="metric-grid">
  <div class="metric-item"><div class="label">🚀 策略累计收益</div><div class="value" style="color:{'#16a34a' if strat_total>=0 else '#dc2626'}">{strat_total:+.2f}%</div></div>
  <div class="metric-item"><div class="label">📉 策略最大回撤</div><div class="value" style="color:#dc2626">{strat_mdd:.2f}%</div></div>
  <div class="metric-item"><div class="label">🏛️ 基准累计收益</div><div class="value" style="color:{'#16a34a' if bench_total>=0 else '#dc2626'}">{bench_total:+.2f}%</div></div>
  <div class="metric-item"><div class="label">📊 相对超额收益</div><div class="value" style="color:{'#16a34a' if excess>=0 else '#dc2626'}">{excess:+.2f}%</div></div>
</div>''', unsafe_allow_html=True)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# Section C: 战术指令板
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-head">🎯 战术指令板</div>', unsafe_allow_html=True)
st.caption(f"以下分析基于最新数据日：{pd.to_datetime(last['trade_date'], format='%Y%m%d').strftime('%Y年%m月%d日')}")

# ── 提取最新数据 ─────────────────────────────────────────────────────────────
breadth_val = last['breadth']
hz_val      = last['heat_z']
turn_val    = last['etf_turnover']
close_val   = last['close']
ma30_val    = last['ma_30']
ma10_val    = last['ma_10']
ma5_val     = last['ma_5']
prev_cg     = int(prev['consec_gains'])
is_down     = close_val < prev['close']
ma30_slope  = ma30_val - prev['ma_30']
above_ma30  = close_val > ma30_val
dist_pct    = (close_val / ma30_val - 1) * 100
sig = int(last['signal'])
pos = int(last['actual_pos'])

# ── 市场模式判定 ──
if close_val > ma30_val and ma30_slope > 0:
    mode_text, mode_color = "🐂 多头强趋势", "#16a34a"
    mode_desc = "价格站稳 MA30 上方，均线正向上行"
elif close_val < ma30_val and ma30_slope < 0:
    mode_text, mode_color = "🐻 空头弱趋势", "#dc2626"
    mode_desc = "价格运行于 MA30 下方，均线向下倾斜"
else:
    mode_text, mode_color = "🦓 震荡整理期", "#ea580c"
    mode_desc = "趋势方向不明确，价格与均线交织"

# ── FirstNeg 条件扫描（提前计算供操作状态和建议使用）──
cond_items = [
    ("收盘 > MA10 (趋势确认)", close_val > ma10_val,
     f"收盘 {close_val:.2f} vs MA10 {ma10_val:.2f}"),
    ("昨日连涨 >= 3 天", prev_cg >= 3,
     f"昨日连涨 {prev_cg} 天"),
    ("今日首阴 (收跌)", is_down,
     f"今收 {close_val:.2f} vs 昨收 {prev['close']:.2f}"),
    ("ETF 换手率 > 1.0%", turn_val > 1.0,
     f"当前换手率 {turn_val:.2f}%"),
    ("收盘 > MA5 (短期支撑)", close_val > ma5_val,
     f"收盘 {close_val:.2f} vs MA5 {ma5_val:.2f}"),
    ("收盘 > MA30 (趋势过滤)", close_val > ma30_val,
     f"收盘 {close_val:.2f} vs MA30 {ma30_val:.2f}"),
]
met_count  = sum(1 for _, met, _ in cond_items if met)
total_cond = len(cond_items)

# ── 操作状态判定（细粒度信号标签）──
ref_tip = None  # 参考提示 dict: {text, desc, type}

if sig == 1:
    ls = last['logic_state']
    if ls == 'Composite':
        act_text, act_color = "🔥 抄底买入", "#16a34a"
        act_desc = "Composite 左侧抄底信号触发，T+1 次日开盘执行"
    elif ls == 'FirstNeg':
        act_text, act_color = "⚡ 首阴买入", "#16a34a"
        act_desc = "FirstNeg 连涨首阴低吸信号触发，T+1 次日开盘执行"
    else:
        act_text, act_color = "🚨 执行买入", "#16a34a"
        act_desc = f"入场逻辑：{ls or 'N/A'}，T+1 次日开盘执行"
elif sig == -1:
    er = last['exit_reason']
    if er == 'overheating':
        act_text, act_color = "🚨 过热平仓", "#dc2626"
        act_desc = f"广度 {breadth_val:.1f}% 超过 79% 过热阈值，heat_z {hz_val:.2f}σ 未突破放量阈值 → 量价背离退出，T+1 次日开盘卖出"
    elif er == 'trend_break':
        act_text, act_color = "🚨 短线平仓", "#dc2626"
        act_desc = f"收盘 {close_val:.2f} < MA30 {ma30_val:.2f} 且当日收跌 → 趋势破位退出，T+1 次日开盘卖出"
    elif er == 'time_stop':
        act_text, act_color = "⏰ 时间止损", "#dc2626"
        act_desc = "收盘跌破 MA30 且持仓满5日未收复入场高点 → 时间止损退出，T+1 次日开盘卖出"
    else:
        act_text, act_color = "🚨 执行卖出", "#dc2626"
        act_desc = "触发退出条件，T+1 次日开盘卖出"
elif pos == 1:
    act_text, act_color = "💎 持股待涨", "#2563eb"
    act_desc = f"持仓逻辑：{last['logic_state'] or 'N/A'}，未触发退出条件"
    # 检查参考提示：Composite 持仓时的虚拟首阴仓位
    if vfn['active'] and last['logic_state'] == 'Composite':
        ref_tip = {
            'text': "⚡ 首阴加仓（参考提示）",
            'desc': f"Composite 持仓期间 FirstNeg 6项条件全部满足，可考虑加仓 | 虚拟入场高点: {vfn['entry_high']:.2f}",
            'type': 'entry',
        }
    # vfn.exit_signal 仅在当前 Composite 持仓时才可能非 None（函数内部有前置守卫）
    elif vfn.get('exit_signal') == 'trend_break':
        ref_tip = {
            'text': "🚨 首阴短线平仓（参考提示）",
            'desc': "虚拟首阴仓位触发趋势破位退出：收盘跌破 MA30 且当日收跌",
            'type': 'exit',
        }
    elif vfn.get('exit_signal') == 'time_stop':
        ref_tip = {
            'text': "⏰ 首阴时间止损（参考提示）",
            'desc': f"虚拟首阴仓位触发时间止损：跌破 MA30 且持仓 {vfn['held_days']} 日未收复入场高点",
            'type': 'exit',
        }
    # 检查参考提示：FirstNeg 持仓时首阴条件再次满足（无需虚拟仓位）
    elif last['logic_state'] == 'FirstNeg' and met_count == total_cond and sig == 0:
        ref_tip = {
            'text': "⚡ 首阴加仓（参考提示）",
            'desc': "FirstNeg 持仓期间首阴6项条件再次满足，可考虑加仓",
            'type': 'entry',
        }
else:
    act_text, act_color = "🛡️ 空仓观望", "#6b7280"
    act_desc = "未满足入场条件，耐心等待信号"

# ── 综合理由 & 风险计算 ──
reasons = []
risks   = []
if pos == 1:
    if sig == -1:
        er = last['exit_reason']
        if er == 'overheating':
            reasons.append(f"广度 {breadth_val:.1f}% 超过 79% 过热阈值，同时 heat_z={hz_val:.2f}σ < 1.5σ 表明资金已退潮")
            reasons.append("通用过热退出条件触发，适用于 Composite 和 FirstNeg 两种持仓逻辑")
        elif er == 'trend_break':
            reasons.append(f"收盘 {close_val:.2f} 跌破 MA30 趋势防线 {ma30_val:.2f}")
            reasons.append(f"当日收跌（收盘 {close_val:.2f} < 昨收 {prev['close']:.2f}），趋势破位确认")
        elif er == 'time_stop':
            reasons.append(f"收盘 {close_val:.2f} 跌破 MA30 趋势防线 {ma30_val:.2f}")
            reasons.append("持仓已满 5 个交易日且期间收盘价从未超过入场当日最高价，时间止损触发")
        else:
            reasons.append("策略已触发退出信号")
        risks.append("次日以开盘价执行，如隔夜有大幅波动可能产生滑点")
    else:
        if above_ma30:
            reasons.append(f"MA30 趋势保护有效，收盘 {close_val:.2f} > MA30 {ma30_val:.2f}")
        if breadth_val < 70:
            reasons.append(f"广度 {breadth_val:.1f}% 未过热，上涨空间仍存")
        if hz_val > -1.5:
            reasons.append("资金热度正常，无冷清退潮迹象")
        if not above_ma30:
            risks.append(f"价格已在 MA30 下方，若继续走弱可能触发 FirstNeg 退出条件")
        if breadth_val > 65:
            risks.append(f"广度 {breadth_val:.1f}% 偏高，关注是否接近过热卖出阈值")
        # 参考提示上下文
        if ref_tip is not None:
            if ref_tip['type'] == 'entry':
                reasons.append(f"[参考] {last['logic_state'] or ''} 持仓期间 FirstNeg 6项条件全部满足，可参考加仓")
                risks.append("[参考] 首阴加仓为参考提示，非策略强制动作，请自行判断仓位管理")
            elif ref_tip['type'] == 'exit':
                reasons.append("[参考] 虚拟首阴仓位已触发退出条件，主仓位策略暂未触发卖出")
                risks.append("[参考] 虚拟首阴退出仅供参考，实际仓位应以策略信号为准")
else:
    if sig == 1:
        ls = last['logic_state']
        if ls == 'Composite':
            reasons.append(f"广度 {breadth_val:.1f}% 触及冰点 (< 16%) → Composite 左侧抄底信号")
            reasons.append("市场极度恐慌，历史上冰点往往对应阶段性底部区域")
            risks.append("T+1 执行，次日以开盘价买入")
            risks.append("底部可能有反复磨底，需做好短期波动准备")
        elif ls == 'FirstNeg':
            reasons.append("FirstNeg 6 项条件全部满足 → 连涨后首阴低吸机会")
            reasons.append(f"趋势向上 (收盘 > MA10/MA5/MA30)，量能充沛 (换手率 {turn_val:.2f}%)")
            risks.append("T+1 执行，次日以开盘价买入")
            risks.append("首阴反弹失败概率存在，最长 5 天后可能触发时间止损")
        else:
            reasons.append(f"入场逻辑：{ls or 'N/A'}")
            risks.append("T+1 执行，次日以开盘价买入")
    else:
        reasons.append("当前未产生任何入场信号")
        if breadth_val > 30:
            reasons.append(f"广度 {breadth_val:.1f}% 远离冰点（16%），Composite 条件不满足")
        if met_count < total_cond:
            reasons.append(f"FirstNeg 仅满足 {met_count}/{total_cond} 项条件")
        if not above_ma30:
            reasons.append("价格在 MA30 下方，趋势不友好，不宜贸然入场")
        risks.append("空仓期间可能错过突发行情，但风控优先")

# ═══════════════════════════════════════════════════════════════════════════════
# 渲染：① 状态栏 → ② 操作建议 → ③ 逻辑分析
# ═══════════════════════════════════════════════════════════════════════════════

# ── ① 市场模式 + 操作建议（两列，手机自动堆叠）──
s1, s2 = st.columns(2)
with s1:
    st.markdown(
        f'<div class="status-card" style="background:{mode_color}">{mode_text}</div>'
        f'<p class="status-desc">{mode_desc}</p>',
        unsafe_allow_html=True,
    )
with s2:
    st.markdown(
        f'<div class="status-card" style="background:{act_color}">{act_text}</div>'
        f'<p class="status-desc">{act_desc}</p>',
        unsafe_allow_html=True,
    )

# ── 参考提示卡片（虚拟首阴仓位）──
if ref_tip is not None:
    tip_css = 'ref-tip-card' if ref_tip['type'] == 'entry' else 'ref-tip-exit-card'
    st.markdown(
        f'<div class="{tip_css}">{ref_tip["text"]}</div>'
        f'<p class="ref-tip-desc">{ref_tip["desc"]}</p>'
        f'<p class="ref-tip-disclaimer">* 参考提示仅供辅助决策，非回测策略执行指令</p>',
        unsafe_allow_html=True,
    )

# ── ② 关键理由 + 风险提示（白色卡片）──
reason_md = "\n".join(f"- {r}" for r in reasons)
risk_md   = "\n".join(f"- ⚠️ {r}" for r in risks) if risks else ""
body_parts = [f"**判断依据：**\n{reason_md}"]
if risk_md:
    body_parts.append(f"**风险提示：**\n{risk_md}")
st.markdown(
    '<div class="reason-block">' + '</div>',
    unsafe_allow_html=True,
)
st.markdown("\n\n".join(body_parts))

# ── 技术快照 ──
st.markdown("")
st.markdown(f'''<div class="metric-grid">
  <div class="metric-item"><div class="label">广度</div><div class="value">{breadth_val:.1f}%</div></div>
  <div class="metric-item"><div class="label">20日热度</div><div class="value">{hz_val:.2f}σ</div></div>
  <div class="metric-item"><div class="label">ETF换手率</div><div class="value">{turn_val:.2f}%</div></div>
  <div class="metric-item"><div class="label">MA30</div><div class="value">{ma30_val:.2f}</div></div>
</div>''', unsafe_allow_html=True)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# Section B: 参考图表
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-head">📈 参考图表</div>', unsafe_allow_html=True)

dates = df['date'].values

# ── 图 1：策略净值 vs 基准净值 + 信号标记 ─────────────────────────────────────
fig1, ax1 = plt.subplots(figsize=(16, 6))

# 持仓区间阴影
for i in range(1, n):
    if df['actual_pos'].iloc[i] == 1:
        ax1.axvspan(df['date'].iloc[i - 1], df['date'].iloc[i],
                    color='#3b82f6', alpha=0.06)

ax1.plot(dates, df['bench_nav'], color='#94a3b8', linewidth=1.0,
         linestyle='--', alpha=0.7, label='基准净值 (买入持有)')
ax1.plot(dates, df['strat_nav'], color='#1e3a5f', linewidth=2.0,
         label='策略净值')

# 买卖标记 (T+1 执行日)
for i in range(n):
    if df['signal'].iloc[i] == 1 and i + 1 < n:
        ax1.scatter(df['date'].iloc[i + 1], df['strat_nav'].iloc[i + 1],
                    marker='^', color='#22c55e', s=120, zorder=5)
    elif df['signal'].iloc[i] == -1 and i + 1 < n:
        ax1.scatter(df['date'].iloc[i + 1], df['strat_nav'].iloc[i + 1],
                    marker='v', color='#ef4444', s=120, zorder=5)

ax1.axhline(y=1.0, color='gray', linestyle=':', alpha=0.4)
ax1.set_title('策略净值表现与信号点分布', fontsize=14)
ax1.set_ylabel('累计净值')
ax1.legend(loc='upper left', fontsize=10)
ax1.grid(True, alpha=0.2)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig1.autofmt_xdate(rotation=30)
plt.tight_layout()
st.pyplot(fig1)
plt.close(fig1)

# ── 图 2：中证500 收盘价 + MA30 ──────────────────────────────────────────────
fig2, ax2 = plt.subplots(figsize=(16, 5))

# 价格在 MA30 上方/下方的区间填充
ax2.fill_between(dates, df['close'], df['ma_30'],
                 where=(df['close'] >= df['ma_30']),
                 color='#22c55e', alpha=0.08, interpolate=True)
ax2.fill_between(dates, df['close'], df['ma_30'],
                 where=(df['close'] < df['ma_30']),
                 color='#ef4444', alpha=0.08, interpolate=True)

ax2.plot(dates, df['close'], color='#1e40af', linewidth=1.2, label='中证500收盘价')
ax2.plot(dates, df['ma_30'], color='#f59e0b', linewidth=1.5,
         linestyle='--', label='MA30 趋势线')

ax2.set_title('中证500 价格趋势 & MA30 生命线', fontsize=14)
ax2.set_ylabel('指数点位')
ax2.legend(loc='upper left', fontsize=10)
ax2.grid(True, alpha=0.2)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig2.autofmt_xdate(rotation=30)
plt.tight_layout()
st.pyplot(fig2)
plt.close(fig2)

# ── 图 3：市场广度监控 ───────────────────────────────────────────────────────
fig3, ax3 = plt.subplots(figsize=(16, 4))

for i in range(1, n):
    if df['actual_pos'].iloc[i] == 1:
        ax3.axvspan(df['date'].iloc[i - 1], df['date'].iloc[i],
                    color='#3b82f6', alpha=0.06)

ax3.plot(dates, df['breadth'], color='#f59e0b', linewidth=1.2,
         label='MA20上方占比 (%)')
ax3.axhline(y=16, color='#22c55e', linestyle='--', linewidth=1, label='冰点线 (16%)')
ax3.axhline(y=80, color='#ef4444', linestyle='--', linewidth=1, label='过热线 (80%)')
ax3.fill_between(dates, 0, 16, color='#22c55e', alpha=0.04)
ax3.fill_between(dates, 80, 100, color='#ef4444', alpha=0.04)

ax3.set_title('市场广度监控', fontsize=14)
ax3.set_ylabel('广度 (%)')
ax3.set_ylim(0, 100)
ax3.legend(loc='upper left', fontsize=9, ncol=3)
ax3.grid(True, alpha=0.2)
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig3.autofmt_xdate(rotation=30)
plt.tight_layout()
st.pyplot(fig3)
plt.close(fig3)

# ── 图 4：资金成交热度 Z-Score ────────────────────────────────────────────────
fig4, ax4 = plt.subplots(figsize=(16, 4))

hz_pos = df['heat_z'].clip(lower=0)
hz_neg = df['heat_z'].clip(upper=0)
ax4.fill_between(dates, 0, hz_pos, color='#ef4444', alpha=0.4, label='过热 (Z>0)')
ax4.fill_between(dates, 0, hz_neg, color='#3b82f6', alpha=0.4, label='冷清 (Z<0)')
ax4.axhline(y=1.5, color='#ef4444', linestyle=':', linewidth=1, label='过热阈值 (1.5σ)')
ax4.axhline(y=-1.5, color='#3b82f6', linestyle=':', linewidth=1, label='冰点阈值 (-1.5σ)')
ax4.axhline(y=0, color='gray', linestyle='-', linewidth=0.5, alpha=0.5)

ax4.set_title('资金成交热度 (Z-Score)', fontsize=14)
ax4.set_ylabel('Z-Score (σ)')
ax4.legend(loc='upper left', fontsize=9, ncol=4)
ax4.grid(True, alpha=0.2)
ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig4.autofmt_xdate(rotation=30)
plt.tight_layout()
st.pyplot(fig4)
plt.close(fig4)

# ── 图 5：ETF 换手率 ─────────────────────────────────────────────────────────
fig5, ax5 = plt.subplots(figsize=(16, 4))

ax5.plot(dates, df['etf_turnover'], color='#8b5cf6', linewidth=1.0,
         label='510500 ETF 换手率 (%)')
ax5.axhline(y=1.0, color='#22c55e', linestyle='--', linewidth=1,
            label='流动性下限 (1.0%)')
ax5.fill_between(dates, 0, df['etf_turnover'],
                 where=(df['etf_turnover'] < 1.0),
                 color='#fbbf24', alpha=0.15)

ax5.set_title('510500 ETF 换手率监控', fontsize=14)
ax5.set_ylabel('换手率 (%)')
ax5.legend(loc='upper left', fontsize=9)
ax5.grid(True, alpha=0.2)
ax5.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax5.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig5.autofmt_xdate(rotation=30)
plt.tight_layout()
st.pyplot(fig5)
plt.close(fig5)

st.divider()

# ── ③ 逻辑实时深度扫描（折叠面板，手机友好）──
st.markdown('<div class="section-head">🔍 逻辑实时深度扫描</div>', unsafe_allow_html=True)

with st.expander("A. 市场广度分析", expanded=True):
    if breadth_val < 16:
        st.markdown(
            f"📉 **[极端冰点逻辑]**　当前广度 = **{breadth_val:.1f}%**\n\n"
            f"全场仅不足 16% 的个股站上其 20 日均线，这意味着市场已进入极度恐慌或卖盘枯竭阶段。"
            f"历史回测表明，广度触及冰点往往预示着阶段性底部的临近——此时绝大多数筹码已被"
            f"充分换手，剩余抛压有限。但需注意：冰点状态可能持续数日甚至更久（「磨底」），"
            f"不宜盲目追涨。\n\n"
            f"**策略含义**：Composite 买入条件已满足（breadth < 16%），若当前空仓则触发左侧入场信号。"
        )
    elif breadth_val > 80:
        st.markdown(
            f"🚩 **[广度高位警示]**　当前广度 = **{breadth_val:.1f}%**\n\n"
            f"超过 80% 的个股已站上其 20 日均线，市场处于全面亢奋状态。赚钱效应虽然普遍，"
            f"但也意味着场内潜在买盘可能已被充分消耗。历史经验表明，广度持续高于 80% 后，"
            f"市场容易出现两种情况：(1) 指数惯性冲高但个股开始分化，(2) 某一根放量阴线"
            f"引发集体获利了结。\n\n"
            f"**策略含义**：如同时满足 heat_z < 1.5（资金退潮），将触发通用卖出条件。"
            f"即使未触发，也建议提高警惕，避免追涨。"
        )
    elif breadth_val < 30:
        st.markdown(
            f"🔵 **[偏冷区间]**　当前广度 = **{breadth_val:.1f}%**\n\n"
            f"不到三成个股站上均线，市场整体偏弱。虽未达冰点触发线（16%），但赚钱效应"
            f"已经较差。此阶段通常对应指数的缩量下跌或横盘整理，需要关注广度是否继续"
            f"下探接近冰点（可能产生 Composite 入场信号），还是企稳回升。"
        )
    elif breadth_val > 65:
        st.markdown(
            f"🟡 **[偏热区间]**　当前广度 = **{breadth_val:.1f}%**\n\n"
            f"超过六成个股站上均线，市场情绪偏向乐观。赚钱效应尚好，但已逐步接近过热"
            f"区间。如果广度继续走高突破 80%，需密切关注是否叠加 heat_z 降温，形成卖出条件。"
            f"此阶段适合顺势持有，但不建议新增仓位追涨。"
        )
    else:
        st.markdown(
            f"✅ **[常规区间]**　当前广度 = **{breadth_val:.1f}%**\n\n"
            f"市场广度处于正常波动范围（16%~80%），无极端信号。"
            f"广度在 30%~65% 之间说明市场多空力量相对均衡，暂时不构成方向性判断依据。"
        )

with st.expander("B. 资金热度分析", expanded=True):
    if hz_val > 1.5:
        st.markdown(
            f"🔥 **[情绪过热逻辑]**　当前 Heat_Z = **{hz_val:.2f}σ**\n\n"
            f"成交额已超出近 20 日均值 1.5 倍标准差，市场情绪达到高潮。大幅放量通常"
            f"伴随着「最后一波」的集中入场，量能急速释放后往往出现动能衰竭。\n\n"
            f"历史规律：heat_z > 1.5σ 后的 3~5 个交易日内，指数出现阶段性回调的概率"
            f"显著升高。如同时叠加广度 > 79%，策略将触发通用卖出条件。\n\n"
            f"**策略含义**：若当前持仓，需高度警惕短期回调风险。"
        )
    elif hz_val < -1.5:
        st.markdown(
            f"🧊 **[交投冷清逻辑]**　当前 Heat_Z = **{hz_val:.2f}σ**\n\n"
            f"成交额跌至近 20 日均值 1.5 倍标准差以下，市场陷入地量状态。这通常出现在"
            f"长假前、阴跌末期或市场极度观望阶段。地量往往意味着浮筹已被充分清洗，"
            f"但也可能是趋势延续下跌的中继缩量。\n\n"
            f"**策略含义**：单独的冷清信号不构成买卖依据，需结合广度冰点（breadth < 16%）"
            f"综合判断。当冷清遇上冰点，往往是底部区域的标志性特征。"
        )
    else:
        hz_status = "偏热" if hz_val > 0.5 else ("偏冷" if hz_val < -0.5 else "中性")
        st.markdown(
            f"✅ **[资金面{hz_status}]**　当前 Heat_Z = **{hz_val:.2f}σ**\n\n"
            f"成交热度处于正常波动区间（±1.5σ 以内），市场交投活跃度无极端表现。"
            f"当前 Z 值为 {hz_val:.2f}σ，{'成交略高于平均水平，市场参与意愿尚可' if hz_val > 0 else '成交略低于平均水平，市场观望情绪较浓'}。"
            f"暂不构成独立的买卖信号参考。"
        )

with st.expander("C. 趋势保护分析", expanded=True):
    if above_ma30:
        slope_desc = "均线正向上行" if ma30_slope > 0 else "均线走平或微降"
        st.markdown(
            f"✅ **[趋势生命线保护]**　收盘价 **{close_val:.2f}** > MA30 **{ma30_val:.2f}**"
            f"（偏离 {dist_pct:+.2f}%）\n\n"
            f"当前价格站稳在 MA30 (30日均线) 之上，{slope_desc}。只要不放量跌破该防守位，"
            f"中线「看多做多」的逻辑基石依然稳固。MA30 同时也是 Composite 退出判定中"
            f"的 MA_Filter 参数，价格在其上方时 FirstNeg 退出条件更难触发。\n\n"
            f"**建议**：趋势友好，适合顺势持有或等待回踩 MA30 附近的入场机会。"
        )
    else:
        slope_desc = "均线正在走低" if ma30_slope < 0 else "均线走平"
        st.markdown(
            f"⚠️ **[趋势压制风险]**　收盘价 **{close_val:.2f}** < MA30 **{ma30_val:.2f}**"
            f"（偏离 {dist_pct:+.2f}%）\n\n"
            f"价格处于 MA30 下方，{slope_desc}，属于典型的空头排布。在这种格局下，"
            f"任何反弹在没有放量收复 MA30 之前，都应视为「技术性抽风」而非真正的反转。"
            f"如果当前以 FirstNeg 逻辑持仓，跌破 MA30 将满足退出条件的前置因子"
            f"（is_below_ma），配合日内下跌或 5 日滞涨即触发卖出。\n\n"
            f"**建议**：趋势偏空，控制仓位，避免左侧抄底（除非广度触及冰点 16%）。"
        )

with st.expander("D. 首阴 (FirstNeg) 入场条件扫描", expanded=True):
    for label, met, detail in cond_items:
        icon = "✅" if met else "❌"
        st.markdown(f"- {icon} **{label}**　→ {detail}")

    if met_count == total_cond:
        if pos == 1 and last['logic_state'] == 'Composite':
            st.success(
                f"🎯 全部 {total_cond} 项条件满足！当前以 Composite 逻辑持仓，"
                f"FirstNeg 加仓窗口已打开（参考提示）。"
            )
        elif pos == 0:
            st.success(
                f"🎯 全部 {total_cond} 项条件满足！若当前空仓，FirstNeg 入场信号已触发。"
                f"该信号代表连涨后的首次回调（首阴），在多头趋势中属于典型的「强势低吸」机会。"
            )
        else:
            st.success(
                f"🎯 全部 {total_cond} 项条件满足！"
                f"当前已持仓（逻辑：{last['logic_state'] or 'N/A'}）。"
            )
    else:
        missing = [label for label, met, _ in cond_items if not met]
        st.info(
            f"📋 已满足 {met_count}/{total_cond} 项条件，"
            f"尚缺：{'、'.join(missing)}。FirstNeg 入场条件暂不具备。"
        )
