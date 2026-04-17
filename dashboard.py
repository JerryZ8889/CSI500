#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dashboard.py — CSI500 量化实战决策中心 (Streamlit)
基于 strategy_data.csv + backtest.py 策略引擎
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import json
import html

from strategy_engine import (
    compute_strategy_frame,
    compute_trade_summary,
    compute_virtual_firstneg,
    max_drawdown,
)
from tools.strategy_meta_switch_research import BASE_HYBRID
from tools.strategy_research_advanced import (
    merge_base_with_features,
    simulate as simulate_advanced,
)

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
QUANT_REQUIRED_COLS = ['breadth_ma20', 'breadth_ma60']


def summarize_display(frame):
    total_pct = (frame['strat_nav'].iloc[-1] - 1) * 100
    bench_total_pct = (frame['bench_nav'].iloc[-1] - 1) * 100
    mdd_pct = max_drawdown(frame['strat_nav']) * 100
    bench_mdd_pct = max_drawdown(frame['bench_nav']) * 100
    trade_summary = compute_trade_summary(frame)
    return {
        'total_pct': total_pct,
        'bench_total_pct': bench_total_pct,
        'mdd_pct': mdd_pct,
        'bench_mdd_pct': bench_mdd_pct,
        'excess_pct': total_pct - bench_total_pct,
        'n_trades': trade_summary['n_trades'],
        'win_rate': trade_summary['win_rate'],
        'hold_pct': frame['actual_pos'].mean() * 100,
    }


def render_metric_grid(items):
    cards = []
    for label, value, color in items:
        cards.append(
            f'<div class="metric-item"><div class="label">{label}</div>'
            f'<div class="value" style="color:{color}">{value}</div></div>'
        )
    return '<div class="metric-grid">' + ''.join(cards) + '</div>'


def render_reason_block(title, reasons, risks):
    parts = [f'<div class="reason-block">']
    if title:
        parts.append(f'<div style="font-weight:700; margin-bottom:10px;">{html.escape(title)}</div>')
    if reasons:
        parts.append('<div style="font-weight:600; margin-bottom:6px;">判断依据：</div><ul style="margin:0 0 12px 1.1rem; padding:0;">')
        for reason in reasons:
            parts.append(f'<li style="margin:0 0 4px 0;">{html.escape(reason)}</li>')
        parts.append('</ul>')
    if risks:
        parts.append('<div style="font-weight:600; margin-bottom:6px;">风险提示：</div><ul style="margin:0 0 0 1.1rem; padding:0;">')
        for risk in risks:
            parts.append(f'<li style="margin:0 0 4px 0;">{html.escape(risk)}</li>')
        parts.append('</ul>')
    parts.append('</div>')
    return ''.join(parts)


def load_quant_source(end_date, rebuild_features=False):
    src = merge_base_with_features(rebuild=rebuild_features)
    src['trade_date'] = src['trade_date'].astype(str)
    src = src[(src['trade_date'] >= START_DATE) & (src['trade_date'] <= end_date)].copy()
    src = src.sort_values('trade_date').reset_index(drop=True)
    return src

# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载 & 策略引擎
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def load_and_compute(end_date):
    base = pd.read_csv(DATA_FILE)
    base['trade_date'] = base['trade_date'].astype(str)
    base = base[(base['trade_date'] >= START_DATE) & (base['trade_date'] <= end_date)].copy()
    base = base.sort_values('trade_date').reset_index(drop=True)

    subjective = compute_strategy_frame(base.copy(), cost=COST)

    quant_source = load_quant_source(end_date, rebuild_features=False)
    need_rebuild = (
        quant_source.empty
        or any(col not in quant_source.columns for col in QUANT_REQUIRED_COLS)
        or len(quant_source) != len(base)
        or quant_source[QUANT_REQUIRED_COLS].tail(min(len(quant_source), 5)).isna().any().any()
    )
    if need_rebuild:
        quant_source = load_quant_source(end_date, rebuild_features=True)

    quantitative = simulate_advanced(quant_source.copy(), BASE_HYBRID)
    quantitative['trade_date'] = quantitative['trade_date'].astype(str)
    quantitative['date'] = pd.to_datetime(quantitative['trade_date'], format='%Y%m%d')
    # Keep the source context fields that the dashboard explanation layer needs.
    for col in ['open', 'ma_30', 'breadth_ma20', 'breadth_ma60']:
        quantitative[col] = quant_source[col].to_numpy()
    return subjective, quantitative


df, quant_df = load_and_compute(END_DATE)
n = len(df)


# ── 虚拟首阴仓位计算（仅供战术指令板参考提示使用）──
vfn = compute_virtual_firstneg(df)

# ── 统计指标 ──
subjective_summary = summarize_display(df)
quant_summary = summarize_display(quant_df)
bench_total = subjective_summary['bench_total_pct']
bench_mdd = subjective_summary['bench_mdd_pct']

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
    f'主观策略：{subjective_summary["n_trades"]} 次 / 胜率 {subjective_summary["win_rate"]:.1f}%　|　'
    f'量化策略：{quant_summary["n_trades"]} 次 / 胜率 {quant_summary["win_rate"]:.1f}%</p>'
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

perf_col_1, perf_col_2 = st.columns(2)
with perf_col_1:
    st.markdown("**主观策略**")
    st.markdown(
        render_metric_grid(
            [
                ("🚀 累计收益", f"{subjective_summary['total_pct']:+.2f}%", '#16a34a' if subjective_summary['total_pct'] >= 0 else '#dc2626'),
                ("📉 最大回撤", f"{subjective_summary['mdd_pct']:.2f}%", '#dc2626'),
                ("📊 超额收益", f"{subjective_summary['excess_pct']:+.2f}%", '#16a34a' if subjective_summary['excess_pct'] >= 0 else '#dc2626'),
                ("🎯 胜率", f"{subjective_summary['win_rate']:.1f}%", '#111827'),
            ]
        ),
        unsafe_allow_html=True,
    )
    st.caption(
        f"累计交易 {subjective_summary['n_trades']} 次 | 平均持仓 {subjective_summary['hold_pct']:.1f}%"
    )

with perf_col_2:
    st.markdown("**量化策略（增强规则）**")
    st.markdown(
        render_metric_grid(
            [
                ("🚀 累计收益", f"{quant_summary['total_pct']:+.2f}%", '#16a34a' if quant_summary['total_pct'] >= 0 else '#dc2626'),
                ("📉 最大回撤", f"{quant_summary['mdd_pct']:.2f}%", '#dc2626'),
                ("📊 超额收益", f"{quant_summary['excess_pct']:+.2f}%", '#16a34a' if quant_summary['excess_pct'] >= 0 else '#dc2626'),
                ("🎯 胜率", f"{quant_summary['win_rate']:.1f}%", '#111827'),
            ]
        ),
        unsafe_allow_html=True,
    )
    st.caption(
        f"累计交易 {quant_summary['n_trades']} 次 | 平均持仓 {quant_summary['hold_pct']:.1f}%"
    )

st.caption(f"共同基准（从 {start_date_fmt} 重新起算）：累计收益 {bench_total:+.2f}% | 最大回撤 {bench_mdd:.2f}%")

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

# ── 量化策略当前状态 ──
quant_last = quant_df.iloc[-1]
quant_prev = quant_df.iloc[-2]
q_b20 = float(quant_last['breadth_ma20'])
q_b60 = float(quant_last['breadth_ma60'])
q_heat = float(quant_last['heat_z'])
q_close = float(quant_last['close'])
q_ma30 = float(quant_last['ma_30'])
q_sig = int(quant_last['signal'])
q_pos = int(quant_last['actual_pos'])
q_comp_active = bool(quant_last['comp_active'])
q_trend_active = bool(quant_last['trend_active'])
q_prev_comp_active = bool(quant_prev['comp_active'])
q_prev_trend_active = bool(quant_prev['trend_active'])
q_comp_entry = float(BASE_HYBRID['comp_entry'])
q_comp_exit_breadth = float(BASE_HYBRID['comp_exit_breadth'])
q_comp_exit_heat = float(BASE_HYBRID['comp_exit_heat'])
q_trend_b60_entry = float(BASE_HYBRID['trend_b60_entry'])
q_trend_b20_entry = float(BASE_HYBRID['trend_b20_entry'])
q_trend_b60_exit = float(BASE_HYBRID['trend_b60_exit'])
q_trend_b20_exit = float(BASE_HYBRID['trend_b20_exit'])

if q_comp_active and q_trend_active:
    q_logic_name = "抄底层 + 趋势层"
elif q_comp_active:
    q_logic_name = "抄底层"
elif q_trend_active:
    q_logic_name = "趋势层"
else:
    q_logic_name = "空仓"

q_reasons = []
q_risks = []
if q_sig == 1:
    if q_comp_active and q_trend_active:
        q_act_text, q_act_color = "🚀 双引擎买入", "#16a34a"
        q_act_desc = "抄底层和趋势层同时打开，T+1 次日开盘执行"
    elif q_comp_active:
        q_act_text, q_act_color = "🧲 抄底买入", "#16a34a"
        q_act_desc = "20日广度跌入量化冰点区，T+1 次日开盘执行"
    elif q_trend_active:
        q_act_text, q_act_color = "📈 趋势买入", "#16a34a"
        q_act_desc = "趋势层确认开启，T+1 次日开盘执行"
    else:
        q_act_text, q_act_color = "🚨 执行买入", "#16a34a"
        q_act_desc = "量化策略触发入场信号，T+1 次日开盘执行"
elif q_sig == -1:
    if q_prev_comp_active and not q_comp_active and q_prev_trend_active and not q_trend_active:
        q_act_text, q_act_color = "🚨 双引擎卖出", "#dc2626"
        q_act_desc = "抄底层与趋势层同时关闭，T+1 次日开盘卖出"
    elif q_prev_comp_active and not q_comp_active:
        q_act_text, q_act_color = "🚨 抄底层止盈", "#dc2626"
        q_act_desc = "抄底层达到退出条件，T+1 次日开盘卖出"
    else:
        q_act_text, q_act_color = "🚨 趋势层离场", "#dc2626"
        q_act_desc = "趋势层跌回防守线下方，T+1 次日开盘卖出"
elif q_pos == 1:
    if q_comp_active and q_trend_active:
        q_act_text, q_act_color = "💎 双引擎持有", "#2563eb"
        q_act_desc = "抄底层与趋势层同时持有，未触发退出条件"
    elif q_comp_active:
        q_act_text, q_act_color = "🛡️ 抄底层持有", "#2563eb"
        q_act_desc = "量化抄底层仍在持有区，未触发退出条件"
    elif q_trend_active:
        q_act_text, q_act_color = "📈 趋势层持有", "#2563eb"
        q_act_desc = "量化趋势层仍在持有区，未触发退出条件"
    else:
        q_act_text, q_act_color = "⏳ 持仓待执行", "#2563eb"
        q_act_desc = "量化策略持仓尚未完成切换，等待下一交易日执行"
else:
    q_act_text, q_act_color = "🛡️ 空仓观望", "#6b7280"
    q_act_desc = "量化抄底层和趋势层都未开启"

if q_pos == 1:
    if q_sig == -1:
        if q_prev_comp_active and not q_comp_active:
            q_reasons.append(
                f"抄底层退出：20日广度 {q_b20:.1f}% 超过 {q_comp_exit_breadth:.0f}% ，且 heat_z {q_heat:.2f}σ 低于 {q_comp_exit_heat:.1f}σ"
            )
        if q_prev_trend_active and not q_trend_active:
            q_exit_parts = []
            if q_close < q_ma30:
                q_exit_parts.append(f"收盘 {q_close:.2f} 跌回 MA30 {q_ma30:.2f} 下方")
            if q_b60 < q_trend_b60_exit:
                q_exit_parts.append(f"60日广度 {q_b60:.1f}% 失守 {q_trend_b60_exit:.0f}%")
            if q_b20 < q_trend_b20_exit:
                q_exit_parts.append(f"20日广度 {q_b20:.1f}% 失守 {q_trend_b20_exit:.0f}%")
            q_reasons.append("趋势层退出：" + "；".join(q_exit_parts))
        q_risks.append("量化策略同样按 T+1 执行，次日开盘价可能与信号日有偏差")
    else:
        if q_comp_active:
            q_reasons.append(
                f"抄底层仍开启：20日广度 {q_b20:.1f}% 仍未达到 {q_comp_exit_breadth:.0f}% 的退出区"
            )
        if q_trend_active:
            q_reasons.append(
                f"趋势层仍开启：收盘 {q_close:.2f} > MA30 {q_ma30:.2f}，60日广度 {q_b60:.1f}% / 20日广度 {q_b20:.1f}% 维持强势"
            )
        if q_comp_active and q_b20 > q_comp_exit_breadth - 4:
            q_risks.append("抄底层已经接近高位退出区，若市场降温会较快离场")
        if q_trend_active and (q_close < q_ma30 * 1.01 or q_b60 < q_trend_b60_exit + 5 or q_b20 < q_trend_b20_exit + 5):
            q_risks.append("趋势层离退出阈值不远，留意 MA30 与广度是否继续回落")
else:
    if q_sig == 1:
        if q_comp_active:
            q_reasons.append(f"抄底层开仓：20日广度 {q_b20:.1f}% 低于 {q_comp_entry:.0f}% 冰点阈值")
        if q_trend_active:
            q_reasons.append(
                f"趋势层开仓：收盘 {q_close:.2f} 站上 MA30 {q_ma30:.2f}，60日广度 {q_b60:.1f}% ≥ {q_trend_b60_entry:.0f}% ，20日广度 {q_b20:.1f}% ≥ {q_trend_b20_entry:.0f}%"
            )
        q_risks.append("量化策略也是 T+1 执行，次日开盘若跳空会影响实际成交表现")
    else:
        q_reasons.append("当前量化策略没有入场信号")
        if q_b20 >= q_comp_entry:
            q_reasons.append(f"抄底层未开：20日广度 {q_b20:.1f}% 没有低于 {q_comp_entry:.0f}%")
        q_trend_missing = []
        if q_close <= q_ma30:
            q_trend_missing.append("收盘未站上 MA30")
        if q_b60 < q_trend_b60_entry:
            q_trend_missing.append(f"60日广度 {q_b60:.1f}% 未到 {q_trend_b60_entry:.0f}%")
        if q_b20 < q_trend_b20_entry:
            q_trend_missing.append(f"20日广度 {q_b20:.1f}% 未到 {q_trend_b20_entry:.0f}%")
        if q_trend_missing:
            q_reasons.append("趋势层未开：" + "；".join(q_trend_missing))
        q_risks.append("量化策略空仓时可能错过很短的反抽，但能减少无效进出")

# ═══════════════════════════════════════════════════════════════════════════════
# 渲染：① 状态栏 → ② 操作建议 → ③ 逻辑分析
# ═══════════════════════════════════════════════════════════════════════════════

# ── ① 市场模式 + 双策略操作建议（PC 三列，手机自动堆叠）──
s1, s2, s3 = st.columns(3)
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
with s3:
    st.markdown(
        f'<div class="status-card" style="background:{q_act_color}">{q_act_text}</div>'
        f'<p class="status-desc">{q_act_desc}</p>',
        unsafe_allow_html=True,
    )

if pos == q_pos and sig == q_sig:
    st.caption("当前两套策略方向一致：信号与持仓状态同步。")
else:
    st.caption("当前两套策略存在分歧：主观策略与量化策略的入场/持有判断不完全相同。")

# ── 参考提示卡片（虚拟首阴仓位）──
if ref_tip is not None:
    tip_css = 'ref-tip-card' if ref_tip['type'] == 'entry' else 'ref-tip-exit-card'
    st.markdown(
        f'<div class="{tip_css}">{ref_tip["text"]}</div>'
        f'<p class="ref-tip-desc">{ref_tip["desc"]}</p>'
        f'<p class="ref-tip-disclaimer">* 参考提示仅供辅助决策，非回测策略执行指令</p>',
        unsafe_allow_html=True,
    )

# ── ② 双策略判断依据 + 风险提示 ──
reason_col_1, reason_col_2 = st.columns(2)
with reason_col_1:
    st.markdown(render_reason_block("主观策略判断", reasons, risks), unsafe_allow_html=True)

with reason_col_2:
    st.markdown(render_reason_block("量化策略判断", q_reasons, q_risks), unsafe_allow_html=True)

# ── 技术快照 ──
st.markdown("")
st.markdown(
    render_metric_grid(
        [
            ("市场广度", f"{breadth_val:.1f}%", '#111827'),
            ("20日热度", f"{hz_val:.2f}σ", '#111827'),
            ("ETF换手率", f"{turn_val:.2f}%", '#111827'),
            ("MA30", f"{ma30_val:.2f}", '#111827'),
            ("量化20日广度", f"{q_b20:.1f}%", '#111827'),
            ("量化60日广度", f"{q_b60:.1f}%", '#111827'),
            ("量化当前层", q_logic_name, '#111827'),
        ]
    ),
    unsafe_allow_html=True,
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# Section B: 参考图表
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-head">📈 参考图表</div>', unsafe_allow_html=True)
st.caption("净值图中，主观策略买卖点是三角形，量化策略买卖点是圆点；绿色表示买点，红色表示卖点。")

dates = df['date'].values

# ── 图 1：策略净值 vs 基准净值 + 信号标记 ─────────────────────────────────────
fig1, ax1 = plt.subplots(figsize=(16, 6))

ax1.plot(dates, df['bench_nav'], color='#94a3b8', linewidth=1.0,
         linestyle='--', alpha=0.7, label='基准净值 (买入持有)')
ax1.plot(dates, df['strat_nav'], color='#1e3a5f', linewidth=2.0,
         label='主观策略净值')
ax1.plot(quant_df['date'].values, quant_df['strat_nav'], color='#7c3aed', linewidth=2.0,
         label='量化策略净值')

# 买卖标记 (T+1 执行日)
main_buy_label = False
main_sell_label = False
for i in range(n):
    if df['signal'].iloc[i] == 1 and i + 1 < n:
        ax1.scatter(df['date'].iloc[i + 1], df['strat_nav'].iloc[i + 1],
                    marker='^', color='#22c55e', s=110, zorder=5,
                    label='主观买点' if not main_buy_label else None)
        main_buy_label = True
    elif df['signal'].iloc[i] == -1 and i + 1 < n:
        ax1.scatter(df['date'].iloc[i + 1], df['strat_nav'].iloc[i + 1],
                    marker='v', color='#ef4444', s=110, zorder=5,
                    label='主观卖点' if not main_sell_label else None)
        main_sell_label = True

quant_buy_label = False
quant_sell_label = False
for i in range(len(quant_df)):
    if quant_df['signal'].iloc[i] == 1 and i + 1 < len(quant_df):
        ax1.scatter(quant_df['date'].iloc[i + 1], quant_df['strat_nav'].iloc[i + 1],
                    marker='o', color='#22c55e', s=52, zorder=5,
                    label='量化买点' if not quant_buy_label else None)
        quant_buy_label = True
    elif quant_df['signal'].iloc[i] == -1 and i + 1 < len(quant_df):
        ax1.scatter(quant_df['date'].iloc[i + 1], quant_df['strat_nav'].iloc[i + 1],
                    marker='o', color='#ef4444', s=52, zorder=5,
                    label='量化卖点' if not quant_sell_label else None)
        quant_sell_label = True

ax1.axhline(y=1.0, color='gray', linestyle=':', alpha=0.4)
ax1.set_title('主观策略 / 量化策略 / 基准净值对比', fontsize=14)
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

# ── ③ 主观策略逻辑实时深度扫描（折叠面板，手机友好）──
st.markdown('<div class="section-head">🔍 主观策略逻辑实时深度扫描</div>', unsafe_allow_html=True)

with st.expander("A. 市场广度分析", expanded=False):
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

with st.expander("B. 资金热度分析", expanded=False):
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

with st.expander("C. 趋势保护分析", expanded=False):
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

with st.expander("D. 首阴 (FirstNeg) 入场条件扫描", expanded=False):
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

st.markdown('<div class="section-head">🔍 量化策略逻辑实时深度扫描</div>', unsafe_allow_html=True)

with st.expander("A. 当前量化状态", expanded=False):
    st.markdown(
        f"**当前状态**：{q_act_text}\n\n"
        f"{q_act_desc}\n\n"
        f"当前有效层：**{q_logic_name}**。"
        f" 当前 20 日广度为 **{q_b20:.1f}%**，60 日广度为 **{q_b60:.1f}%**，"
        f"收盘 **{q_close:.2f}**，MA30 **{q_ma30:.2f}**。"
    )

with st.expander("B. 抄底层（低吸层）", expanded=False):
    if q_comp_active:
        st.markdown(
            f"抄底层当前 **已开启**。\n\n"
            f"这一层只看两个条件：\n"
            f"- 入场：20日广度低于 **{q_comp_entry:.0f}%**\n"
            f"- 退出：20日广度高于 **{q_comp_exit_breadth:.0f}%** 且 heat_z 低于 **{q_comp_exit_heat:.1f}σ**\n\n"
            f"现在 20 日广度为 **{q_b20:.1f}%**，heat_z 为 **{q_heat:.2f}σ**，所以抄底层仍在场内。"
        )
    elif q_prev_comp_active and not q_comp_active:
        st.markdown(
            f"抄底层刚刚 **关闭**。\n\n"
            f"当前 20 日广度 **{q_b20:.1f}%** 已经超过 **{q_comp_exit_breadth:.0f}%**，"
            f"同时 heat_z **{q_heat:.2f}σ** 低于 **{q_comp_exit_heat:.1f}σ**，"
            f"说明情绪已经从低位修复到了兑现区。"
        )
    else:
        st.markdown(
            f"抄底层当前 **未开启**。\n\n"
            f"它的入场门槛是 20 日广度低于 **{q_comp_entry:.0f}%**。"
            f"现在是 **{q_b20:.1f}%**，还没有进入量化定义的极端冰点区。"
        )

with st.expander("C. 趋势层（持有层）", expanded=False):
    q_trend_checks = [
        ("收盘站上 MA30", q_close > q_ma30, f"收盘 {q_close:.2f} vs MA30 {q_ma30:.2f}"),
        (f"60日广度 ≥ {q_trend_b60_entry:.0f}%", q_b60 >= q_trend_b60_entry, f"当前 {q_b60:.1f}%"),
        (f"20日广度 ≥ {q_trend_b20_entry:.0f}%", q_b20 >= q_trend_b20_entry, f"当前 {q_b20:.1f}%"),
    ]
    for label, met, detail in q_trend_checks:
        icon = "✅" if met else "❌"
        st.markdown(f"- {icon} **{label}**　→ {detail}")

    if q_trend_active:
        st.success(
            f"趋势层当前已开启。只要收盘不跌回 MA30 下方，且 60 日广度不低于 {q_trend_b60_exit:.0f}% 、"
            f"20 日广度不低于 {q_trend_b20_exit:.0f}% ，量化策略就继续顺势持有。"
        )
    else:
        st.info(
            f"趋势层当前未开启。它要求价格、20 日广度、60 日广度一起站稳，"
            f"比主观策略的单点触发更严格。"
        )

with st.expander("D. 两套策略当前分歧", expanded=False):
    subjective_state = "持仓" if pos == 1 else "空仓"
    quant_state = "持仓" if q_pos == 1 else "空仓"
    if pos == q_pos and sig == q_sig:
        st.markdown(
            f"当前两套策略 **方向一致**。\n\n"
            f"主观策略：**{subjective_state}**，量化策略：**{quant_state}**。"
            f" 这说明当前市场状态下，两套逻辑给出的结论接近。"
        )
    else:
        st.markdown(
            f"当前两套策略 **存在分歧**。\n\n"
            f"- 主观策略：**{subjective_state}**，当前动作是 **{act_text}**\n"
            f"- 量化策略：**{quant_state}**，当前动作是 **{q_act_text}**\n\n"
            f"主观策略更偏向人工规则下的冰点抄底与首阴低吸；"
            f"量化策略则多了一层对全市场强弱结构的过滤，所以有时会更早持有，也有时会更晚入场。"
        )

st.divider()
st.markdown('<div class="section-head">🔄 主观策略和量化策略差异分析</div>', unsafe_allow_html=True)

diff_points = []
diff_risks = []
if pos == q_pos and sig == q_sig:
    diff_points.append(f"当前两套策略方向一致，都是“{'持仓' if pos == 1 else '空仓'}”状态。")
    if pos == 1:
        diff_points.append(
            f"主观策略当前持仓逻辑是 {last['logic_state'] or 'N/A'}，量化策略当前持仓层是 {q_logic_name}。"
        )
        if q_trend_active:
            diff_points.append("量化策略当前除了抄底修复外，还叠加了趋势过滤层。")
        else:
            diff_points.append("量化策略当前主要由抄底层维持持仓，趋势层还没有打开。")
    else:
        diff_points.append("虽然当前方向一致，但两套策略的下一次入场触发条件并不相同。")
else:
    diff_points.append(
        f"当前两套策略有分歧：主观策略是“{act_text}”，量化策略是“{q_act_text}”。"
    )
    diff_points.append(
        f"主观策略状态：{'持仓' if pos == 1 else '空仓'}；量化策略状态：{'持仓' if q_pos == 1 else '空仓'}。"
    )

diff_points.append("主观策略更偏向规则触发后的直接执行，核心是冰点抄底和首阴低吸。")
diff_points.append("量化策略多了一层全市场强弱过滤，会根据 20 日广度、60 日广度和 MA30 联合判断是否值得持有。")
if not q_trend_active and (q_b60 < q_trend_b60_entry or q_b20 < q_trend_b20_entry):
    diff_points.append(
        f"当前量化趋势层没开，主要因为 60 日广度 {q_b60:.1f}% / 20 日广度 {q_b20:.1f}% 还没同时达到趋势开仓线。"
    )
if pos == 1 and q_pos == 1 and last['logic_state'] == 'Composite' and q_comp_active and not q_trend_active:
    diff_points.append("这说明当前市场更像“修复中的存量反弹”，而不是量化定义下的全面强趋势。")

diff_risks.append("两套策略口径不同，短期内买卖点不完全重合是正常现象，不代表哪一套算错。")
if pos != q_pos or sig != q_sig:
    diff_risks.append("如果你主要拿这个页面做决策，分歧期要特别留意自己最终跟哪一套执行。")

st.markdown(
    render_reason_block("", diff_points, diff_risks),
    unsafe_allow_html=True,
)
