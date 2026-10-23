#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
补回 strategy_data.csv 中间漏掉的交易日。

做法：
1. 拿交易日历和 strategy_data.csv 对照，找出表内首尾之间漏掉的交易日
2. 下载这些日子的个股日线，写进对应成分股的个股文件
3. 从第一个漏掉的日子起，往后整段重算（均线、heat_z、广度、ETF 换手率）；
   这之前的行一行不动（包括 design.md 6.4 里手动修正过的广度）
4. 输出预演报告：补了哪些天、原有数字怎么变、两套策略的买卖点有没有变

本脚本总是直接改工作区里的文件。“预演”指在 GitHub Actions 的临时环境里跑完、
只看报告不提交；确认后再用“写入”模式跑一次并提交（见 backfill_missing_days.yml）。

用法：
  TUSHARE_TOKEN=xxx python tools/backfill_missing_days.py --report backfill_report.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from market_data import (  # noqa: E402
    STRATEGY_COLUMNS,
    add_etf_turnover,
    add_index_indicators,
    adjusted_stock_row,
    asof_for,
    compute_breadth,
    find_missing_days,
    insert_stock_row,
    load_components,
    load_stock_parts,
    open_trading_days,
)
from strategy_engine import compute_strategy_frame, compute_trade_summary, list_trades, max_drawdown  # noqa: E402

STRATEGY_FILE = os.path.join(ROOT, 'strategy_data.csv')
COMP_FILE = os.path.join(ROOT, 'csi500_components_schedule.csv')
ADJ_BASE_FILE = os.path.join(ROOT, 'adj_factor_base.csv')
STATUS_FILE = os.path.join(ROOT, 'update_status.json')
STOCKS_DIR = os.path.join(ROOT, 'stocks_data')
ARCHIVE_DIR = os.path.join(ROOT, 'stocks_archive')

INDEX_CODE = '000905.SH'
ETF_CODE = '510500.SH'
CALENDAR_START = '20190101'
DASHBOARD_START = '20240101'   # 看板从这天起算两套策略
API_SLEEP = 0.35
NUMERIC_COLS = [c for c in STRATEGY_COLUMNS if c != 'trade_date']


def api(func, **kwargs):
    time.sleep(API_SLEEP)
    out = func(**kwargs)
    if out is None or out.empty:
        return None
    return out


def read_strategy():
    df = pd.read_csv(STRATEGY_FILE)
    df['trade_date'] = df['trade_date'].astype(str)
    return df.sort_values('trade_date').reset_index(drop=True)


# ── 两套策略的快照（改数据前后各拍一次，用来写报告）─────────────────────────────

def strategy_snapshot(base):
    from tools.strategy_meta_switch_research import BASE_HYBRID
    from tools.strategy_research_advanced import merge_base_with_features, simulate

    view = base[base['trade_date'] >= DASHBOARD_START].reset_index(drop=True)
    subj = compute_strategy_frame(view.copy())

    quant_src = merge_base_with_features(rebuild=True)
    quant_src['trade_date'] = quant_src['trade_date'].astype(str)
    quant_src = quant_src[quant_src['trade_date'] >= DASHBOARD_START].reset_index(drop=True)
    quant = simulate(quant_src.copy(), BASE_HYBRID)
    quant['trade_date'] = quant['trade_date'].astype(str)
    return {'subj': subj, 'quant': quant}


def summary(frame):
    ts_ = compute_trade_summary(frame)
    return {
        'total': (frame['strat_nav'].iloc[-1] - 1) * 100,
        'mdd': max_drawdown(frame['strat_nav']) * 100,
        'n': ts_['n_trades'],
        'win': ts_['win_rate'],
    }


# ── 第 2 步：补个股文件 ─────────────────────────────────────────────────────────

def backfill_stock_files(pro, missing, comp, log):
    adj_base = pd.read_csv(ADJ_BASE_FILE).drop_duplicates('ts_code').set_index('ts_code')['adj_factor']
    asof_sorted = sorted(comp['asof_date'].unique())
    written = {}
    for day in missing:
        daily = api(pro.daily, trade_date=day)
        adj = api(pro.adj_factor, trade_date=day)
        if daily is None or adj is None:
            raise RuntimeError(f"{day} 取不到个股日线或复权因子")
        daily = daily.drop_duplicates('ts_code').set_index('ts_code')
        adj = adj.drop_duplicates('ts_code').set_index('ts_code')['adj_factor']

        asof = asof_for(day, asof_sorted)
        codes = comp.loc[comp['asof_date'] == asof, 'con_code'].unique()
        count = 0
        for code in codes:
            if code not in daily.index or code not in adj.index:
                continue
            base_factor = adj_base.get(code, adj[code])
            row_text = adjusted_stock_row(code, day, daily.loc[code], adj[code], base_factor)
            path = os.path.join(STOCKS_DIR, f'{code}.csv')
            archived = os.path.join(ARCHIVE_DIR, f'{code}.csv')
            if not os.path.exists(path) and os.path.exists(archived):
                path = archived
            if insert_stock_row(path, day, row_text):
                count += 1
        written[day] = (count, len(codes))
        log(f"  {day}: 写入 {count} 只（当日成分股 {len(codes)} 只）")
    return written


# ── 第 3 步：从第一个漏掉的日子起重算 strategy_data.csv ─────────────────────────

def recompute_from(pro, old, first_day, last_day, comp):
    keep = old[old['trade_date'] < first_day]

    idx = api(pro.index_daily, ts_code=INDEX_CODE, start_date=first_day, end_date=last_day)
    if idx is None:
        raise RuntimeError("取不到指数日线")
    idx = idx.sort_values('trade_date').drop_duplicates('trade_date')
    idx = idx[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']].rename(columns={'vol': 'volume'})
    idx['trade_date'] = idx['trade_date'].astype(str)

    # 之前的行提供均线和 heat_z 的热身数据，本身不改
    series = pd.concat(
        [keep[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']], idx],
        ignore_index=True,
    ).sort_values('trade_date').reset_index(drop=True)
    series = add_index_indicators(series)

    new_dates = idx['trade_date'].tolist()
    stock_parts, _ = load_stock_parts([d for d in [ARCHIVE_DIR, STOCKS_DIR] if os.path.isdir(d)])
    series['breadth'] = series['trade_date'].map(compute_breadth(new_dates, comp, stock_parts))

    # 换手率要用前一交易日的份额：带上重算段之前的若干行，份额缺失时可以向前补
    lead = keep['trade_date'].tail(10).tolist()
    etf_start = lead[0] if lead else first_day
    etf_daily = api(pro.fund_daily, ts_code=ETF_CODE, start_date=etf_start, end_date=last_day)
    etf_share = api(pro.fund_share, ts_code=ETF_CODE, start_date=etf_start, end_date=last_day)
    if etf_daily is None or etf_share is None:
        raise RuntimeError("取不到 ETF 日线或份额")
    for f in (etf_daily, etf_share):
        f['trade_date'] = f['trade_date'].astype(str)
    turn = add_etf_turnover(series[['trade_date']][series['trade_date'] >= etf_start].copy(), etf_daily, etf_share)
    series['etf_turnover'] = series['trade_date'].map(turn.set_index('trade_date')['etf_turnover'])

    fresh = series[series['trade_date'] >= first_day][STRATEGY_COLUMNS]
    if fresh[NUMERIC_COLS].isna().any().any():
        bad = fresh[fresh[NUMERIC_COLS].isna().any(axis=1)]['trade_date'].tolist()
        raise RuntimeError(f"重算结果有空值，停止写入：{bad}")
    return pd.concat([keep[STRATEGY_COLUMNS], fresh], ignore_index=True)


# ── 报告 ───────────────────────────────────────────────────────────────────────

def fmt_d(d):
    return f"{d[:4]}-{d[4:6]}-{d[6:]}" if d else '—'


def trade_rows(frame):
    return [
        (t['signal_date'], t['logic'], t['buy_date'], t['held_days'], round(t['ret'] * 100, 2), t['is_open'])
        for t in list_trades(frame)
    ]


def state_text(frame, logic_col):
    last = frame.iloc[-1]
    pos = '持仓' if int(last['actual_pos']) == 1 else '空仓'
    logic = last.get(logic_col)
    sig = {1: '，当天出买入信号', -1: '，当天出卖出信号'}.get(int(last['signal']), '')
    return f"{pos}{'（' + str(logic) + '）' if pos == '持仓' and logic else ''}{sig}"


def build_report(missing, written, first_day, last_day, old, new, before, after):
    L = []
    L.append("# 漏交易日补数预演报告\n")
    L.append(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    L.append("## 一、补了哪些天\n")
    L.append(f"按交易日历核对，`strategy_data.csv` 中间共缺 **{len(missing)}** 个交易日：\n")
    for d in missing:
        c, total = written.get(d, (0, 0))
        L.append(f"- {fmt_d(d)}：个股文件写入 {c} 只（当日成分股 {total} 只）")
    L.append("")
    L.append(f"从 {fmt_d(first_day)} 到 {fmt_d(last_day)} 整段重算，共 {int((new['trade_date'] >= first_day).sum())} 行"
             f"（其中新补 {len(missing)} 行）；{fmt_d(first_day)} 之前的行一行没动。\n")

    L.append("## 二、原有的日子数字变了多少\n")
    o = old[old['trade_date'] >= first_day].set_index('trade_date')
    n = new.set_index('trade_date').loc[o.index]
    L.append("| 字段 | 有变化的天数 | 最大变化 |")
    L.append("|---|---|---|")
    for col in NUMERIC_COLS:
        diff = (n[col] - o[col]).abs()
        L.append(f"| {col} | {int((diff > 1e-9).sum())} / {len(diff)} | {diff.max():.4g} |")
    L.append("")
    cross = []
    for d in o.index:
        ob, nb = o.at[d, 'breadth'], n.at[d, 'breadth']
        for line, name in [(16, '冰点线 16%'), (79, '过热线 79%')]:
            if (ob < line) != (nb < line):
                cross.append(f"- {fmt_d(d)}：广度 {ob:.2f}% → {nb:.2f}%，跨过了{name}")
    L.append("**广度跨过买卖线的日子**（最可能改变买卖点）：\n")
    L.extend(cross or ["- 没有"])
    L.append("")

    for key, name, logic_col in [('subj', '主观策略', 'logic_state'), ('quant', '量化策略', 'regime')]:
        b, a = before[key], after[key]
        sb, sa = summary(b), summary(a)
        L.append(f"## {'三' if key == 'subj' else '四'}、{name}（看板口径，{DASHBOARD_START[:4]} 年起）\n")
        L.append("| | 补数前 | 补数后 |")
        L.append("|---|---|---|")
        L.append(f"| 累计收益 | {sb['total']:+.2f}% | {sa['total']:+.2f}% |")
        L.append(f"| 最大回撤 | {sb['mdd']:.2f}% | {sa['mdd']:.2f}% |")
        L.append(f"| 交易次数 | {sb['n']} | {sa['n']} |")
        L.append(f"| 胜率 | {sb['win']:.1f}% | {sa['win']:.1f}% |")
        L.append(f"| 最新状态 | {state_text(b, logic_col)}（{fmt_d(b['trade_date'].iloc[-1])}） | "
                 f"{state_text(a, logic_col)}（{fmt_d(a['trade_date'].iloc[-1])}） |")
        L.append("")
        bs = b[b['signal'] != 0][['trade_date', 'signal']]
        as_ = a[a['signal'] != 0][['trade_date', 'signal']]
        bset = {(d, s) for d, s in bs.itertuples(index=False)}
        aset = {(d, s) for d, s in as_.itertuples(index=False)}
        gone = sorted(bset - aset)
        came = sorted(aset - bset)
        word = {1: '买入', -1: '卖出'}
        L.append("买卖信号变化：\n")
        if not gone and not came:
            L.append("- 没有变化：所有买卖信号的日子都和补数前一样")
        for d, s in gone:
            L.append(f"- 消失：{fmt_d(d)} 的{word[s]}信号")
        for d, s in came:
            L.append(f"- 新出现：{fmt_d(d)} 的{word[s]}信号")
        L.append("")
        if key == 'subj':
            bt, at = trade_rows(b), trade_rows(a)
            changed = [t for t in at if t not in bt]
            if changed:
                L.append("补数后与之前不同的交易（信号日 / 逻辑 / 买入日 / 持有天数 / 收益%）：\n")
                for t in changed:
                    L.append(f"- {fmt_d(t[0])} / {t[1]} / {fmt_d(t[2])} / {t[3]} 天 / {t[4]:+.2f}%"
                             f"{'（持仓中）' if t[5] else ''}")
                L.append("")
    return "\n".join(L) + "\n"


def main():
    parser = argparse.ArgumentParser(description='补回 strategy_data.csv 中间漏掉的交易日')
    parser.add_argument('--report', default='backfill_report.md', help='报告输出路径')
    args = parser.parse_args()

    token = os.environ.get('TUSHARE_TOKEN')
    if not token:
        raise RuntimeError("请设置环境变量 TUSHARE_TOKEN")
    import tushare as ts
    ts.set_token(token)
    pro = ts.pro_api()
    run(pro, args.report)


def run(pro, report_path, log=print):
    old = read_strategy()
    last_day = old['trade_date'].iloc[-1]
    open_days = open_trading_days(pro, CALENDAR_START, last_day)
    missing = find_missing_days(old['trade_date'], open_days)
    if not missing:
        text = "# 漏交易日补数预演报告\n\n按交易日历核对，strategy_data.csv 没有缺口，什么都没改。\n"
        open(report_path, 'w', encoding='utf-8').write(text)
        log(text)
        return
    first_day = missing[0]
    log(f"缺 {len(missing)} 天：{missing}；从 {first_day} 起重算到 {last_day}")

    log("拍补数前的策略快照…")
    before = strategy_snapshot(old)

    comp = load_components(COMP_FILE)
    log("补个股文件…")
    written = backfill_stock_files(pro, missing, comp, log)

    log("重算 strategy_data.csv…")
    new = recompute_from(pro, old, first_day, last_day, comp)
    still = find_missing_days(new['trade_date'], open_days)
    if still:
        raise RuntimeError(f"重算后仍缺：{still}")
    new.to_csv(STRATEGY_FILE, index=False)

    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status = json.load(f)
        status['missing_days'] = []
        with open(STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, ensure_ascii=False, indent=2)

    log("拍补数后的策略快照…")
    after = strategy_snapshot(new)

    text = build_report(missing, written, first_day, last_day, old, new, before, after)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(text)
    log(text)


if __name__ == '__main__':
    main()
