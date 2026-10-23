#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data.py — strategy_data.csv 各字段的计算口径（全量重建和补数共用）

build_strategy_data.py（从头重建）和 tools/backfill_missing_days.py（从某天起重算）
都从这里算字段，保证两条路算出来的数字是同一个口径。
"""

import os

import numpy as np
import pandas as pd

STRATEGY_COLUMNS = [
    'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount',
    'ma_5', 'ma_10', 'ma_20', 'ma_30',
    'breadth', 'heat_z', 'etf_turnover',
]


def load_stock_parts(stock_dirs):
    """
    读取多个股票目录，并按 ts_code + trade_date 合并。

    当同一只股票同时存在于归档目录和当前目录时：
    - 保留两边全部历史记录
    - 若同一 trade_date 重复，优先使用当前目录中的记录
    """
    code_to_paths = {}
    for priority, stock_dir in enumerate(stock_dirs):
        if not os.path.isdir(stock_dir):
            continue
        for fname in os.listdir(stock_dir):
            if not fname.endswith('.csv'):
                continue
            ts_code = fname.replace('.csv', '')
            code_to_paths.setdefault(ts_code, []).append((priority, os.path.join(stock_dir, fname)))

    stock_parts = []
    for ts_code in sorted(code_to_paths):
        frames = []
        for priority, path in code_to_paths[ts_code]:
            df_s = pd.read_csv(path)
            df_s['trade_date'] = df_s['trade_date'].astype(str).str.strip()
            df_s['close'] = pd.to_numeric(df_s['close'], errors='coerce')
            df_s['source_priority'] = priority
            frames.append(df_s[['trade_date', 'close', 'source_priority']])

        merged = pd.concat(frames, ignore_index=True)
        merged = (
            merged
            .sort_values(['trade_date', 'source_priority'])
            .drop_duplicates('trade_date', keep='last')
            .sort_values('trade_date')
            .reset_index(drop=True)
        )
        merged['ma20'] = merged['close'].rolling(20).mean()
        merged['ts_code'] = ts_code
        stock_parts.append(merged[['ts_code', 'trade_date', 'close', 'ma20']])

    return stock_parts, code_to_paths


def load_components(comp_file):
    comp = pd.read_csv(comp_file)
    comp['asof_date'] = comp['asof_date'].astype(str).str.strip()
    comp['con_code'] = comp['con_code'].str.strip()
    return comp


def asof_for(trade_date, asof_sorted):
    """某个交易日适用的成分股名单日期（不晚于该日的最近一次）。"""
    valid = [d for d in asof_sorted if d <= trade_date]
    return max(valid) if valid else None


def compute_breadth(target_dates, comp, stock_parts):
    """
    每个交易日：当日成分股中收盘价 > 自身 MA20 的占比（%）。
    返回以 trade_date 为索引的 Series；算不出的日子不出现。
    """
    asof_sorted = sorted(comp['asof_date'].unique())
    trade_to_asof = {}
    for td in target_dates:
        asof = asof_for(td, asof_sorted)
        if asof:
            trade_to_asof[td] = asof

    trade_asof_df = pd.DataFrame({
        'trade_date': list(trade_to_asof.keys()),
        'asof_date': list(trade_to_asof.values()),
    })

    all_stocks = pd.concat(stock_parts, ignore_index=True)
    stock_with_asof = all_stocks.merge(trade_asof_df, on='trade_date', how='inner')
    active = stock_with_asof.merge(
        comp[['con_code', 'asof_date']].rename(columns={'con_code': 'ts_code'}),
        on=['ts_code', 'asof_date'],
        how='inner'
    )
    valid_rows = active[active['ma20'].notna() & active['close'].notna()].copy()
    valid_rows['above'] = (valid_rows['close'] > valid_rows['ma20']).astype(int)

    breadth_agg = (
        valid_rows
        .groupby('trade_date')
        .agg(total=('above', 'count'), above_count=('above', 'sum'))
        .reset_index()
    )
    breadth_agg['breadth'] = breadth_agg['above_count'] / breadth_agg['total'] * 100
    return breadth_agg.set_index('trade_date')['breadth']


def add_index_indicators(idx):
    """MA5/10/20/30 基于指数 close；heat_z = 指数成交额的 rolling(20) Z-score。"""
    for n in [5, 10, 20, 30]:
        idx[f'ma_{n}'] = idx['close'].rolling(n).mean()
    idx['heat_z'] = (
        (idx['amount'] - idx['amount'].rolling(20).mean())
        / idx['amount'].rolling(20).std()
    )
    return idx


def add_etf_turnover(idx, etf_daily, etf_share):
    """etf_turnover = 510500 ETF vol(手) / 前一交易日 fd_share(万份)。"""
    etf_daily = (
        etf_daily[['trade_date', 'vol']]
        .sort_values('trade_date')
        .drop_duplicates('trade_date')
        .rename(columns={'vol': 'etf_vol'})
    )
    etf_share = (
        etf_share[['trade_date', 'fd_share']]
        .sort_values('trade_date')
        .drop_duplicates('trade_date')
    )
    idx = idx.merge(etf_daily, on='trade_date', how='left')
    idx = idx.merge(etf_share, on='trade_date', how='left')
    idx['fd_share'] = idx['fd_share'].ffill()
    idx['fd_share_t1'] = idx['fd_share'].shift(1)
    idx['etf_turnover'] = idx['etf_vol'] / idx['fd_share_t1']
    return idx.drop(columns=['etf_vol', 'fd_share', 'fd_share_t1'])


def open_trading_days(pro, start_date, end_date):
    """交易所日历里 [start, end] 的开市日，升序。"""
    cal = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
    if cal is None or cal.empty:
        return []
    return sorted(cal['cal_date'].astype(str).unique())


def find_missing_days(strategy_dates, open_days):
    """日历里开市、但 strategy_data.csv 没有的日子（只看表内首尾之间的缺口）。"""
    have = set(str(d) for d in strategy_dates)
    if not have:
        return []
    first, last = min(have), max(have)
    return [d for d in open_days if first <= d <= last and d not in have]


def adjusted_stock_row(ts_code, trade_date, daily_row, adj_factor_today, base_factor):
    """把一行原始日线换成后复权口径，返回写进个股文件的一行文本。"""
    adj_close = daily_row['close'] * adj_factor_today / base_factor
    adj_pre_close = daily_row['pre_close'] * adj_factor_today / base_factor
    return (f"{ts_code},{trade_date},{adj_close},{adj_pre_close},"
            f"{daily_row['vol']},{daily_row['amount']}\n")


def insert_stock_row(csv_path, trade_date, row_text):
    """
    把一行写进个股文件，保持“新日期在上”的降序；已有该日则不动。
    返回 True 表示写入了新行。
    """
    header = "ts_code,trade_date,close,pre_close,vol,amount\n"
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', encoding='utf-8-sig') as f:
            f.write(header)
            f.write(row_text)
        return True

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    body = [ln if ln.endswith('\n') else ln + '\n' for ln in lines[1:] if ln.strip()]
    dates = [ln.split(',', 2)[1].strip() for ln in body]
    if trade_date in dates:
        return False
    pos = 0
    while pos < len(dates) and dates[pos] > trade_date:
        pos += 1
    body.insert(pos, row_text)
    with open(csv_path, 'w', encoding='utf-8-sig') as f:
        f.writelines([lines[0]] + body)
    return True
