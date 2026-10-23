#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_strategy_data.py  v5
价格/均线/热度 数据源改为中证500指数（000905.SH），无需复权。
ETF换手率（etf_turnover）仍取自 510500.SH ETF。

字段来源：
  open/high/low/close  -- 000905.SH 指数日线（天然全收益，无需复权）
  volume/amount        -- 000905.SH 指数成交量/成交额
  ma_5/10/20/30        -- 基于指数 close 的滚动均线
  breadth              -- 每日成分股收盘价 > MA20 的占比（来自本地 stocks_data）
  heat_z               -- 000905.SH 指数成交额的 rolling(20) Z-score
  etf_turnover         -- 510500 ETF vol(手) / fd_share_T-1(万份)
"""

import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime
import os
import time

from market_data import (
    STRATEGY_COLUMNS,
    add_etf_turnover,
    add_index_indicators,
    compute_breadth,
    load_components,
    load_stock_parts,
)

# ── 配置 ─────────────────────────────────────────────────────────────────────
DATA_DIR     = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR   = os.path.join(DATA_DIR, 'stocks_data')
ARCHIVE_DIR  = os.path.join(DATA_DIR, 'stocks_archive')
COMP_FILE    = os.path.join(DATA_DIR, 'csi500_components_schedule.csv')
OUTPUT_FILE  = os.path.join(DATA_DIR, 'strategy_data.csv')

INDEX_CODE   = '000905.SH'   # 中证500指数（价格/均线/热度数据源）
ETF_CODE     = '510500.SH'   # 510500 ETF（仅用于 etf_turnover）
WARMUP_START = '20181101'    # MA30 + rolling(20) heat_z 均需热身数据
TARGET_START = '20190101'


def main():
    token = os.environ.get('TUSHARE_TOKEN')
    if not token:
        raise RuntimeError("请设置环境变量 TUSHARE_TOKEN")
    ts.set_token(token)
    pro = ts.pro_api()
    end_date = datetime.today().strftime('%Y%m%d')

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1：拉取中证500指数日线数据（无需复权）
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"[1/4] 拉取 {INDEX_CODE} 指数日线 ({WARMUP_START} ~ {end_date})...")

    idx = pro.index_daily(ts_code=INDEX_CODE, start_date=WARMUP_START, end_date=end_date)
    idx = idx.sort_values('trade_date').drop_duplicates('trade_date').reset_index(drop=True)
    idx = idx[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']].copy()
    idx.rename(columns={'vol': 'volume'}, inplace=True)
    print(f"    指数日线：{len(idx)} 行，{idx['trade_date'].iloc[0]} ~ {idx['trade_date'].iloc[-1]}")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2：MA5 / MA10 / MA20 / MA30（基于指数 close）
    #         heat_z = rolling(20) Z-score of 指数 amount
    # ═══════════════════════════════════════════════════════════════════════════
    print("[2/4] 计算均线与 heat_z...")
    idx = add_index_indicators(idx)

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3：breadth（每日成分股收盘价 > MA20 的占比）
    # ═══════════════════════════════════════════════════════════════════════════
    print("[3/4] 计算 breadth（基于本地 stocks_data）...")

    comp = load_components(COMP_FILE)
    print(f"    成分股时间表：{len(comp)} 行，{comp['asof_date'].nunique()} 个取样日期")

    stock_dirs = [d for d in [ARCHIVE_DIR, STOCKS_DIR] if os.path.isdir(d)]
    print(f"    加载股票目录: {', '.join(os.path.basename(d) for d in stock_dirs)}")
    stock_parts, code_to_paths = load_stock_parts(stock_dirs)
    print(f"    唯一个股文件集合: {len(code_to_paths)} 只")

    target_dates = idx[idx['trade_date'] >= TARGET_START]['trade_date'].tolist()
    breadth = compute_breadth(target_dates, comp, stock_parts)
    idx['breadth'] = idx['trade_date'].map(breadth)
    print(f"    breadth 计算完成，覆盖 {breadth.index.nunique()} 个交易日")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4：etf_turnover = 510500 ETF vol(手) / fd_share_T-1(万份)
    #         仅此字段仍取自 ETF，其余全部来自指数
    # ═══════════════════════════════════════════════════════════════════════════
    print(f"[4/4] 计算 etf_turnover（来源：{ETF_CODE}）...")
    try:
        time.sleep(0.3)
        etf_daily = pro.fund_daily(ts_code=ETF_CODE, start_date=WARMUP_START, end_date=end_date)
        time.sleep(0.3)
        etf_share = pro.fund_share(ts_code=ETF_CODE, start_date=WARMUP_START, end_date=end_date)
        idx = add_etf_turnover(idx, etf_daily, etf_share)
        print(f"    ETF日线 {len(etf_daily)} 行，份额记录 {len(etf_share)} 行")
    except Exception as e:
        print(f"    警告：etf_turnover 计算失败（{e}），填 NaN")
        idx['etf_turnover'] = np.nan

    # ═══════════════════════════════════════════════════════════════════════════
    # 汇总：截取目标区间，输出 CSV
    # ═══════════════════════════════════════════════════════════════════════════
    result = idx[idx['trade_date'] >= TARGET_START].copy()
    result = result[STRATEGY_COLUMNS].reset_index(drop=True)

    result.to_csv(OUTPUT_FILE, index=False)
    print(f"\n完成！共 {len(result)} 行 × 14 列，已保存至：{OUTPUT_FILE}")
    print(f"日期范围：{result['trade_date'].iloc[0]} ~ {result['trade_date'].iloc[-1]}")
    print("\n前3行预览：")
    print(result.head(3).to_string())
    print("\n后3行预览：")
    print(result.tail(3).to_string())
    print("\n各字段空值数：")
    print(result.isnull().sum().to_string())


if __name__ == '__main__':
    main()
