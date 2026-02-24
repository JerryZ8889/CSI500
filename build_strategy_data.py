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

# ── 配置 ─────────────────────────────────────────────────────────────────────
TOKEN        = os.environ.get('TUSHARE_TOKEN')
if not TOKEN:
    raise RuntimeError("请设置环境变量 TUSHARE_TOKEN")
DATA_DIR     = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR   = os.path.join(DATA_DIR, 'stocks_data')
COMP_FILE    = os.path.join(DATA_DIR, 'csi500_components_schedule.csv')
OUTPUT_FILE  = os.path.join(DATA_DIR, 'strategy_data.csv')

INDEX_CODE   = '000905.SH'   # 中证500指数（价格/均线/热度数据源）
ETF_CODE     = '510500.SH'   # 510500 ETF（仅用于 etf_turnover）
WARMUP_START = '20181101'    # MA30 + rolling(20) heat_z 均需热身数据
TARGET_START = '20190101'
END_DATE     = datetime.today().strftime('%Y%m%d')

ts.set_token(TOKEN)
pro = ts.pro_api()

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1：拉取中证500指数日线数据（无需复权）
# ═══════════════════════════════════════════════════════════════════════════════
print(f"[1/4] 拉取 {INDEX_CODE} 指数日线 ({WARMUP_START} ~ {END_DATE})...")

idx = pro.index_daily(ts_code=INDEX_CODE, start_date=WARMUP_START, end_date=END_DATE)
idx = idx.sort_values('trade_date').drop_duplicates('trade_date').reset_index(drop=True)
idx = idx[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']].copy()
idx.rename(columns={'vol': 'volume'}, inplace=True)
print(f"    指数日线：{len(idx)} 行，{idx['trade_date'].iloc[0]} ~ {idx['trade_date'].iloc[-1]}")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2：MA5 / MA10 / MA20 / MA30（基于指数 close）
#         heat_z = rolling(20) Z-score of 指数 amount
# ═══════════════════════════════════════════════════════════════════════════════
print("[2/4] 计算均线与 heat_z...")

for n in [5, 10, 20, 30]:
    idx[f'ma_{n}'] = idx['close'].rolling(n).mean()

idx['heat_z'] = (
    (idx['amount'] - idx['amount'].rolling(20).mean())
    / idx['amount'].rolling(20).std()
)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3：breadth（每日成分股收盘价 > MA20 的占比）—— 逻辑不变
# ═══════════════════════════════════════════════════════════════════════════════
print("[3/4] 计算 breadth（基于本地 stocks_data）...")

comp = pd.read_csv(COMP_FILE)
comp['asof_date'] = comp['asof_date'].astype(str).str.strip()
comp['con_code']  = comp['con_code'].str.strip()
asof_sorted = sorted(comp['asof_date'].unique())
print(f"    成分股时间表：{len(comp)} 行，{len(asof_sorted)} 个取样日期")

print(f"    加载 stocks_data/ 中的个股...")
stock_parts = []
for fname in os.listdir(STOCKS_DIR):
    if not fname.endswith('.csv'):
        continue
    ts_code = fname.replace('.csv', '')
    df_s = pd.read_csv(os.path.join(STOCKS_DIR, fname))
    df_s['trade_date'] = df_s['trade_date'].astype(str).str.strip()
    df_s = df_s.sort_values('trade_date').drop_duplicates('trade_date')
    df_s['ma20']    = df_s['close'].rolling(20).mean()
    df_s['ts_code'] = ts_code
    stock_parts.append(df_s[['ts_code', 'trade_date', 'close', 'ma20']])

all_stocks = pd.concat(stock_parts, ignore_index=True)
print(f"    合并后共 {len(all_stocks):,} 条个股日线记录")

target_dates = idx[idx['trade_date'] >= TARGET_START]['trade_date'].tolist()
trade_to_asof = {}
for td in target_dates:
    valid = [d for d in asof_sorted if d <= td]
    if valid:
        trade_to_asof[td] = max(valid)

trade_asof_df = pd.DataFrame({
    'trade_date': list(trade_to_asof.keys()),
    'asof_date' : list(trade_to_asof.values())
})

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
idx['breadth'] = idx['trade_date'].map(breadth_agg.set_index('trade_date')['breadth'])
print(f"    breadth 计算完成，覆盖 {breadth_agg['trade_date'].nunique()} 个交易日")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4：etf_turnover = 510500 ETF vol(手) / fd_share_T-1(万份)
#         仅此字段仍取自 ETF，其余全部来自指数
# ═══════════════════════════════════════════════════════════════════════════════
print(f"[4/4] 计算 etf_turnover（来源：{ETF_CODE}）...")
try:
    time.sleep(0.3)
    etf_daily = pro.fund_daily(ts_code=ETF_CODE, start_date=WARMUP_START, end_date=END_DATE)
    etf_daily = (
        etf_daily[['trade_date', 'vol']]
        .sort_values('trade_date')
        .drop_duplicates('trade_date')
        .rename(columns={'vol': 'etf_vol'})
    )
    time.sleep(0.3)
    etf_share = pro.fund_share(ts_code=ETF_CODE, start_date=WARMUP_START, end_date=END_DATE)
    etf_share = (
        etf_share[['trade_date', 'fd_share']]
        .sort_values('trade_date')
        .drop_duplicates('trade_date')
    )
    idx = idx.merge(etf_daily, on='trade_date', how='left')
    idx = idx.merge(etf_share,  on='trade_date', how='left')
    idx['fd_share']     = idx['fd_share'].ffill()
    idx['fd_share_t1']  = idx['fd_share'].shift(1)
    idx['etf_turnover'] = idx['etf_vol'] / idx['fd_share_t1']
    idx = idx.drop(columns=['etf_vol', 'fd_share', 'fd_share_t1'])
    print(f"    ETF日线 {len(etf_daily)} 行，份额记录 {len(etf_share)} 行")
except Exception as e:
    print(f"    警告：etf_turnover 计算失败（{e}），填 NaN")
    idx['etf_turnover'] = np.nan

# ═══════════════════════════════════════════════════════════════════════════════
# 汇总：截取目标区间，输出 CSV
# ═══════════════════════════════════════════════════════════════════════════════
result = idx[idx['trade_date'] >= TARGET_START].copy()
result = result[[
    'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount',
    'ma_5', 'ma_10', 'ma_20', 'ma_30',
    'breadth', 'heat_z', 'etf_turnover'
]].reset_index(drop=True)

result.to_csv(OUTPUT_FILE, index=False)
print(f"\n完成！共 {len(result)} 行 × 14 列，已保存至：{OUTPUT_FILE}")
print(f"日期范围：{result['trade_date'].iloc[0]} ~ {result['trade_date'].iloc[-1]}")
print("\n前3行预览：")
print(result.head(3).to_string())
print("\n后3行预览：")
print(result.tail(3).to_string())
print("\n各字段空值数：")
print(result.isnull().sum().to_string())
