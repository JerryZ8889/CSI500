#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
daily_update.py — CSI500 每日数据增量更新管道

功能：
  Task 1: 生成 adj_factor_base.csv（一次性）
  Task 2: 每日增量数据更新（17:30 触发）
  Task 3: 成分股调整检查（月初 / 半年度）

用法：
  python daily_update.py                    # 执行每日更新（默认今日）
  python daily_update.py --date 20260214    # 指定日期
  python daily_update.py --generate-base    # 仅生成 adj_factor_base.csv
  python daily_update.py --check-rebalance  # 强制检查成分股调整
"""

import tushare as ts
import pandas as pd
import numpy as np
import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

# ── 路径配置 ─────────────────────────────────────────────────────────────────
DATA_DIR      = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR    = os.path.join(DATA_DIR, 'stocks_data')
COMP_FILE     = os.path.join(DATA_DIR, 'csi500_components_schedule.csv')
STRATEGY_FILE = os.path.join(DATA_DIR, 'strategy_data.csv')
ADJ_BASE_FILE = os.path.join(DATA_DIR, 'adj_factor_base.csv')
STATUS_FILE   = os.path.join(DATA_DIR, 'update_status.json')
LOG_FILE      = os.path.join(DATA_DIR, 'daily_update.log')

# ── Tushare 配置 ─────────────────────────────────────────────────────────────
TOKEN      = os.environ.get('TUSHARE_TOKEN')
if not TOKEN:
    raise RuntimeError("请设置环境变量 TUSHARE_TOKEN（本地: set TUSHARE_TOKEN=xxx / CI: GitHub Secrets）")
INDEX_CODE = '000905.SH'
ETF_CODE   = '510500.SH'
API_SLEEP  = 0.35

# ── 重试配置 ─────────────────────────────────────────────────────────────────
RETRY_INITIAL_COUNT = 3
RETRY_INITIAL_WAIT  = 600     # 10 分钟
RETRY_EXTENDED_WAIT = 1800    # 30 分钟

# ── 复权基准日 ───────────────────────────────────────────────────────────────
ADJ_BASE_DATE = '20260213'

# ── 时区 ──────────────────────────────────────────────────────────────────────
TZ_BEIJING = timezone(timedelta(hours=8))

# ── Tushare 初始化 ───────────────────────────────────────────────────────────
ts.set_token(TOKEN)
pro = ts.pro_api()


# ═══════════════════════════════════════════════════════════════════════════════
# 基础工具
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    """配置双输出日志：文件 + 控制台"""
    logger = logging.getLogger('daily_update')
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
    fh.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    ))
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def load_status():
    """读取 update_status.json"""
    default = {
        'last_update_date': None,
        'last_update_time': None,
        'status': 'idle',
        'retry_count': 0,
        'error_message': None,
        'last_fd_share_date': None,     # 保留字段兼容旧状态文件
        'last_fd_share_value': None,
        'last_rebalance_check': None,
    }
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            stored = json.load(f)
        default.update(stored)
    return default


def save_status(status):
    """写入 update_status.json"""
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status, f, ensure_ascii=False, indent=2)


def api_call(func, **kwargs):
    """封装 Tushare API 调用，自动 rate limit sleep"""
    time.sleep(API_SLEEP)
    result = func(**kwargs)
    if result is None or result.empty:
        return None
    return result


def get_previous_trading_day(logger):
    """从 strategy_data.csv 获取前一个交易日"""
    df = pd.read_csv(STRATEGY_FILE, usecols=['trade_date'])
    df['trade_date'] = df['trade_date'].astype(str)
    last_date = df['trade_date'].iloc[-1]
    logger.info(f"前一交易日: {last_date}")
    return last_date


# ═══════════════════════════════════════════════════════════════════════════════
# Task 1: 生成 adj_factor_base.csv（一次性）
# ═══════════════════════════════════════════════════════════════════════════════

def generate_adj_factor_base(logger):
    """
    一次性任务：拉取全市场 20260213 复权因子 → adj_factor_base.csv
    """
    if os.path.exists(ADJ_BASE_FILE):
        logger.info(f"adj_factor_base.csv 已存在 ({ADJ_BASE_FILE})")
        return pd.read_csv(ADJ_BASE_FILE)

    logger.info(f"生成 adj_factor_base.csv (基准日: {ADJ_BASE_DATE})...")
    df = api_call(pro.adj_factor, trade_date=ADJ_BASE_DATE)
    if df is None:
        raise RuntimeError(f"无法获取 {ADJ_BASE_DATE} 的复权因子")

    df = df[['ts_code', 'adj_factor']].drop_duplicates('ts_code')
    df.to_csv(ADJ_BASE_FILE, index=False)
    logger.info(f"已保存 adj_factor_base.csv: {len(df)} 只股票")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Task 2: 每日数据更新
# ═══════════════════════════════════════════════════════════════════════════════

def is_trading_day(today_str, logger):
    """检测是否为交易日（通过指数日线是否有数据判断）"""
    result = api_call(pro.index_daily,
                      ts_code=INDEX_CODE,
                      start_date=today_str,
                      end_date=today_str)
    if result is None:
        logger.info(f"{today_str} 非交易日")
        return False, None
    logger.info(f"{today_str} 是交易日")
    return True, result


def fetch_daily_data(today_str, index_daily, status, logger):
    """
    拉取今日全部 API 数据，带重试逻辑。
    index_daily 已在 is_trading_day 中获取，直接传入避免重复调用。

    返回 dict 包含：
      index_daily, stock_daily, adj_factor, etf_daily, fd_share
    """
    yesterday_str = get_previous_trading_day(logger)
    retry_count = 0

    while True:
        try:
            status['status'] = 'retrying' if retry_count > 0 else 'running'
            status['retry_count'] = retry_count
            save_status(status)
            logger.info(f"拉取数据 (第 {retry_count + 1} 次)...")

            # index_daily 已有，验证字段
            row = index_daily.iloc[0]
            for col in ['open', 'high', 'low', 'close', 'vol', 'amount']:
                if pd.isna(row[col]):
                    raise ValueError(f"index_daily.{col} 为空")

            # 全市场个股日线
            stock_daily = api_call(pro.daily, trade_date=today_str)
            if stock_daily is None:
                raise ValueError("stock_daily 返回空")

            # 全市场复权因子
            adj_factor = api_call(pro.adj_factor, trade_date=today_str)
            if adj_factor is None:
                raise ValueError("adj_factor 返回空")

            # ETF 日线
            etf_daily = api_call(pro.fund_daily,
                                 ts_code=ETF_CODE,
                                 start_date=today_str,
                                 end_date=today_str)
            if etf_daily is None:
                raise ValueError("etf_daily 返回空")
            if pd.isna(etf_daily.iloc[0]['vol']):
                raise ValueError("etf_daily.vol 为空")

            # ETF 份额 (T-1)
            fd_share_val = None
            fd_share_df = api_call(pro.fund_share,
                                    ts_code=ETF_CODE,
                                    start_date=yesterday_str,
                                    end_date=yesterday_str)
            if fd_share_df is not None and not fd_share_df.empty:
                val = fd_share_df.iloc[0]['fd_share']
                if not pd.isna(val):
                    fd_share_val = val

            if fd_share_val is None:
                raise ValueError(f"fd_share({yesterday_str}) 为空")

            return {
                'index_daily': index_daily,
                'stock_daily': stock_daily,
                'adj_factor': adj_factor,
                'etf_daily': etf_daily,
                'fd_share': fd_share_val,
            }

        except Exception as e:
            retry_count += 1
            status['retry_count'] = retry_count
            status['error_message'] = str(e)
            status['status'] = 'retrying'
            save_status(status)

            if retry_count <= RETRY_INITIAL_COUNT:
                wait = RETRY_INITIAL_WAIT
                logger.warning(f"数据拉取失败: {e}. "
                               f"第 {retry_count}/{RETRY_INITIAL_COUNT} 次重试, "
                               f"等待 {wait // 60} 分钟...")
            else:
                wait = RETRY_EXTENDED_WAIT
                logger.warning(f"数据拉取失败: {e}. "
                               f"扩展重试第 {retry_count} 次, "
                               f"等待 {wait // 60} 分钟...")
            time.sleep(wait)

            # 重试时也要重新拉取 index_daily（可能之前的已过期）
            new_idx = api_call(pro.index_daily,
                               ts_code=INDEX_CODE,
                               start_date=today_str,
                               end_date=today_str)
            if new_idx is not None:
                index_daily = new_idx


def update_stocks_data(today_str, stock_daily, adj_factor_df, adj_base, logger):
    """
    更新 stocks_data/ 中当前 CSI500 成分股的日线数据。
    返回今日 breadth 值。
    """
    # 获取当前成分股列表
    comp = pd.read_csv(COMP_FILE)
    comp['asof_date'] = comp['asof_date'].astype(str).str.strip()
    comp['con_code'] = comp['con_code'].str.strip()

    asof_dates = sorted(comp['asof_date'].unique())
    valid_asof = [d for d in asof_dates if d <= today_str]
    if not valid_asof:
        raise RuntimeError("无法找到有效的成分股日期")
    current_asof = max(valid_asof)

    current_components = comp[comp['asof_date'] == current_asof]['con_code'].unique().tolist()
    logger.info(f"当前成分股 {len(current_components)} 只 (asof={current_asof})")

    # 建立查找索引
    stock_idx = stock_daily.set_index('ts_code')
    adj_idx = adj_factor_df.set_index('ts_code')
    base_idx = adj_base.set_index('ts_code')

    above_ma20_count = 0
    valid_count = 0
    updated_count = 0

    for ts_code in current_components:
        csv_path = os.path.join(STOCKS_DIR, f'{ts_code}.csv')

        # 获取原始日线数据
        if ts_code not in stock_idx.index:
            continue
        row_daily = stock_idx.loc[ts_code]
        if isinstance(row_daily, pd.DataFrame):
            row_daily = row_daily.iloc[0]

        raw_close = row_daily['close']
        raw_pre_close = row_daily['pre_close']
        raw_vol = row_daily['vol']
        raw_amount = row_daily['amount']

        # 获取今日复权因子
        if ts_code not in adj_idx.index:
            continue
        adj_row = adj_idx.loc[ts_code]
        if isinstance(adj_row, pd.DataFrame):
            adj_row = adj_row.iloc[0]
        adj_factor_today = adj_row['adj_factor']

        # 获取基准复权因子
        if ts_code in base_idx.index:
            latest_factor = base_idx.loc[ts_code, 'adj_factor']
            if isinstance(latest_factor, pd.Series):
                latest_factor = latest_factor.iloc[0]
        else:
            latest_factor = adj_factor_today
            logger.warning(f"{ts_code} 不在 adj_factor_base 中，使用今日因子")

        # 计算后复权价
        adj_close = raw_close * adj_factor_today / latest_factor
        adj_pre_close = raw_pre_close * adj_factor_today / latest_factor

        # 追加到 CSV（降序：新行插入 header 后）
        new_row = (f"{ts_code},{today_str},{adj_close},{adj_pre_close},"
                   f"{raw_vol},{raw_amount}\n")

        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            # 防重复
            if len(lines) > 1 and today_str in lines[1]:
                pass  # 已有今日数据
            else:
                lines.insert(1, new_row)
                with open(csv_path, 'w', encoding='utf-8-sig') as f:
                    f.writelines(lines)
                updated_count += 1
        else:
            header = "ts_code,trade_date,close,pre_close,vol,amount\n"
            with open(csv_path, 'w', encoding='utf-8-sig') as f:
                f.write(header)
                f.write(new_row)
            updated_count += 1

        # 计算 MA20 并判断 close > MA20
        if os.path.exists(csv_path):
            df_stock = pd.read_csv(csv_path, nrows=21, encoding='utf-8-sig')
            df_stock['close'] = pd.to_numeric(df_stock['close'], errors='coerce')
            closes = df_stock['close'].dropna().values

            if len(closes) >= 20:
                ma20 = closes[:20].mean()
                valid_count += 1
                if closes[0] > ma20:
                    above_ma20_count += 1
            elif len(closes) > 0:
                valid_count += 1

    breadth = (above_ma20_count / valid_count * 100) if valid_count > 0 else np.nan
    logger.info(f"stocks_data 更新 {updated_count} 个文件")
    logger.info(f"breadth = {breadth:.2f}% ({above_ma20_count}/{valid_count})")
    return breadth


def calculate_and_append_strategy_row(today_str, data, breadth, logger):
    """
    计算今日的 MA/heat_z/etf_turnover，追加一行到 strategy_data.csv。
    """
    strategy = pd.read_csv(STRATEGY_FILE)
    strategy['trade_date'] = strategy['trade_date'].astype(str)

    # 防重复
    if today_str in strategy['trade_date'].values:
        logger.warning(f"{today_str} 已存在于 strategy_data.csv，跳过")
        return

    # 今日指数数据
    idx_row = data['index_daily'].iloc[0]
    today_close  = idx_row['close']
    today_amount = idx_row['amount']

    # MA5/10/20/30
    closes = strategy['close'].tolist() + [today_close]
    ma_5  = np.mean(closes[-5:])  if len(closes) >= 5  else np.nan
    ma_10 = np.mean(closes[-10:]) if len(closes) >= 10 else np.nan
    ma_20 = np.mean(closes[-20:]) if len(closes) >= 20 else np.nan
    ma_30 = np.mean(closes[-30:]) if len(closes) >= 30 else np.nan

    # heat_z（rolling 20 Z-score of amount, ddof=1）
    amounts = strategy['amount'].tolist() + [today_amount]
    if len(amounts) >= 20:
        recent_20 = amounts[-20:]
        mean_20 = np.mean(recent_20)
        std_20 = np.std(recent_20, ddof=1)
        heat_z = (today_amount - mean_20) / std_20 if std_20 > 0 else 0.0
    else:
        heat_z = np.nan

    # etf_turnover
    etf_vol = data['etf_daily'].iloc[0]['vol']
    fd_share = data['fd_share']
    etf_turnover = (etf_vol / fd_share) if fd_share and fd_share > 0 else np.nan

    # 构造新行
    new_row = pd.DataFrame([{
        'trade_date':   today_str,
        'open':         idx_row['open'],
        'high':         idx_row['high'],
        'low':          idx_row['low'],
        'close':        today_close,
        'volume':       idx_row['vol'],
        'amount':       today_amount,
        'ma_5':         ma_5,
        'ma_10':        ma_10,
        'ma_20':        ma_20,
        'ma_30':        ma_30,
        'breadth':      breadth,
        'heat_z':       heat_z,
        'etf_turnover': etf_turnover,
    }])

    new_row.to_csv(STRATEGY_FILE, mode='a', header=False, index=False)

    logger.info(f"已追加 strategy_data.csv: {today_str} | "
                f"close={today_close:.2f} ma30={ma_30:.2f} "
                f"breadth={breadth:.1f}% heat_z={heat_z:.2f} "
                f"turnover={'%.2f' % etf_turnover if not np.isnan(etf_turnover) else 'NaN'}")


# ═══════════════════════════════════════════════════════════════════════════════
# Task 3: 成分股调整检查
# ═══════════════════════════════════════════════════════════════════════════════

def check_component_rebalance(today_str, adj_base, logger, force=False):
    """
    检查并执行成分股调整。
    触发条件：每月1日 / 6月12月第三个周一，或 force=True。
    """
    today = datetime.strptime(today_str, '%Y%m%d')
    month = today.month
    day = today.day
    weekday = today.weekday()  # 0=Monday

    is_monthly_1st = (day == 1)
    is_third_monday = (weekday == 0 and 15 <= day <= 21 and month in [6, 12])

    if not force and not is_monthly_1st and not is_third_monday:
        return False

    logger.info("=" * 40)
    logger.info("触发成分股调整检查")
    logger.info("=" * 40)

    # 拉取最新权重
    weight_df = api_call(pro.index_weight, index_code=INDEX_CODE)
    if weight_df is None:
        logger.warning("无法获取最新成分股权重，跳过")
        return False

    latest_trade_date = weight_df['trade_date'].max()
    new_components = weight_df[weight_df['trade_date'] == latest_trade_date].copy()
    new_codes = set(new_components['con_code'].str.strip())

    # 现有成分股
    comp = pd.read_csv(COMP_FILE)
    comp['asof_date'] = comp['asof_date'].astype(str).str.strip()
    comp['con_code'] = comp['con_code'].str.strip()
    latest_asof = max(comp['asof_date'].unique())
    old_codes = set(comp[comp['asof_date'] == latest_asof]['con_code'])

    added_codes = new_codes - old_codes
    removed_codes = old_codes - new_codes

    if not added_codes and not removed_codes:
        logger.info("成分股无变化")
        return False

    logger.info(f"成分股变动: 新增 {len(added_codes)} 只, 移除 {len(removed_codes)} 只")
    if added_codes:
        logger.info(f"  新增: {sorted(added_codes)[:10]}{'...' if len(added_codes) > 10 else ''}")
    if removed_codes:
        logger.info(f"  移除: {sorted(removed_codes)[:10]}{'...' if len(removed_codes) > 10 else ''}")

    # 新的 asof_date
    new_asof = today_str

    # 追加到 schedule
    new_rows = []
    for _, row in new_components.iterrows():
        new_rows.append({
            'index_code': INDEX_CODE,
            'trade_date': latest_trade_date,
            'con_code': row['con_code'].strip(),
            'weight': row['weight'],
            'asof_date': new_asof,
        })

    pd.DataFrame(new_rows).to_csv(COMP_FILE, mode='a', header=False, index=False)
    logger.info(f"已追加 {len(new_rows)} 条成分股记录 (asof={new_asof})")

    # 新增股票回填
    if added_codes:
        backfill_new_stocks(added_codes, today_str, adj_base, logger)

    return True


def backfill_new_stocks(new_codes, today_str, adj_base, logger):
    """为新增成分股回填 35 个交易日的后复权数据"""
    adj_base_idx = adj_base.set_index('ts_code')

    # 35 个交易日 ≈ 往前 60 自然日
    today = datetime.strptime(today_str, '%Y%m%d')
    start_date = (today - timedelta(days=60)).strftime('%Y%m%d')

    for ts_code in sorted(new_codes):
        logger.info(f"  回填 {ts_code} ({start_date} ~ {today_str})...")

        daily = api_call(pro.daily, ts_code=ts_code,
                         start_date=start_date, end_date=today_str)
        if daily is None:
            logger.warning(f"  {ts_code} 无日线数据，跳过")
            continue

        adj = api_call(pro.adj_factor, ts_code=ts_code,
                       start_date=start_date, end_date=today_str)
        if adj is None:
            logger.warning(f"  {ts_code} 无复权因子，跳过")
            continue

        merged = daily.merge(adj[['ts_code', 'trade_date', 'adj_factor']],
                             on=['ts_code', 'trade_date'], how='left')

        # 确定 latest_factor
        if ts_code in adj_base_idx.index:
            latest_factor = adj_base_idx.loc[ts_code, 'adj_factor']
            if isinstance(latest_factor, pd.Series):
                latest_factor = latest_factor.iloc[0]
        else:
            # 新股（上市晚于基准日）
            latest_factor = merged.sort_values('trade_date', ascending=False)['adj_factor'].iloc[0]
            # 追加到 adj_factor_base.csv
            with open(ADJ_BASE_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{ts_code},{latest_factor}\n")
            logger.info(f"  新股 {ts_code} latest_factor={latest_factor} 已追加到 base")

        # 计算后复权价
        merged['adj_close'] = merged['close'] * merged['adj_factor'] / latest_factor
        merged['adj_pre_close'] = merged['pre_close'] * merged['adj_factor'] / latest_factor

        # 只保留最近 35 个交易日
        merged = merged.sort_values('trade_date', ascending=False).head(35)

        output = pd.DataFrame({
            'ts_code': merged['ts_code'],
            'trade_date': merged['trade_date'],
            'close': merged['adj_close'],
            'pre_close': merged['adj_pre_close'],
            'vol': merged['vol'],
            'amount': merged['amount'],
        })

        csv_path = os.path.join(STOCKS_DIR, f'{ts_code}.csv')
        if os.path.exists(csv_path):
            existing = pd.read_csv(csv_path, encoding='utf-8-sig')
            existing['trade_date'] = existing['trade_date'].astype(str)
            combined = pd.concat([output, existing]).drop_duplicates(
                subset=['ts_code', 'trade_date'], keep='first'
            )
            combined = combined.sort_values('trade_date', ascending=False)
            combined.to_csv(csv_path, index=False, encoding='utf-8-sig')
        else:
            output.to_csv(csv_path, index=False, encoding='utf-8-sig')

        logger.info(f"  {ts_code}: 回填 {len(merged)} 天")


# ═══════════════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════════════

def run_daily_update(today_str=None):
    """主入口：执行每日更新全流程"""
    logger = setup_logging()

    if today_str is None:
        today_str = datetime.now(TZ_BEIJING).strftime('%Y%m%d')

    logger.info("=" * 60)
    logger.info(f"每日更新启动: {today_str}")
    logger.info("=" * 60)

    status = load_status()

    # 防止同日重复运行
    if status.get('last_update_date') == today_str and status.get('status') == 'success':
        logger.info(f"{today_str} 已成功更新，跳过")
        return

    try:
        # Step 0: 确保 adj_factor_base.csv 存在
        adj_base = generate_adj_factor_base(logger)

        # Step 1: 检查是否为交易日
        is_td, index_daily = is_trading_day(today_str, logger)
        if not is_td:
            status['last_update_time'] = datetime.now(TZ_BEIJING).strftime('%Y-%m-%d %H:%M:%S')
            status['status'] = 'idle'
            status['error_message'] = f'{today_str} 非交易日'
            save_status(status)
            return

        # Step 2: 拉取所有 API 数据（含重试）
        data = fetch_daily_data(today_str, index_daily, status, logger)

        # Step 3: 更新 stocks_data/ 并计算 breadth
        breadth = update_stocks_data(
            today_str, data['stock_daily'], data['adj_factor'], adj_base, logger
        )

        # Step 4: 计算指标并追加 strategy_data.csv
        calculate_and_append_strategy_row(today_str, data, breadth, logger)

        # Step 5: 成分股调整检查
        check_component_rebalance(today_str, adj_base, logger)

        # 更新状态
        status['last_update_date'] = today_str
        status['last_update_time'] = datetime.now(TZ_BEIJING).strftime('%Y-%m-%d %H:%M:%S')
        status['status'] = 'success'
        status['retry_count'] = 0
        status['error_message'] = None
        save_status(status)

        logger.info("=" * 60)
        logger.info(f"每日更新完成: {today_str}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"更新失败: {e}", exc_info=True)
        status['status'] = 'failed'
        status['error_message'] = str(e)
        status['last_update_time'] = datetime.now(TZ_BEIJING).strftime('%Y-%m-%d %H:%M:%S')
        save_status(status)
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CSI500 每日数据更新')
    parser.add_argument('--date', type=str, default=None,
                        help='指定更新日期 (YYYYMMDD)，默认今日')
    parser.add_argument('--generate-base', action='store_true',
                        help='仅生成 adj_factor_base.csv')
    parser.add_argument('--check-rebalance', action='store_true',
                        help='强制执行成分股调整检查')
    args = parser.parse_args()

    if args.generate_base:
        log = setup_logging()
        generate_adj_factor_base(log)
    elif args.check_rebalance:
        log = setup_logging()
        base = generate_adj_factor_base(log)
        td = args.date or datetime.now(TZ_BEIJING).strftime('%Y%m%d')
        check_component_rebalance(td, base, log, force=True)
    else:
        run_daily_update(args.date)
