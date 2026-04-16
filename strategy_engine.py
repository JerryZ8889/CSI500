#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared strategy engine for the CSI500 project.

This module centralizes the current signal, execution, and summary logic
without changing the existing behavior of the dashboard or backtest scripts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_strategy_frame(df: pd.DataFrame, cost: float = 0.001) -> pd.DataFrame:
    """
    Apply the current CSI500 strategy logic to a prepared strategy_data frame.

    The implementation intentionally matches the existing dashboard/backtest
    behavior, including signal timing, T+1 execution, exit reasons, and
    strategy/benchmark NAV calculations.
    """
    df = df.copy()
    df["trade_date"] = df["trade_date"].astype(str)
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    df["is_up"] = (df["close"] > df["close"].shift(1)).astype(int)
    df["streak_group"] = (df["is_up"] != df["is_up"].shift(1)).cumsum()
    df["streak"] = df.groupby("streak_group").cumcount() + 1
    df["consec_gains"] = np.where(df["is_up"] == 1, df["streak"], 0)

    n = len(df)
    signals = np.zeros(n, dtype=int)
    position = 0
    logic_state = None
    entry_idx = 0
    entry_high = 0.0
    logic_states = [None] * n
    exit_reasons = [None] * n
    entry_highs = [np.nan] * n
    entry_idxs = [np.nan] * n

    for i in range(1, n):
        curr_c = df["close"].iloc[i]
        prev_c = df["close"].iloc[i - 1]
        ma_f = df["ma_30"].iloc[i]
        ma_t = df["ma_10"].iloc[i]
        ma_s = df["ma_5"].iloc[i]
        brd = df["breadth"].iloc[i]
        hz = df["heat_z"].iloc[i]
        turn = df["etf_turnover"].iloc[i]
        prev_cg = df["consec_gains"].iloc[i - 1]

        if position == 0:
            if brd < 16:
                signals[i] = 1
                logic_state = "Composite"
                entry_idx = i
                entry_high = df["high"].iloc[i]
                position = 1
            elif (
                curr_c > ma_t
                and prev_cg >= 3
                and curr_c < prev_c
                and turn > 1.0
                and curr_c > ma_s
                and curr_c > ma_f
            ):
                signals[i] = 1
                logic_state = "FirstNeg"
                entry_idx = i
                entry_high = df["high"].iloc[i]
                position = 1
        else:
            if logic_state == "FirstNeg" and brd < 16:
                logic_state = "Composite"

            cond_common = (brd > 79) and (hz < 1.5)
            if cond_common:
                signals[i] = -1
                exit_reasons[i] = "overheating"
                position = 0
                logic_state = None
            elif logic_state == "FirstNeg":
                is_1d = curr_c < prev_c
                is_below_ma = curr_c < ma_f
                held_days = i - entry_idx
                if held_days >= 5:
                    closes_in_period = df["close"].iloc[entry_idx:i + 1]
                    is_5d = not (closes_in_period > entry_high).any()
                else:
                    is_5d = False
                if is_below_ma and (is_1d or is_5d):
                    signals[i] = -1
                    exit_reasons[i] = "trend_break" if is_1d else "time_stop"
                    position = 0
                    logic_state = None

        logic_states[i] = logic_state
        if position == 1:
            entry_highs[i] = entry_high
            entry_idxs[i] = entry_idx

    df["signal"] = signals
    df["logic_state"] = logic_states
    df["exit_reason"] = exit_reasons
    df["entry_high_val"] = entry_highs
    df["entry_idx_val"] = entry_idxs

    actual_pos = np.zeros(n, dtype=int)
    pos = 0
    for i in range(n):
        if i > 0 and signals[i - 1] == 1:
            pos = 1
        elif i > 0 and signals[i - 1] == -1:
            pos = 0
        actual_pos[i] = pos
    df["actual_pos"] = actual_pos

    strat_ret = np.zeros(n)
    for i in range(1, n):
        if actual_pos[i] == 1 and actual_pos[i - 1] == 0:
            strat_ret[i] = (df["close"].iloc[i] / df["open"].iloc[i] - 1) - cost
        elif actual_pos[i] == 0 and actual_pos[i - 1] == 1:
            strat_ret[i] = (df["open"].iloc[i] / df["close"].iloc[i - 1] - 1) - cost
        elif actual_pos[i] == 1 and actual_pos[i - 1] == 1:
            strat_ret[i] = df["close"].iloc[i] / df["close"].iloc[i - 1] - 1

    df["strat_ret"] = strat_ret
    df["strat_nav"] = (1 + df["strat_ret"]).cumprod()
    df["strat_nav"] /= df["strat_nav"].iloc[0]

    df["bench_ret"] = df["close"].pct_change().fillna(0)
    df["bench_nav"] = (1 + df["bench_ret"]).cumprod()
    df["bench_nav"] /= df["bench_nav"].iloc[0]

    return df


def compute_virtual_firstneg(df: pd.DataFrame) -> dict[str, object]:
    """Track the virtual FirstNeg lifecycle during an active Composite hold."""
    empty = {
        "active": False,
        "entry_date": None,
        "entry_high": None,
        "held_days": 0,
        "exit_signal": None,
    }
    n = len(df)
    if n < 2:
        return empty
    last_idx = n - 1

    if df["actual_pos"].iloc[last_idx] != 1:
        return empty
    if df["logic_state"].iloc[last_idx] != "Composite":
        return empty

    scan_start = last_idx
    while scan_start > 0 and df["actual_pos"].iloc[scan_start - 1] == 1:
        scan_start -= 1

    v_active = False
    v_entry_idx = 0
    v_entry_high = 0.0
    v_exit_signal = None

    for i in range(max(scan_start, 1), n):
        curr_c = df["close"].iloc[i]
        prev_c = df["close"].iloc[i - 1]
        ma_f = df["ma_30"].iloc[i]
        ma_t = df["ma_10"].iloc[i]
        ma_s = df["ma_5"].iloc[i]
        turn = df["etf_turnover"].iloc[i]
        prev_cg = int(df["consec_gains"].iloc[i - 1])
        is_down = curr_c < prev_c

        if df["actual_pos"].iloc[i] != 1:
            v_active = False
            v_exit_signal = None
            continue

        if df["logic_state"].iloc[i] != "Composite":
            v_active = False
            v_exit_signal = None
            continue

        if v_active:
            is_below_ma = curr_c < ma_f
            v_held_days = i - v_entry_idx

            if is_below_ma and is_down:
                v_exit_signal = "trend_break"
                v_active = False
                continue

            if v_held_days >= 5 and is_below_ma:
                closes_in_period = df["close"].iloc[v_entry_idx:i + 1]
                if not (closes_in_period > v_entry_high).any():
                    v_exit_signal = "time_stop"
                    v_active = False
                    continue

            if v_held_days > 5:
                v_active = False
                v_exit_signal = None
                continue

            v_exit_signal = None
        else:
            v_exit_signal = None
            conds_met = (
                curr_c > ma_t
                and prev_cg >= 3
                and is_down
                and turn > 1.0
                and curr_c > ma_s
                and curr_c > ma_f
            )
            if conds_met:
                v_active = True
                v_entry_idx = i
                v_entry_high = df["high"].iloc[i]
                v_exit_signal = None

    result = empty.copy()
    if v_active:
        result.update(
            {
                "active": True,
                "entry_date": df["trade_date"].iloc[v_entry_idx],
                "entry_high": v_entry_high,
                "held_days": last_idx - v_entry_idx,
                "exit_signal": None,
            }
        )
    elif v_exit_signal is not None:
        result.update(
            {
                "active": False,
                "entry_date": df["trade_date"].iloc[v_entry_idx],
                "entry_high": v_entry_high,
                "held_days": last_idx - v_entry_idx,
                "exit_signal": v_exit_signal,
            }
        )
    return result


def compute_trade_summary(df: pd.DataFrame) -> dict[str, object]:
    """Return trade counts and win rate using the current T+1 execution logic."""
    n = len(df)
    buy_dates = df.index[df["signal"] == 1].tolist()
    sell_dates = df.index[df["signal"] == -1].tolist()
    n_trades = len(buy_dates)

    trade_returns = []
    for bi, si in zip(buy_dates, sell_dates[: len(buy_dates)]):
        exec_buy = min(bi + 1, n - 1)
        exec_sell = min(si + 1, n - 1)
        trade_nav = (1 + df["strat_ret"].iloc[exec_buy : exec_sell + 1]).prod() - 1
        trade_returns.append(trade_nav)

    win_rate = np.mean([r > 0 for r in trade_returns]) * 100 if trade_returns else 0
    return {
        "buy_dates": buy_dates,
        "sell_dates": sell_dates,
        "n_trades": n_trades,
        "trade_returns": trade_returns,
        "win_rate": win_rate,
    }


def max_drawdown(nav_series: pd.Series) -> float:
    peak = nav_series.cummax()
    drawdown = (nav_series - peak) / peak
    return drawdown.min()
