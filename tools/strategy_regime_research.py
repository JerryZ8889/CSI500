#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local CSI500 regime research.

This script keeps the new "bull / bear / chaos" idea fully separated from the
formal production pipeline. It does three things:

1. Search a simple market regime detector based on price and market breadth.
2. On top of the best detector candidates, search regime-specific strategies.
3. Save the best labels, switch dates, strategy frame, and summary tables.
"""

from __future__ import annotations

import argparse
import ast
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
RESULT_DIR = ROOT / "research_results"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from strategy_engine import max_drawdown
from strategy_research_advanced import merge_base_with_features


TRAIN_END = pd.Timestamp("2022-12-31")
VALID_START = pd.Timestamp("2023-01-01")
VALID_END = pd.Timestamp("2024-12-31")
HOLDOUT_START = pd.Timestamp("2025-01-01")
RECENT_START = pd.Timestamp("2024-01-01")

DETECTOR_FILE = RESULT_DIR / "regime_detector_results.csv"
DETECTOR_TOP_FILE = RESULT_DIR / "regime_detector_top.csv"
STRATEGY_FILE = RESULT_DIR / "regime_strategy_results.csv"
STRATEGY_TOP_FILE = RESULT_DIR / "regime_strategy_top.csv"
BEST_LABELS_FILE = RESULT_DIR / "regime_best_labels.csv"
BEST_SWITCHES_FILE = RESULT_DIR / "regime_best_switches.csv"
BEST_STRATEGY_FILE = RESULT_DIR / "regime_best_strategy.csv"
BEST_SUMMARY_FILE = RESULT_DIR / "regime_best_summary.csv"


def load_source() -> pd.DataFrame:
    df = merge_base_with_features(rebuild=False)
    df = df.copy()
    df["trade_date"] = df["trade_date"].astype(str)
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    df["index_ma60"] = df["close"].rolling(60).mean()
    df["index_ma120"] = df["close"].rolling(120).mean()
    df["index_ma250"] = df["close"].rolling(250).mean()
    df["ret_20d"] = df["close"].pct_change(20)
    df["ret_60d"] = df["close"].pct_change(60)
    df["fwd_20d"] = df["close"].shift(-20) / df["close"] - 1
    return df


def build_detector_cases() -> list[tuple[str, dict[str, float]]]:
    cases: list[tuple[str, dict[str, float]]] = []
    for (
        bull_entry_b20,
        bull_entry_b60,
        bull_exit_b20,
        bull_exit_b60,
        bear_entry_b20,
        bear_entry_b60,
        bear_exit_b20,
        bear_exit_b60,
    ) in itertools.product(
        [70, 75],
        [50, 55],
        [45, 50],
        [30, 35],
        [25, 30, 35],
        [25, 30, 35],
        [45, 50],
        [40, 45],
    ):
        if bull_exit_b20 >= bull_entry_b20:
            continue
        if bull_exit_b60 >= bull_entry_b60:
            continue
        if bear_exit_b20 <= bear_entry_b20:
            continue
        if bear_exit_b60 <= bear_entry_b60:
            continue

        params = {
            "bull_entry_b20": bull_entry_b20,
            "bull_entry_b60": bull_entry_b60,
            "bull_exit_b20": bull_exit_b20,
            "bull_exit_b60": bull_exit_b60,
            "bear_entry_b20": bear_entry_b20,
            "bear_entry_b60": bear_entry_b60,
            "bear_exit_b20": bear_exit_b20,
            "bear_exit_b60": bear_exit_b60,
        }
        label = (
            f"bull_e20_{bull_entry_b20}_e60_{bull_entry_b60}_"
            f"x20_{bull_exit_b20}_x60_{bull_exit_b60}__"
            f"bear_e20_{bear_entry_b20}_e60_{bear_entry_b60}_"
            f"x20_{bear_exit_b20}_x60_{bear_exit_b60}"
        )
        cases.append((label, params))
    return cases


def apply_regime_detector(df: pd.DataFrame, params: dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    n = len(out)
    regimes = ["Chaos"] * n

    state = "Chaos"
    for i in range(n):
        close = out.at[i, "close"]
        ma120 = out.at[i, "index_ma120"]
        b20 = out.at[i, "breadth_ma20"]
        b60 = out.at[i, "breadth_ma60"]

        bull_enter = (
            pd.notna(ma120)
            and pd.notna(b20)
            and pd.notna(b60)
            and close > ma120
            and b20 >= params["bull_entry_b20"]
            and b60 >= params["bull_entry_b60"]
        )
        bull_exit = (
            pd.notna(ma120)
            and pd.notna(b20)
            and pd.notna(b60)
            and (
                close < ma120
                or b20 < params["bull_exit_b20"]
                or b60 < params["bull_exit_b60"]
            )
        )

        bear_enter = (
            pd.notna(ma120)
            and pd.notna(b20)
            and pd.notna(b60)
            and close < ma120
            and b20 <= params["bear_entry_b20"]
            and b60 <= params["bear_entry_b60"]
        )
        bear_exit = (
            pd.notna(ma120)
            and pd.notna(b20)
            and pd.notna(b60)
            and (
                close > ma120
                or b20 > params["bear_exit_b20"]
                or b60 > params["bear_exit_b60"]
            )
        )

        if state == "Bull":
            if bull_exit:
                state = "Bear" if bear_enter else "Chaos"
        elif state == "Bear":
            if bear_exit:
                state = "Bull" if bull_enter else "Chaos"
        else:
            if bull_enter:
                state = "Bull"
            elif bear_enter:
                state = "Bear"

        regimes[i] = state

    out["regime"] = regimes
    out["regime_prev"] = out["regime"].shift(1)
    out["regime_switch"] = (
        out["regime_prev"].notna() & (out["regime_prev"] != out["regime"])
    ).astype(int)
    return out


def summarize_detector_window(window: pd.DataFrame, prefix: str) -> dict[str, float]:
    if window.empty:
        return {
            f"{prefix}_bull_days": 0,
            f"{prefix}_bear_days": 0,
            f"{prefix}_chaos_days": 0,
            f"{prefix}_switches": 0,
            f"{prefix}_switches_per_year": np.nan,
            f"{prefix}_bull_fwd20_mean_pct": np.nan,
            f"{prefix}_bear_fwd20_mean_pct": np.nan,
            f"{prefix}_chaos_fwd20_mean_pct": np.nan,
            f"{prefix}_bull_hit_pct": np.nan,
            f"{prefix}_bear_hit_pct": np.nan,
            f"{prefix}_sep_pct": np.nan,
            f"{prefix}_score": np.nan,
        }

    years = max((window["date"].iloc[-1] - window["date"].iloc[0]).days / 365.25, 1 / 365.25)
    bull = window.loc[window["regime"] == "Bull", "fwd_20d"].dropna()
    bear = window.loc[window["regime"] == "Bear", "fwd_20d"].dropna()
    chaos = window.loc[window["regime"] == "Chaos", "fwd_20d"].dropna()

    bull_mean = float(bull.mean() * 100) if not bull.empty else np.nan
    bear_mean = float(bear.mean() * 100) if not bear.empty else np.nan
    chaos_mean = float(chaos.mean() * 100) if not chaos.empty else np.nan
    bull_hit = float((bull > 0).mean() * 100) if not bull.empty else np.nan
    bear_hit = float((bear < 0).mean() * 100) if not bear.empty else np.nan
    sep = bull_mean - bear_mean if pd.notna(bull_mean) and pd.notna(bear_mean) else np.nan
    switches = int(window["regime_switch"].sum())
    switches_per_year = switches / years if years > 0 else np.nan

    score = np.nan
    if pd.notna(sep) and pd.notna(bull_hit) and pd.notna(bear_hit):
        chaos_penalty = 0.0 if pd.isna(chaos_mean) else abs(chaos_mean) * 0.35
        score = sep + (bull_hit + bear_hit - 100.0) * 0.18 - chaos_penalty - switches_per_year * 0.9

    return {
        f"{prefix}_bull_days": int((window["regime"] == "Bull").sum()),
        f"{prefix}_bear_days": int((window["regime"] == "Bear").sum()),
        f"{prefix}_chaos_days": int((window["regime"] == "Chaos").sum()),
        f"{prefix}_switches": switches,
        f"{prefix}_switches_per_year": round(float(switches_per_year), 3) if pd.notna(switches_per_year) else np.nan,
        f"{prefix}_bull_fwd20_mean_pct": round(bull_mean, 2) if pd.notna(bull_mean) else np.nan,
        f"{prefix}_bear_fwd20_mean_pct": round(bear_mean, 2) if pd.notna(bear_mean) else np.nan,
        f"{prefix}_chaos_fwd20_mean_pct": round(chaos_mean, 2) if pd.notna(chaos_mean) else np.nan,
        f"{prefix}_bull_hit_pct": round(bull_hit, 2) if pd.notna(bull_hit) else np.nan,
        f"{prefix}_bear_hit_pct": round(bear_hit, 2) if pd.notna(bear_hit) else np.nan,
        f"{prefix}_sep_pct": round(sep, 2) if pd.notna(sep) else np.nan,
        f"{prefix}_score": round(float(score), 4) if pd.notna(score) else np.nan,
    }


def search_detectors(df: pd.DataFrame, top: int) -> pd.DataFrame:
    train_mask = df["date"] <= TRAIN_END
    valid_mask = (df["date"] >= VALID_START) & (df["date"] <= VALID_END)
    holdout_mask = df["date"] >= HOLDOUT_START

    rows = []
    for label, params in build_detector_cases():
        labeled = apply_regime_detector(df, params)
        base_strategy = simulate_regime_strategy(
            labeled,
            {
                "bull_mode": "hold",
                "chaos_mode": "flat",
                "bear_mode": "flat",
            },
        )
        row: dict[str, object] = {"detector_label": label, "detector_params": repr(params)}
        row.update(summarize_detector_window(labeled, "all"))
        row.update(summarize_detector_window(labeled.loc[train_mask].copy(), "train"))
        row.update(summarize_detector_window(labeled.loc[valid_mask].copy(), "valid"))
        row.update(summarize_detector_window(labeled.loc[holdout_mask].copy(), "holdout"))
        row.update(summarize_strategy_window(base_strategy, "all_base"))
        row.update(summarize_strategy_window(base_strategy.loc[train_mask].copy(), "train_base"))
        row.update(summarize_strategy_window(base_strategy.loc[valid_mask].copy(), "valid_base"))
        row.update(summarize_strategy_window(base_strategy.loc[holdout_mask].copy(), "holdout_base"))
        rows.append(row)

    result = pd.DataFrame(rows)
    valid_guard = (
        (result["valid_bull_days"] >= 40)
        & (result["valid_bear_days"] >= 40)
        & (result["valid_chaos_days"] >= 40)
    )
    result = result.loc[valid_guard].copy()
    result = result.sort_values(
        [
            "valid_base_calmar",
            "holdout_base_calmar",
            "valid_base_excess_pct",
            "holdout_base_excess_pct",
            "all_base_excess_pct",
            "valid_score",
        ],
        ascending=[False, False, False, False, False, False],
    ).reset_index(drop=True)

    RESULT_DIR.mkdir(exist_ok=True)
    result.to_csv(DETECTOR_FILE, index=False, encoding="utf-8-sig")
    result.head(top).to_csv(DETECTOR_TOP_FILE, index=False, encoding="utf-8-sig")
    return result


def build_strategy_cases() -> list[tuple[str, dict[str, object]]]:
    cases: list[tuple[str, dict[str, object]]] = []

    chaos_cases = [("flat", {"chaos_mode": "flat"})]
    for entry, exit_breadth, exit_heat in itertools.product([10, 12, 14], [74, 79], [1.5, 2.0]):
        label = f"low_e{entry}_x{exit_breadth}_h{exit_heat}"
        chaos_cases.append(
            (
                label,
                {
                    "chaos_mode": "low",
                    "chaos_entry": entry,
                    "chaos_exit_breadth": exit_breadth,
                    "chaos_exit_heat": exit_heat,
                },
            )
        )

    bear_cases = [("flat", {"bear_mode": "flat"})]
    for entry, exit_breadth, exit_heat in itertools.product([4, 6, 8], [45, 55], [1.0, 1.5]):
        label = f"deep_e{entry}_x{exit_breadth}_h{exit_heat}"
        bear_cases.append(
            (
                label,
                {
                    "bear_mode": "deep_low",
                    "bear_entry": entry,
                    "bear_exit_breadth": exit_breadth,
                    "bear_exit_heat": exit_heat,
                },
            )
        )

    for chaos_label, chaos_params in chaos_cases:
        for bear_label, bear_params in bear_cases:
            label = f"bull_hold__chaos_{chaos_label}__bear_{bear_label}"
            params = {"bull_mode": "hold"}
            params.update(chaos_params)
            params.update(bear_params)
            cases.append((label, params))

    return cases


def simulate_regime_strategy(df: pd.DataFrame, params: dict[str, object], cost: float = 0.001) -> pd.DataFrame:
    out = df.copy()
    n = len(out)
    signals = np.zeros(n, dtype=int)
    actual_pos = np.zeros(n, dtype=int)
    strat_ret = np.zeros(n, dtype=float)
    logic = [None] * n

    chaos_active = False
    bear_active = False
    prev_overall = False

    for i in range(1, n):
        regime = out.at[i, "regime"]
        breadth = out.at[i, "breadth_ma20"]
        heat_z = out.at[i, "heat_z"]

        bull_hold = params.get("bull_mode") == "hold" and regime == "Bull"

        if regime != "Chaos" or params.get("chaos_mode") != "low":
            chaos_active = False
        else:
            if not chaos_active and breadth < float(params["chaos_entry"]):
                chaos_active = True
            elif chaos_active and breadth > float(params["chaos_exit_breadth"]) and heat_z < float(params["chaos_exit_heat"]):
                chaos_active = False

        if regime != "Bear" or params.get("bear_mode") != "deep_low":
            bear_active = False
        else:
            if not bear_active and breadth < float(params["bear_entry"]):
                bear_active = True
            elif bear_active and breadth > float(params["bear_exit_breadth"]) and heat_z < float(params["bear_exit_heat"]):
                bear_active = False

        overall = bull_hold or chaos_active or bear_active
        if overall and not prev_overall:
            signals[i] = 1
        elif prev_overall and not overall:
            signals[i] = -1

        if bull_hold:
            logic[i] = "BullHold"
        elif chaos_active:
            logic[i] = "ChaosLow"
        elif bear_active:
            logic[i] = "BearLow"

        prev_overall = overall

    pos = 0
    for i in range(n):
        if i > 0 and signals[i - 1] == 1:
            pos = 1
        elif i > 0 and signals[i - 1] == -1:
            pos = 0
        actual_pos[i] = pos

    for i in range(1, n):
        if actual_pos[i] == 1 and actual_pos[i - 1] == 0:
            strat_ret[i] = (out.at[i, "close"] / out.at[i, "open"] - 1) - cost
        elif actual_pos[i] == 0 and actual_pos[i - 1] == 1:
            strat_ret[i] = (out.at[i, "open"] / out.at[i - 1, "close"] - 1) - cost
        elif actual_pos[i] == 1 and actual_pos[i - 1] == 1:
            strat_ret[i] = out.at[i, "close"] / out.at[i - 1, "close"] - 1

    out["signal"] = signals
    out["actual_pos"] = actual_pos
    out["logic"] = logic
    out["strat_ret"] = strat_ret
    out["strat_nav"] = (1 + out["strat_ret"]).cumprod()
    out["bench_ret"] = out["close"].pct_change().fillna(0.0)
    out["bench_nav"] = (1 + out["bench_ret"]).cumprod()
    return out


def summarize_strategy_window(window: pd.DataFrame, prefix: str) -> dict[str, float]:
    if window.empty:
        return {
            f"{prefix}_total_pct": np.nan,
            f"{prefix}_bench_pct": np.nan,
            f"{prefix}_excess_pct": np.nan,
            f"{prefix}_cagr_pct": np.nan,
            f"{prefix}_mdd_pct": np.nan,
            f"{prefix}_calmar": np.nan,
            f"{prefix}_trades": 0,
            f"{prefix}_hold_pct": np.nan,
        }

    years = max((window["date"].iloc[-1] - window["date"].iloc[0]).days / 365.25, 1 / 365.25)
    strat_nav = (1 + window["strat_ret"]).cumprod()
    strat_nav /= strat_nav.iloc[0]
    bench_nav = (1 + window["bench_ret"]).cumprod()
    bench_nav /= bench_nav.iloc[0]

    total = strat_nav.iloc[-1] - 1
    bench = bench_nav.iloc[-1] - 1
    cagr = strat_nav.iloc[-1] ** (1 / years) - 1
    mdd = max_drawdown(strat_nav)
    calmar = np.nan if mdd == 0 else cagr / abs(mdd)

    return {
        f"{prefix}_total_pct": round(float(total * 100), 2),
        f"{prefix}_bench_pct": round(float(bench * 100), 2),
        f"{prefix}_excess_pct": round(float((total - bench) * 100), 2),
        f"{prefix}_cagr_pct": round(float(cagr * 100), 2),
        f"{prefix}_mdd_pct": round(float(mdd * 100), 2),
        f"{prefix}_calmar": round(float(calmar), 4) if pd.notna(calmar) else np.nan,
        f"{prefix}_trades": int((window["signal"] == 1).sum()),
        f"{prefix}_hold_pct": round(float(window["actual_pos"].mean() * 100), 2),
    }


def build_switch_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i in range(1, len(df)):
        prev_regime = df.at[i - 1, "regime"]
        curr_regime = df.at[i, "regime"]
        if prev_regime == curr_regime:
            continue
        rows.append(
            {
                "trade_date": df.at[i, "trade_date"],
                "from_regime": prev_regime,
                "to_regime": curr_regime,
                "close": round(float(df.at[i, "close"]), 2),
                "breadth_ma20": round(float(df.at[i, "breadth_ma20"]), 2),
                "breadth_ma60": round(float(df.at[i, "breadth_ma60"]), 2),
                "heat_z": round(float(df.at[i, "heat_z"]), 4),
            }
        )
    return pd.DataFrame(rows)


def search_regime_strategies(df: pd.DataFrame, detector_result: pd.DataFrame, top_detectors: int, top: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_mask = df["date"] <= TRAIN_END
    valid_mask = (df["date"] >= VALID_START) & (df["date"] <= VALID_END)
    holdout_mask = df["date"] >= HOLDOUT_START
    recent_mask = df["date"] >= RECENT_START

    detector_subset = detector_result.head(top_detectors).copy()
    strategy_cases = build_strategy_cases()

    rows = []
    cached_labels: dict[str, pd.DataFrame] = {}
    for _, det_row in detector_subset.iterrows():
        detector_label = det_row["detector_label"]
        detector_params = ast.literal_eval(det_row["detector_params"])
        labeled = apply_regime_detector(df, detector_params)
        cached_labels[detector_label] = labeled

        for strategy_label, strategy_params in strategy_cases:
            out = simulate_regime_strategy(labeled, strategy_params)
            row: dict[str, object] = {
                "detector_label": detector_label,
                "strategy_label": strategy_label,
                "detector_params": repr(detector_params),
                "strategy_params": repr(strategy_params),
            }
            row.update(summarize_strategy_window(out, "all"))
            row.update(summarize_strategy_window(out.loc[train_mask].copy(), "train"))
            row.update(summarize_strategy_window(out.loc[valid_mask].copy(), "valid"))
            row.update(summarize_strategy_window(out.loc[holdout_mask].copy(), "holdout"))
            row.update(summarize_strategy_window(out.loc[recent_mask].copy(), "recent"))
            rows.append(row)

    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["valid_calmar", "holdout_calmar", "valid_excess_pct", "holdout_excess_pct", "recent_excess_pct"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    result.to_csv(STRATEGY_FILE, index=False, encoding="utf-8-sig")
    result.head(top).to_csv(STRATEGY_TOP_FILE, index=False, encoding="utf-8-sig")

    best_row = result.iloc[0]
    best_detector_label = best_row["detector_label"]
    best_detector_params = ast.literal_eval(best_row["detector_params"])
    best_strategy_params = ast.literal_eval(best_row["strategy_params"])
    best_labels = cached_labels.get(best_detector_label)
    if best_labels is None:
        best_labels = apply_regime_detector(df, best_detector_params)
    best_strategy = simulate_regime_strategy(best_labels, best_strategy_params)
    return result, best_labels, best_strategy


def write_best_outputs(best_labels: pd.DataFrame, best_strategy: pd.DataFrame) -> None:
    best_labels[
        [
            "trade_date",
            "date",
            "close",
            "index_ma120",
            "breadth_ma20",
            "breadth_ma60",
            "heat_z",
            "regime",
        ]
    ].to_csv(BEST_LABELS_FILE, index=False, encoding="utf-8-sig")

    switches = build_switch_table(best_labels)
    switches.to_csv(BEST_SWITCHES_FILE, index=False, encoding="utf-8-sig")

    best_strategy[
        [
            "trade_date",
            "date",
            "close",
            "regime",
            "logic",
            "signal",
            "actual_pos",
            "strat_ret",
            "strat_nav",
            "bench_ret",
            "bench_nav",
        ]
    ].to_csv(BEST_STRATEGY_FILE, index=False, encoding="utf-8-sig")

    summary_rows = []
    windows = {
        "all": best_strategy,
        "train": best_strategy.loc[best_strategy["date"] <= TRAIN_END].copy(),
        "valid": best_strategy.loc[(best_strategy["date"] >= VALID_START) & (best_strategy["date"] <= VALID_END)].copy(),
        "holdout": best_strategy.loc[best_strategy["date"] >= HOLDOUT_START].copy(),
        "recent": best_strategy.loc[best_strategy["date"] >= RECENT_START].copy(),
    }
    for window_name, window in windows.items():
        row = {"window": window_name}
        row.update(summarize_strategy_window(window, "metrics"))
        summary_rows.append(row)
    pd.DataFrame(summary_rows).to_csv(BEST_SUMMARY_FILE, index=False, encoding="utf-8-sig")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local bull/bear/chaos regime research.")
    parser.add_argument("--top", type=int, default=20, help="How many top rows to print.")
    parser.add_argument(
        "--top-detectors",
        type=int,
        default=12,
        help="How many top regime detectors to feed into the strategy search.",
    )
    args = parser.parse_args()

    RESULT_DIR.mkdir(exist_ok=True)
    src = load_source()

    detector_result = search_detectors(src, top=args.top)
    strategy_result, best_labels, best_strategy = search_regime_strategies(
        src,
        detector_result=detector_result,
        top_detectors=args.top_detectors,
        top=args.top,
    )
    write_best_outputs(best_labels, best_strategy)

    det_cols = [
        "detector_label",
        "valid_base_excess_pct",
        "valid_base_cagr_pct",
        "valid_base_mdd_pct",
        "valid_base_calmar",
        "holdout_base_excess_pct",
        "holdout_base_cagr_pct",
        "holdout_base_mdd_pct",
        "holdout_base_calmar",
        "valid_switches",
        "valid_score",
        "holdout_score",
    ]
    strat_cols = [
        "detector_label",
        "strategy_label",
        "valid_excess_pct",
        "valid_cagr_pct",
        "valid_mdd_pct",
        "valid_calmar",
        "holdout_excess_pct",
        "holdout_cagr_pct",
        "holdout_mdd_pct",
        "holdout_calmar",
        "recent_excess_pct",
        "all_excess_pct",
        "all_cagr_pct",
        "all_mdd_pct",
    ]

    print("Top regime detectors:")
    print(detector_result[det_cols].head(args.top).to_string(index=False))
    print()
    print("Top regime-aware strategies:")
    print(strategy_result[strat_cols].head(args.top).to_string(index=False))
    print()
    print(f"Saved detector results to: {DETECTOR_FILE}")
    print(f"Saved detector top list to: {DETECTOR_TOP_FILE}")
    print(f"Saved strategy results to: {STRATEGY_FILE}")
    print(f"Saved strategy top list to: {STRATEGY_TOP_FILE}")
    print(f"Saved best regime labels to: {BEST_LABELS_FILE}")
    print(f"Saved best switch dates to: {BEST_SWITCHES_FILE}")
    print(f"Saved best strategy frame to: {BEST_STRATEGY_FILE}")
    print(f"Saved best summary to: {BEST_SUMMARY_FILE}")


if __name__ == "__main__":
    main()
