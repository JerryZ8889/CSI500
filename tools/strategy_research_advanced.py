#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Advanced local CSI500 strategy research.

This script:
1. Builds daily market-internal features from the local component stock files.
2. Searches interpretable strategy families that combine:
   - oversold mean-reversion entries (Composite style)
   - optional trend-following overlay

It never touches the formal production pipeline files.
"""

from __future__ import annotations

import argparse
import itertools
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
RESULT_DIR = ROOT / "research_results"
STRATEGY_FILE = ROOT / "strategy_data.csv"
COMP_FILE = ROOT / "csi500_components_schedule.csv"
STOCK_DIRS = [ROOT / "stocks_archive", ROOT / "stocks_data"]
FEATURE_FILE = RESULT_DIR / "market_internal_features.csv"

TRAIN_END = pd.Timestamp("2022-12-31")
VALID_START = pd.Timestamp("2023-01-01")
VALID_END = pd.Timestamp("2024-12-31")
HOLDOUT_START = pd.Timestamp("2025-01-01")


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    drawdown = nav / peak - 1
    return float(drawdown.min())


def load_strategy_frame() -> pd.DataFrame:
    df = pd.read_csv(STRATEGY_FILE)
    df["trade_date"] = df["trade_date"].astype(str)
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    return df


def load_stock_panel() -> pd.DataFrame:
    code_to_paths: dict[str, list[tuple[int, Path]]] = {}
    for priority, stock_dir in enumerate(STOCK_DIRS):
        if not stock_dir.exists():
            continue
        for path in stock_dir.glob("*.csv"):
            code_to_paths.setdefault(path.stem, []).append((priority, path))

    stock_parts: list[pd.DataFrame] = []
    for ts_code in sorted(code_to_paths):
        frames = []
        for priority, path in code_to_paths[ts_code]:
            df_s = pd.read_csv(path)
            df_s["trade_date"] = df_s["trade_date"].astype(str).str.strip()
            for col in ["close", "pre_close", "vol", "amount"]:
                df_s[col] = pd.to_numeric(df_s[col], errors="coerce")
            df_s["source_priority"] = priority
            frames.append(
                df_s[
                    [
                        "trade_date",
                        "close",
                        "pre_close",
                        "vol",
                        "amount",
                        "source_priority",
                    ]
                ]
            )

        merged = pd.concat(frames, ignore_index=True)
        merged = (
            merged.sort_values(["trade_date", "source_priority"])
            .drop_duplicates("trade_date", keep="last")
            .sort_values("trade_date")
            .reset_index(drop=True)
        )
        merged["ret_1d"] = merged["close"] / merged["pre_close"] - 1
        for n in [5, 10, 20, 60]:
            merged[f"ma{n}"] = merged["close"].rolling(n).mean()
            merged[f"above_ma{n}"] = np.where(
                merged[f"ma{n}"].notna(),
                (merged["close"] > merged[f"ma{n}"]).astype(float),
                np.nan,
            )
        merged["ts_code"] = ts_code
        stock_parts.append(
            merged[
                [
                    "ts_code",
                    "trade_date",
                    "close",
                    "ret_1d",
                    "amount",
                    "above_ma5",
                    "above_ma10",
                    "above_ma20",
                    "above_ma60",
                ]
            ]
        )

    return pd.concat(stock_parts, ignore_index=True)


def build_trade_asof_map(trade_dates: list[str]) -> pd.DataFrame:
    comp = pd.read_csv(COMP_FILE)
    comp["asof_date"] = comp["asof_date"].astype(str).str.strip()
    asof_sorted = sorted(comp["asof_date"].unique())

    trade_to_asof = {}
    for td in trade_dates:
        valid = [d for d in asof_sorted if d <= td]
        if valid:
            trade_to_asof[td] = max(valid)

    return pd.DataFrame(
        {
            "trade_date": list(trade_to_asof.keys()),
            "asof_date": list(trade_to_asof.values()),
        }
    )


def build_market_features() -> pd.DataFrame:
    base = load_strategy_frame()[["trade_date", "date"]].copy()
    trade_asof_df = build_trade_asof_map(base["trade_date"].tolist())

    comp = pd.read_csv(COMP_FILE)
    comp["asof_date"] = comp["asof_date"].astype(str).str.strip()
    comp["con_code"] = comp["con_code"].astype(str).str.strip()

    panel = load_stock_panel()
    panel = panel.merge(trade_asof_df, on="trade_date", how="inner")
    active = panel.merge(
        comp[["con_code", "asof_date"]].rename(columns={"con_code": "ts_code"}),
        on=["ts_code", "asof_date"],
        how="inner",
    )

    active = active[active["close"].notna()].copy()
    active["up"] = (active["ret_1d"] > 0).astype(float)
    active["down"] = (active["ret_1d"] < 0).astype(float)
    active["gt2"] = (active["ret_1d"] > 0.02).astype(float)
    active["lt2"] = (active["ret_1d"] < -0.02).astype(float)

    feat = (
        active.groupby("trade_date").agg(
            active_count=("ts_code", "count"),
            up_ratio=("up", "mean"),
            down_ratio=("down", "mean"),
            gt2_ratio=("gt2", "mean"),
            lt2_ratio=("lt2", "mean"),
            mean_ret=("ret_1d", "mean"),
            median_ret=("ret_1d", "median"),
            std_ret=("ret_1d", "std"),
            total_amount=("amount", "sum"),
            breadth_ma5=("above_ma5", "mean"),
            breadth_ma10=("above_ma10", "mean"),
            breadth_ma20=("above_ma20", "mean"),
            breadth_ma60=("above_ma60", "mean"),
        )
    ).reset_index()

    for col in [
        "up_ratio",
        "down_ratio",
        "gt2_ratio",
        "lt2_ratio",
        "breadth_ma5",
        "breadth_ma10",
        "breadth_ma20",
        "breadth_ma60",
    ]:
        feat[col] = feat[col] * 100

    feat["breadth_5_20_gap"] = feat["breadth_ma5"] - feat["breadth_ma20"]
    feat["breadth_20_60_gap"] = feat["breadth_ma20"] - feat["breadth_ma60"]
    feat["advance_net"] = feat["up_ratio"] - feat["down_ratio"]
    feat["trade_date"] = feat["trade_date"].astype(str)
    feat = base[["trade_date", "date"]].merge(feat, on="trade_date", how="left")
    return feat


def get_market_features(rebuild: bool) -> pd.DataFrame:
    RESULT_DIR.mkdir(exist_ok=True)
    if FEATURE_FILE.exists() and not rebuild:
        feat = pd.read_csv(FEATURE_FILE)
        feat["trade_date"] = feat["trade_date"].astype(str)
        feat["date"] = pd.to_datetime(feat["date"])
        return feat

    feat = build_market_features()
    feat.to_csv(FEATURE_FILE, index=False, encoding="utf-8-sig")
    return feat


def merge_base_with_features(rebuild: bool) -> pd.DataFrame:
    base = load_strategy_frame()
    feat = get_market_features(rebuild=rebuild)
    merged = base.merge(feat.drop(columns=["date"]), on="trade_date", how="left")
    return merged


def simulate(df: pd.DataFrame, params: dict[str, object]) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    cost = float(params.get("cost", 0.001))

    comp_entry = float(params.get("comp_entry", 16))
    comp_exit_breadth = float(params.get("comp_exit_breadth", 79))
    comp_exit_heat = float(params.get("comp_exit_heat", 1.5))
    comp_b60_floor = params.get("comp_b60_floor")

    enable_trend = bool(params.get("enable_trend", False))
    trend_b60_entry = float(params.get("trend_b60_entry", 55))
    trend_b20_entry = float(params.get("trend_b20_entry", 60))
    trend_b60_exit = float(params.get("trend_b60_exit", 45))
    trend_b20_exit = float(params.get("trend_b20_exit", 45))

    comp_active = False
    trend_active = False
    signals = np.zeros(n, dtype=int)
    regime = [None] * n
    comp_flags = np.zeros(n, dtype=int)
    trend_flags = np.zeros(n, dtype=int)

    prev_overall = False

    for i in range(1, n):
        close = df.at[i, "close"]
        ma30 = df.at[i, "ma_30"]
        b20 = df.at[i, "breadth_ma20"]
        b60 = df.at[i, "breadth_ma60"]
        heat_z = df.at[i, "heat_z"]

        if not comp_active:
            cond = b20 < comp_entry
            if comp_b60_floor is not None:
                cond = cond and b60 >= float(comp_b60_floor)
            if cond:
                comp_active = True
        else:
            if b20 > comp_exit_breadth and heat_z < comp_exit_heat:
                comp_active = False

        if enable_trend:
            if not trend_active:
                if close > ma30 and b60 >= trend_b60_entry and b20 >= trend_b20_entry:
                    trend_active = True
            else:
                if close < ma30 or b60 < trend_b60_exit or b20 < trend_b20_exit:
                    trend_active = False

        overall = comp_active or trend_active
        if overall and not prev_overall:
            signals[i] = 1
        elif prev_overall and not overall:
            signals[i] = -1

        comp_flags[i] = int(comp_active)
        trend_flags[i] = int(trend_active)
        if comp_active and trend_active:
            regime[i] = "Composite+Trend"
        elif comp_active:
            regime[i] = "Composite"
        elif trend_active:
            regime[i] = "Trend"

        prev_overall = overall

    actual_pos = np.zeros(n, dtype=int)
    pos = 0
    for i in range(n):
        if i > 0 and signals[i - 1] == 1:
            pos = 1
        elif i > 0 and signals[i - 1] == -1:
            pos = 0
        actual_pos[i] = pos

    strat_ret = np.zeros(n)
    for i in range(1, n):
        if actual_pos[i] == 1 and actual_pos[i - 1] == 0:
            strat_ret[i] = (df.at[i, "close"] / df.at[i, "open"] - 1) - cost
        elif actual_pos[i] == 0 and actual_pos[i - 1] == 1:
            strat_ret[i] = (df.at[i, "open"] / df.at[i, "close"] - 1) - cost
        elif actual_pos[i] == 1 and actual_pos[i - 1] == 1:
            strat_ret[i] = df.at[i, "close"] / df.at[i - 1, "close"] - 1

    out = df[["trade_date", "date", "close", "heat_z"]].copy()
    out["signal"] = signals
    out["actual_pos"] = actual_pos
    out["comp_active"] = comp_flags
    out["trend_active"] = trend_flags
    out["regime"] = regime
    out["strat_ret"] = strat_ret
    out["strat_nav"] = (1 + out["strat_ret"]).cumprod()
    out["bench_ret"] = df["close"].pct_change().fillna(0)
    out["bench_nav"] = (1 + out["bench_ret"]).cumprod()
    return out


def slice_nav(window: pd.DataFrame, col: str) -> pd.Series:
    nav = (1 + window[col]).cumprod()
    nav /= nav.iloc[0]
    return nav


def summarize_window(window: pd.DataFrame, prefix: str) -> dict[str, float]:
    if window.empty:
        return {
            f"{prefix}_total_pct": np.nan,
            f"{prefix}_excess_pct": np.nan,
            f"{prefix}_cagr_pct": np.nan,
            f"{prefix}_mdd_pct": np.nan,
            f"{prefix}_calmar": np.nan,
            f"{prefix}_trades": 0,
            f"{prefix}_hold_pct": np.nan,
        }

    years = max((window["date"].iloc[-1] - window["date"].iloc[0]).days / 365.25, 1 / 365.25)
    strat_nav = slice_nav(window, "strat_ret")
    bench_nav = slice_nav(window, "bench_ret")
    total = strat_nav.iloc[-1] - 1
    bench = bench_nav.iloc[-1] - 1
    cagr = strat_nav.iloc[-1] ** (1 / years) - 1
    mdd = max_drawdown(strat_nav)
    calmar = np.nan if mdd == 0 else cagr / abs(mdd)
    return {
        f"{prefix}_total_pct": round(float(total * 100), 2),
        f"{prefix}_excess_pct": round(float((total - bench) * 100), 2),
        f"{prefix}_cagr_pct": round(float(cagr * 100), 2),
        f"{prefix}_mdd_pct": round(float(mdd * 100), 2),
        f"{prefix}_calmar": round(float(calmar), 4),
        f"{prefix}_trades": int((window["signal"] == 1).sum()),
        f"{prefix}_hold_pct": round(float(window["actual_pos"].mean() * 100), 2),
    }


def build_cases() -> list[tuple[str, dict[str, object]]]:
    cases: list[tuple[str, dict[str, object]]] = [
        ("baseline_like", {}),
        ("composite_only", {"enable_trend": False}),
    ]

    for comp_entry, comp_floor, comp_exit_breadth, comp_exit_heat in itertools.product(
        [12, 14, 16, 18],
        [None, 30, 40, 50],
        [74, 79, 84],
        [1.2, 1.5, 2.0],
    ):
        floor_label = "none" if comp_floor is None else int(comp_floor)
        label = f"comp_e{comp_entry}_f{floor_label}_x{comp_exit_breadth}_h{comp_exit_heat}"
        cases.append(
            (
                label,
                {
                    "enable_trend": False,
                    "comp_entry": comp_entry,
                    "comp_b60_floor": comp_floor,
                    "comp_exit_breadth": comp_exit_breadth,
                    "comp_exit_heat": comp_exit_heat,
                },
            )
        )

    for (
        comp_entry,
        comp_floor,
        comp_exit_breadth,
        comp_exit_heat,
        trend_b60_entry,
        trend_b20_entry,
        trend_b60_exit,
        trend_b20_exit,
    ) in itertools.product(
        [12, 14, 16],
        [None, 30, 40],
        [79, 84],
        [1.5, 2.0],
        [45, 55, 65],
        [50, 60, 70],
        [30, 40, 50],
        [35, 45, 55],
    ):
        if trend_b60_exit >= trend_b60_entry:
            continue
        if trend_b20_exit >= trend_b20_entry:
            continue
        floor_label = "none" if comp_floor is None else int(comp_floor)
        label = (
            f"hybrid_ce{comp_entry}_cf{floor_label}_cx{comp_exit_breadth}_ch{comp_exit_heat}_"
            f"t60e{trend_b60_entry}_t20e{trend_b20_entry}_t60x{trend_b60_exit}_t20x{trend_b20_exit}"
        )
        cases.append(
            (
                label,
                {
                    "enable_trend": True,
                    "comp_entry": comp_entry,
                    "comp_b60_floor": comp_floor,
                    "comp_exit_breadth": comp_exit_breadth,
                    "comp_exit_heat": comp_exit_heat,
                    "trend_b60_entry": trend_b60_entry,
                    "trend_b20_entry": trend_b20_entry,
                    "trend_b60_exit": trend_b60_exit,
                    "trend_b20_exit": trend_b20_exit,
                },
            )
        )

    return cases


def run_search(rebuild_features: bool, top: int) -> pd.DataFrame:
    src = merge_base_with_features(rebuild=rebuild_features)
    train_mask = src["date"] <= TRAIN_END
    valid_mask = (src["date"] >= VALID_START) & (src["date"] <= VALID_END)
    holdout_mask = src["date"] >= HOLDOUT_START

    rows = []
    for label, params in build_cases():
        out = simulate(src, params)
        row: dict[str, object] = {"label": label, "params": repr(params)}
        row.update(summarize_window(out, "all"))
        row.update(summarize_window(out.loc[train_mask].copy(), "train"))
        row.update(summarize_window(out.loc[valid_mask].copy(), "valid"))
        row.update(summarize_window(out.loc[holdout_mask].copy(), "holdout"))
        rows.append(row)

    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["valid_calmar", "holdout_calmar", "valid_cagr_pct", "holdout_cagr_pct"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    RESULT_DIR.mkdir(exist_ok=True)
    result.to_csv(RESULT_DIR / "strategy_advanced_results.csv", index=False, encoding="utf-8-sig")
    result.head(top).to_csv(
        RESULT_DIR / "strategy_advanced_top.csv", index=False, encoding="utf-8-sig"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Advanced local CSI500 strategy research.")
    parser.add_argument("--top", type=int, default=20, help="How many top rows to print.")
    parser.add_argument(
        "--rebuild-features",
        action="store_true",
        help="Rebuild daily market internal features from local stock files.",
    )
    args = parser.parse_args()

    result = run_search(rebuild_features=args.rebuild_features, top=args.top)
    cols = [
        "label",
        "valid_trades",
        "valid_total_pct",
        "valid_excess_pct",
        "valid_cagr_pct",
        "valid_mdd_pct",
        "valid_calmar",
        "holdout_trades",
        "holdout_total_pct",
        "holdout_excess_pct",
        "holdout_cagr_pct",
        "holdout_mdd_pct",
        "holdout_calmar",
        "all_total_pct",
        "all_cagr_pct",
        "all_mdd_pct",
    ]
    print(result[cols].head(args.top).to_string(index=False))
    print()
    print(f"Saved market features to: {FEATURE_FILE}")
    print(f"Saved full results to: {RESULT_DIR / 'strategy_advanced_results.csv'}")
    print(f"Saved top list to: {RESULT_DIR / 'strategy_advanced_top.csv'}")


if __name__ == "__main__":
    main()
