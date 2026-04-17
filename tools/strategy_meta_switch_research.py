#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local meta-switch research for CSI500.

Purpose:
- Keep the current global enhanced rule candidate intact as the main baseline.
- Test whether a regime detector can improve results by switching modules:
  - Bull: switch to a bull-special module
  - Chaos: keep using the global enhanced rule or another chaos module
  - Bear: stay flat or only allow a very strict rebound module

This script is local research only and never touches the formal pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
RESULT_DIR = ROOT / "research_results"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from strategy_engine import compute_strategy_frame, max_drawdown
from strategy_research_advanced import merge_base_with_features, simulate
from strategy_regime_research import apply_regime_detector


STAMP = datetime.now().strftime("%Y%m%d")

RESULT_FILE = RESULT_DIR / f"meta_switch_results_{STAMP}.csv"
TOP_FILE = RESULT_DIR / f"meta_switch_top_{STAMP}.csv"
SUMMARY_FILE = RESULT_DIR / f"meta_switch_summary_{STAMP}.md"
BEST_FRAME_FILE = RESULT_DIR / f"meta_switch_best_frame_{STAMP}.csv"

TRAIN_END = "20221231"
VALID_START = "20230101"
VALID_END = "20241231"
HOLDOUT_START = "20250101"
RECENT_START = "20240101"

BEST_DETECTOR = {
    "bull_entry_b20": 75,
    "bull_entry_b60": 50,
    "bull_exit_b20": 45,
    "bull_exit_b60": 30,
    "bear_entry_b20": 30,
    "bear_entry_b60": 35,
    "bear_exit_b20": 50,
    "bear_exit_b60": 45,
}

BASE_HYBRID = {
    "enable_trend": True,
    "comp_entry": 12,
    "comp_exit_breadth": 79,
    "comp_exit_heat": 2.0,
    "trend_b60_entry": 55,
    "trend_b20_entry": 70,
    "trend_b60_exit": 30,
    "trend_b20_exit": 45,
}

BULL_SPECIAL = {
    "enable_trend": True,
    "comp_entry": 12,
    "comp_exit_breadth": 79,
    "comp_exit_heat": 2.0,
    "trend_b60_entry": 45,
    "trend_b20_entry": 75,
    "trend_b60_exit": 15,
    "trend_b20_exit": 50,
}

CHAOS_SIMPLE = {
    "enable_trend": False,
    "comp_entry": 12,
    "comp_exit_breadth": 79,
    "comp_exit_heat": 1.5,
}

BEAR_DEEP_6 = {
    "enable_trend": False,
    "comp_entry": 6,
    "comp_exit_breadth": 55,
    "comp_exit_heat": 1.0,
}

BEAR_DEEP_8 = {
    "enable_trend": False,
    "comp_entry": 8,
    "comp_exit_breadth": 55,
    "comp_exit_heat": 1.0,
}


@dataclass
class ModuleDef:
    name: str
    kind: str
    params: dict[str, float] | None = None


def load_source() -> pd.DataFrame:
    df = merge_base_with_features(rebuild=False)
    df["trade_date"] = df["trade_date"].astype(str)
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df["index_ma60"] = df["close"].rolling(60).mean()
    df["index_ma120"] = df["close"].rolling(120).mean()
    df["index_ma250"] = df["close"].rolling(250).mean()
    return df


def build_module_library() -> dict[str, ModuleDef]:
    return {
        "flat": ModuleDef(name="flat", kind="flat"),
        "bull_hold": ModuleDef(name="bull_hold", kind="hold"),
        "hybrid": ModuleDef(name="hybrid", kind="advanced", params=BASE_HYBRID),
        "bull_special": ModuleDef(name="bull_special", kind="advanced", params=BULL_SPECIAL),
        "chaos_simple": ModuleDef(name="chaos_simple", kind="advanced", params=CHAOS_SIMPLE),
        "bear_deep6": ModuleDef(name="bear_deep6", kind="advanced", params=BEAR_DEEP_6),
        "bear_deep8": ModuleDef(name="bear_deep8", kind="advanced", params=BEAR_DEEP_8),
    }


def reset_advanced_state() -> dict[str, bool]:
    return {"comp_active": False, "trend_active": False}


def update_advanced_module(
    row: pd.Series,
    state: dict[str, bool],
    params: dict[str, float],
) -> tuple[bool, str | None]:
    close = float(row["close"])
    ma30 = float(row["ma_30"])
    b20 = float(row["breadth_ma20"])
    b60 = float(row["breadth_ma60"])
    heat_z = float(row["heat_z"])

    comp_entry = float(params.get("comp_entry", 16))
    comp_exit_breadth = float(params.get("comp_exit_breadth", 79))
    comp_exit_heat = float(params.get("comp_exit_heat", 1.5))
    comp_b60_floor = params.get("comp_b60_floor")

    enable_trend = bool(params.get("enable_trend", False))
    trend_b60_entry = float(params.get("trend_b60_entry", 55))
    trend_b20_entry = float(params.get("trend_b20_entry", 60))
    trend_b60_exit = float(params.get("trend_b60_exit", 45))
    trend_b20_exit = float(params.get("trend_b20_exit", 45))

    if not state["comp_active"]:
        cond = b20 < comp_entry
        if comp_b60_floor is not None:
            cond = cond and b60 >= float(comp_b60_floor)
        if cond:
            state["comp_active"] = True
    else:
        if b20 > comp_exit_breadth and heat_z < comp_exit_heat:
            state["comp_active"] = False

    if enable_trend:
        if not state["trend_active"]:
            if close > ma30 and b60 >= trend_b60_entry and b20 >= trend_b20_entry:
                state["trend_active"] = True
        else:
            if close < ma30 or b60 < trend_b60_exit or b20 < trend_b20_exit:
                state["trend_active"] = False
    else:
        state["trend_active"] = False

    overall = state["comp_active"] or state["trend_active"]
    if state["comp_active"] and state["trend_active"]:
        logic = "Composite+Trend"
    elif state["comp_active"]:
        logic = "Composite"
    elif state["trend_active"]:
        logic = "Trend"
    else:
        logic = None
    return overall, logic


def build_cases() -> list[dict[str, str]]:
    cases: list[dict[str, str]] = []
    bull_modules = ["bull_special", "bull_hold"]
    chaos_modules = ["hybrid", "chaos_simple", "flat"]
    bear_modules = ["flat", "bear_deep6", "bear_deep8"]

    for bull in bull_modules:
        for chaos in chaos_modules:
            for bear in bear_modules:
                cases.append({"bull": bull, "chaos": chaos, "bear": bear})
    return cases


def simulate_meta_switch(
    df: pd.DataFrame,
    case: dict[str, str],
    module_lib: dict[str, ModuleDef],
    cost: float = 0.001,
) -> pd.DataFrame:
    out = apply_regime_detector(df.copy(), BEST_DETECTOR)
    n = len(out)

    signals = np.zeros(n, dtype=int)
    actual_pos = np.zeros(n, dtype=int)
    strat_ret = np.zeros(n, dtype=float)

    active_module = [None] * n
    active_logic = [None] * n

    states = {name: reset_advanced_state() for name, mod in module_lib.items() if mod.kind == "advanced"}

    prev_overall = False
    prev_module_name = None

    for i in range(1, n):
        regime = out.at[i, "regime"]
        module_name = case["bull"] if regime == "Bull" else case["bear"] if regime == "Bear" else case["chaos"]

        for name in states:
            if name != module_name:
                states[name] = reset_advanced_state()

        mod = module_lib[module_name]
        if mod.kind == "flat":
            overall = False
            logic = None
        elif mod.kind == "hold":
            overall = True
            logic = "Hold"
        else:
            overall, logic = update_advanced_module(out.iloc[i], states[module_name], mod.params or {})

        if overall and not prev_overall:
            signals[i] = 1
        elif prev_overall and not overall:
            signals[i] = -1
        elif prev_overall and overall and prev_module_name != module_name:
            # State switched but both modules want exposure.
            # Keep the position continuous and only mark the module handoff.
            signals[i] = 0

        active_module[i] = module_name if overall else None
        active_logic[i] = logic
        prev_overall = overall
        prev_module_name = module_name if overall else None

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
    out["active_module"] = active_module
    out["active_logic"] = active_logic
    out["strat_ret"] = strat_ret
    out["bench_ret"] = out["close"].pct_change().fillna(0.0)
    out["strat_nav"] = (1 + out["strat_ret"]).cumprod()
    out["bench_nav"] = (1 + out["bench_ret"]).cumprod()
    return out


def summarize_window(window: pd.DataFrame, prefix: str) -> dict[str, float]:
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

    total = float(strat_nav.iloc[-1] - 1)
    bench = float(bench_nav.iloc[-1] - 1)
    cagr = float(strat_nav.iloc[-1] ** (1 / years) - 1)
    mdd = float(max_drawdown(strat_nav))
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan

    return {
        f"{prefix}_total_pct": round(total * 100, 2),
        f"{prefix}_bench_pct": round(bench * 100, 2),
        f"{prefix}_excess_pct": round((total - bench) * 100, 2),
        f"{prefix}_cagr_pct": round(cagr * 100, 2),
        f"{prefix}_mdd_pct": round(mdd * 100, 2),
        f"{prefix}_calmar": round(calmar, 4) if pd.notna(calmar) else np.nan,
        f"{prefix}_trades": int((window["signal"] == 1).sum()),
        f"{prefix}_hold_pct": round(float(window["actual_pos"].mean() * 100), 2),
    }


def slice_window(df: pd.DataFrame, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if start is not None:
        out = out[out["trade_date"] >= start]
    if end is not None:
        out = out[out["trade_date"] <= end]
    return out.reset_index(drop=True)


def build_baseline_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    current = compute_strategy_frame(df.copy(), cost=0.001)
    hybrid = simulate(df.copy(), BASE_HYBRID)
    bull = simulate(slice_window(df, start="20240924"), BULL_SPECIAL)
    bull["bench_ret"] = bull["close"].pct_change().fillna(0.0)

    baselines = {
        "current_formal_all": current,
        "candidate_hybrid_all": hybrid,
        "bull_special_from_20240924": bull,
    }

    for name, frame in baselines.items():
        row = {"name": name}
        row.update(summarize_window(frame, "metrics"))
        rows.append(row)

    return pd.DataFrame(rows)


def run_search(top: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    RESULT_DIR.mkdir(exist_ok=True)
    src = load_source()
    module_lib = build_module_library()

    rows = []
    best_frame = None

    for case in build_cases():
        frame = simulate_meta_switch(src, case, module_lib)

        row: dict[str, object] = {
            "label": f"bull_{case['bull']}__chaos_{case['chaos']}__bear_{case['bear']}",
            "bull_module": case["bull"],
            "chaos_module": case["chaos"],
            "bear_module": case["bear"],
        }

        windows = {
            "all": frame,
            "valid": slice_window(frame, VALID_START, VALID_END),
            "holdout": slice_window(frame, HOLDOUT_START, None),
            "recent": slice_window(frame, RECENT_START, None),
        }
        for prefix, window in windows.items():
            row.update(summarize_window(window, prefix))

        row["score"] = (
            float(row["holdout_excess_pct"])
            + 0.75 * float(row["recent_excess_pct"])
            + 0.25 * float(row["all_excess_pct"])
            + 0.2 * float(row["valid_excess_pct"])
        )
        rows.append(row)

    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["score", "holdout_excess_pct", "recent_excess_pct", "all_excess_pct", "all_mdd_pct"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    result.to_csv(RESULT_FILE, index=False, encoding="utf-8-sig")
    result.head(top).to_csv(TOP_FILE, index=False, encoding="utf-8-sig")

    best = result.iloc[0]
    best_case = {
        "bull": best["bull_module"],
        "chaos": best["chaos_module"],
        "bear": best["bear_module"],
    }
    best_frame = simulate_meta_switch(src, best_case, module_lib)
    best_frame.to_csv(BEST_FRAME_FILE, index=False, encoding="utf-8-sig")

    baseline = build_baseline_table(src)
    return result, baseline, best_frame


def write_summary(result: pd.DataFrame, baseline: pd.DataFrame) -> None:
    best = result.iloc[0]

    lines = [
        "# Meta Switch Research Summary",
        "",
        f"Date: {STAMP}",
        "",
        "## 1. Goal",
        "",
        "- Keep the current global enhanced rule candidate intact.",
        "- Test whether a fixed bull / bear / chaos detector can improve results by switching strategy modules instead of using one module from start to finish.",
        "",
        "## 2. Fixed detector used in this round",
        "",
        "- Bull enter: breadth_ma20 >= 75, breadth_ma60 >= 50, close > ma120",
        "- Bull exit: breadth_ma20 < 45 or breadth_ma60 < 30 or close < ma120",
        "- Bear enter: breadth_ma20 <= 30, breadth_ma60 <= 35, close < ma120",
        "- Bear exit: breadth_ma20 > 50 or breadth_ma60 > 45 or close > ma120",
        "- Remaining days: Chaos",
        "",
        "## 3. Baselines",
        "",
    ]

    for _, row in baseline.iterrows():
        lines.append(
            f"- {row['name']}: excess `{row['metrics_excess_pct']}%`, "
            f"CAGR `{row['metrics_cagr_pct']}%`, MDD `{row['metrics_mdd_pct']}%`, "
            f"trades `{int(row['metrics_trades'])}`"
        )

    lines.extend(
        [
            "",
            "## 4. Best meta-switch candidate",
            "",
            f"- label: `{best['label']}`",
            f"- Bull module: `{best['bull_module']}`",
            f"- Chaos module: `{best['chaos_module']}`",
            f"- Bear module: `{best['bear_module']}`",
            "",
            "Performance:",
            f"- all excess: `{best['all_excess_pct']}%` | CAGR `{best['all_cagr_pct']}%` | MDD `{best['all_mdd_pct']}%` | trades `{int(best['all_trades'])}`",
            f"- valid excess: `{best['valid_excess_pct']}%`",
            f"- holdout excess: `{best['holdout_excess_pct']}%`",
            f"- recent excess: `{best['recent_excess_pct']}%`",
            "",
            "## 5. Practical read",
            "",
        ]
    )

    if best["all_excess_pct"] > 108.65:
        lines.append("- This round found a modular switch candidate that beats the current hybrid baseline on full-sample excess return.")
    else:
        lines.append("- This round did not beat the current hybrid baseline on full-sample excess return.")

    if best["holdout_excess_pct"] > 23.83:
        lines.append("- It does improve the later holdout window versus the hybrid baseline.")
    else:
        lines.append("- It does not improve the later holdout window enough versus the hybrid baseline.")

    lines.extend(
        [
            "- The real value of this round is to test whether the detector should control module switching, while keeping the current global enhanced rule untouched.",
            "- If this still underperforms, the next iteration should keep the same detector and improve the state modules, not re-open the detector search first.",
            "",
            "## 6. Files",
            "",
            f"- `{RESULT_FILE.relative_to(ROOT)}`",
            f"- `{TOP_FILE.relative_to(ROOT)}`",
            f"- `{BEST_FRAME_FILE.relative_to(ROOT)}`",
        ]
    )

    SUMMARY_FILE.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local meta-switch research for CSI500.")
    parser.add_argument("--top", type=int, default=10, help="Number of top rows to export.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result, baseline, _ = run_search(top=args.top)
    write_summary(result, baseline)
    print(f"Results written to: {RESULT_FILE}")
    print(f"Top rows written to: {TOP_FILE}")
    print(f"Summary written to: {SUMMARY_FILE}")
    print(result.head(args.top).to_string(index=False))


if __name__ == "__main__":
    main()
