#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create and verify a local behavior baseline for the CSI500 project.

This tool is intentionally conservative:
- It snapshots key data-file hashes
- It snapshots backtest outputs from the current implementation
- It snapshots the current dashboard decision state from the current implementation

The goal is to protect "pure refactor" work from accidentally changing behavior.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import importlib
import io
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "baseline" / "behavior_baseline.json"

CORE_FILES = [
    ROOT / "strategy_data.csv",
    ROOT / "csi500_components_schedule.csv",
    ROOT / "adj_factor_base.csv",
]

STOCKS_DIR = ROOT / "stocks_data"
ARCHIVE_DIR = ROOT / "stocks_archive"


def ensure_repo_on_path() -> None:
    root_str = str(ROOT)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def line_count(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f)


def first_data_trade_date(path: Path) -> str | None:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        row = next(reader, None)
    if not row or len(row) < 2:
        return None
    return row[1]


def normalize_scalar(value: Any) -> Any:
    import numpy as np
    import pandas as pd

    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if pd.isna(value):
            return None
        return round(float(value), 12)
    if hasattr(value, "item"):
        try:
            return normalize_scalar(value.item())
        except Exception:
            pass
    return value


def hash_dataframe(df: Any, columns: list[str]) -> str:
    payload = df.loc[:, columns].to_csv(
        index=False,
        na_rep="NaN",
        float_format="%.15g",
    )
    return sha256_text(payload)


def snapshot_stock_directory(path: Path) -> dict[str, Any]:
    stocks_manifest: dict[str, Any] = {}
    combined = hashlib.sha256()
    last_date_buckets: dict[str, int] = {}

    for file_path in sorted(path.glob("*.csv")):
        rel = file_path.relative_to(ROOT).as_posix()
        last_trade_date = first_data_trade_date(file_path)
        file_sha = sha256_file(file_path)
        stocks_manifest[rel] = {
            "bytes": file_path.stat().st_size,
            "sha256": file_sha,
            "last_trade_date": last_trade_date,
        }
        combined.update(rel.encode("utf-8"))
        combined.update(file_sha.encode("utf-8"))
        if last_trade_date is not None:
            last_date_buckets[last_trade_date] = last_date_buckets.get(last_trade_date, 0) + 1

    return {
        "file_count": len(stocks_manifest),
        "aggregate_sha256": combined.hexdigest(),
        "last_trade_dates": dict(sorted(last_date_buckets.items())),
        "files": stocks_manifest,
    }


def snapshot_files() -> dict[str, Any]:
    result: dict[str, Any] = {}

    for path in CORE_FILES:
        result[path.name] = {
            "bytes": path.stat().st_size,
            "lines": line_count(path),
            "sha256": sha256_file(path),
        }

    stock_dirs = [dir_path for dir_path in [STOCKS_DIR, ARCHIVE_DIR] if dir_path.is_dir()]
    total_files = 0
    total_combined = hashlib.sha256()
    total_last_date_buckets: dict[str, int] = {}

    for dir_path in stock_dirs:
        section_name = dir_path.name
        directory_snapshot = snapshot_stock_directory(dir_path)
        result[section_name] = directory_snapshot

        total_files += directory_snapshot["file_count"]
        total_combined.update(section_name.encode("utf-8"))
        total_combined.update(directory_snapshot["aggregate_sha256"].encode("utf-8"))
        for trade_date, count in directory_snapshot["last_trade_dates"].items():
            total_last_date_buckets[trade_date] = total_last_date_buckets.get(trade_date, 0) + count

    result["stock_files_total"] = {
        "file_count": total_files,
        "directories": [dir_path.name for dir_path in stock_dirs],
        "aggregate_sha256": total_combined.hexdigest(),
        "last_trade_dates": dict(sorted(total_last_date_buckets.items())),
    }
    return result


def fresh_import(module_name: str) -> Any:
    if module_name in sys.modules:
        del sys.modules[module_name]
    importlib.invalidate_caches()
    return importlib.import_module(module_name)


def snapshot_backtest() -> dict[str, Any]:
    # Preload dependencies before patching subprocess.Popen.
    os.environ.setdefault("MPLBACKEND", "Agg")
    import pandas  # noqa: F401
    import matplotlib  # noqa: F401
    import matplotlib.pyplot  # noqa: F401
    from unittest.mock import patch

    ensure_repo_on_path()
    import_logs_out = io.StringIO()
    import_logs_err = io.StringIO()

    with (
        contextlib.redirect_stdout(import_logs_out),
        contextlib.redirect_stderr(import_logs_err),
        patch("matplotlib.figure.Figure.savefig", lambda *a, **k: None),
        patch("matplotlib.pyplot.show", lambda *a, **k: None),
        patch("subprocess.Popen", lambda *a, **k: None),
    ):
        module = fresh_import("backtest")

    df = module.df.copy()
    buy_count = int((df["signal"] == 1).sum())
    sell_count = int((df["signal"] == -1).sum())

    return {
        "rows": int(len(df)),
        "trade_date_start": str(df["trade_date"].iloc[0]),
        "trade_date_end": str(df["trade_date"].iloc[-1]),
        "buy_signal_count": buy_count,
        "sell_signal_count": sell_count,
        "open_position_at_end": int(df["actual_pos"].iloc[-1]),
        "n_trades": int(module.n_trades),
        "win_rate": normalize_scalar(module.win_rate),
        "strat_total": normalize_scalar(module.strat_total),
        "bench_total": normalize_scalar(module.bench_total),
        "strat_mdd": normalize_scalar(module.strat_mdd),
        "bench_mdd": normalize_scalar(module.bench_mdd),
        "last_signal": int(df["signal"].iloc[-1]),
        "last_actual_pos": int(df["actual_pos"].iloc[-1]),
        "series_hash": hash_dataframe(
            df,
            ["trade_date", "signal", "actual_pos", "strat_ret", "strat_nav", "bench_nav"],
        ),
    }


def snapshot_dashboard() -> dict[str, Any]:
    os.environ.setdefault("MPLBACKEND", "Agg")
    ensure_repo_on_path()

    import_logs_out = io.StringIO()
    import_logs_err = io.StringIO()
    with contextlib.redirect_stdout(import_logs_out), contextlib.redirect_stderr(import_logs_err):
        module = fresh_import("dashboard")

    df = module.df.copy()
    last = module.last

    return {
        "rows": int(len(df)),
        "trade_date_start": str(df["trade_date"].iloc[0]),
        "trade_date_end": str(df["trade_date"].iloc[-1]),
        "n_trades": int(module.n_trades),
        "win_rate": normalize_scalar(module.win_rate),
        "strat_total": normalize_scalar(module.strat_total),
        "bench_total": normalize_scalar(module.bench_total),
        "strat_mdd": normalize_scalar(module.strat_mdd),
        "bench_mdd": normalize_scalar(module.bench_mdd),
        "excess": normalize_scalar(module.excess),
        "last_signal": int(last["signal"]),
        "last_actual_pos": int(last["actual_pos"]),
        "last_logic_state": normalize_scalar(last["logic_state"]),
        "last_exit_reason": normalize_scalar(last["exit_reason"]),
        "last_trade_date": str(last["trade_date"]),
        "last_close": normalize_scalar(last["close"]),
        "last_breadth": normalize_scalar(module.breadth_val),
        "last_heat_z": normalize_scalar(module.hz_val),
        "last_etf_turnover": normalize_scalar(module.turn_val),
        "last_ma_5": normalize_scalar(module.ma5_val),
        "last_ma_10": normalize_scalar(module.ma10_val),
        "last_ma_30": normalize_scalar(module.ma30_val),
        "mode_text": normalize_scalar(module.mode_text),
        "mode_desc": normalize_scalar(module.mode_desc),
        "act_text": normalize_scalar(module.act_text),
        "act_desc": normalize_scalar(module.act_desc),
        "ref_tip": module.ref_tip,
        "reasons": list(module.reasons),
        "risks": list(module.risks),
        "virtual_firstneg": {
            "active": bool(module.vfn["active"]),
            "entry_date": normalize_scalar(module.vfn["entry_date"]),
            "entry_high": normalize_scalar(module.vfn["entry_high"]),
            "held_days": normalize_scalar(module.vfn["held_days"]),
            "exit_signal": normalize_scalar(module.vfn["exit_signal"]),
        },
        "series_hash": hash_dataframe(
            df,
            [
                "trade_date",
                "signal",
                "actual_pos",
                "logic_state",
                "exit_reason",
                "strat_ret",
                "strat_nav",
                "bench_nav",
            ],
        ),
    }


def build_snapshot() -> dict[str, Any]:
    return {
        "meta": {
            "generated_at": normalize_scalar(__import__("datetime").datetime.now().isoformat(timespec="seconds")),
            "python": sys.version.split()[0],
        },
        "files": snapshot_files(),
        "backtest": snapshot_backtest(),
        "dashboard": snapshot_dashboard(),
    }


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def compare_values(expected: Any, actual: Any, path: str, diffs: list[str]) -> None:
    if type(expected) is not type(actual):
        diffs.append(f"{path}: type mismatch {type(expected).__name__} != {type(actual).__name__}")
        return

    if isinstance(expected, dict):
        expected_keys = set(expected)
        actual_keys = set(actual)
        for missing in sorted(expected_keys - actual_keys):
            diffs.append(f"{path}.{missing}: missing in current snapshot")
        for extra in sorted(actual_keys - expected_keys):
            diffs.append(f"{path}.{extra}: unexpected key in current snapshot")
        for key in sorted(expected_keys & actual_keys):
            compare_values(expected[key], actual[key], f"{path}.{key}", diffs)
        return

    if isinstance(expected, list):
        if len(expected) != len(actual):
            diffs.append(f"{path}: list length mismatch {len(expected)} != {len(actual)}")
            return
        for idx, (lhs, rhs) in enumerate(zip(expected, actual)):
            compare_values(lhs, rhs, f"{path}[{idx}]", diffs)
        return

    if expected != actual:
        diffs.append(f"{path}: expected {expected!r}, got {actual!r}")


def comparable_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "files": snapshot["files"],
        "backtest": snapshot["backtest"],
        "dashboard": snapshot["dashboard"],
    }


def cmd_snapshot(output: Path) -> int:
    snapshot = build_snapshot()
    save_json(output, snapshot)
    print(f"Baseline written to: {output}")
    print(f"Backtest rows: {snapshot['backtest']['rows']}")
    print(f"Dashboard rows: {snapshot['dashboard']['rows']}")
    print(f"stocks_data files: {snapshot['files'].get('stocks_data', {}).get('file_count', 0)}")
    print(f"stocks_archive files: {snapshot['files'].get('stocks_archive', {}).get('file_count', 0)}")
    print(f"stock files total: {snapshot['files']['stock_files_total']['file_count']}")
    return 0


def cmd_verify(input_path: Path) -> int:
    baseline = comparable_snapshot(load_json(input_path))
    current = comparable_snapshot(build_snapshot())
    diffs: list[str] = []
    compare_values(baseline, current, "baseline", diffs)

    if diffs:
        print("Behavior baseline mismatch detected:")
        for line in diffs[:50]:
            print(f"- {line}")
        if len(diffs) > 50:
            print(f"... and {len(diffs) - 50} more differences")
        return 1

    print("Behavior baseline matches current snapshot.")
    print(f"Verified against: {input_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or verify a local behavior baseline.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_snapshot = sub.add_parser("snapshot", help="Generate a new baseline snapshot.")
    p_snapshot.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_BASELINE,
        help=f"Output path (default: {DEFAULT_BASELINE})",
    )

    p_verify = sub.add_parser("verify", help="Verify current behavior against a baseline snapshot.")
    p_verify.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_BASELINE,
        help=f"Baseline path (default: {DEFAULT_BASELINE})",
    )

    args = parser.parse_args()
    if args.command == "snapshot":
        return cmd_snapshot(args.output)
    if args.command == "verify":
        return cmd_verify(args.input)
    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
