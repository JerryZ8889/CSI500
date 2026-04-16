#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plan, apply, and restore a local archive for non-current CSI500 component files.

This tool is intentionally local-first:
- `plan` only writes a manifest and does not move any files
- `apply` moves files from stocks_data/ into stocks_archive/
- `restore` moves files back using the manifest
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
STOCKS_DIR = ROOT / "stocks_data"
ARCHIVE_DIR = ROOT / "stocks_archive"
DEFAULT_MANIFEST = ROOT / "archive_manifests" / "old_component_archive_manifest.json"
COMP_FILE = ROOT / "csi500_components_schedule.csv"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_current_component_codes() -> tuple[str, set[str]]:
    comp = pd.read_csv(COMP_FILE, usecols=["asof_date", "con_code"])
    comp["asof_date"] = comp["asof_date"].astype(str).str.strip()
    comp["con_code"] = comp["con_code"].astype(str).str.strip()
    latest_asof = max(comp["asof_date"].unique())
    current_codes = set(comp.loc[comp["asof_date"] == latest_asof, "con_code"])
    return latest_asof, current_codes


def build_manifest() -> dict:
    latest_asof, current_codes = load_current_component_codes()
    source_files = sorted(STOCKS_DIR.glob("*.csv"))
    file_codes = {path.stem for path in source_files}
    old_codes = sorted(file_codes - current_codes)

    moves = []
    for code in old_codes:
        source_path = STOCKS_DIR / f"{code}.csv"
        archive_path = ARCHIVE_DIR / f"{code}.csv"
        moves.append(
            {
                "code": code,
                "source_rel": source_path.relative_to(ROOT).as_posix(),
                "archive_rel": archive_path.relative_to(ROOT).as_posix(),
                "bytes": source_path.stat().st_size,
                "sha256": sha256_file(source_path),
            }
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "latest_asof": latest_asof,
        "stocks_dir": STOCKS_DIR.relative_to(ROOT).as_posix(),
        "archive_dir": ARCHIVE_DIR.relative_to(ROOT).as_posix(),
        "current_component_count": len(current_codes),
        "stocks_file_count": len(source_files),
        "old_component_file_count": len(moves),
        "moves": moves,
    }


def save_manifest(manifest: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def load_manifest(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def apply_manifest(manifest: dict) -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0
    for item in manifest["moves"]:
        source_path = ROOT / item["source_rel"]
        archive_path = ROOT / item["archive_rel"]
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        if archive_path.exists():
            raise RuntimeError(f"Archive target already exists: {archive_path}")
        if not source_path.exists():
            raise RuntimeError(f"Source file missing: {source_path}")
        if sha256_file(source_path) != item["sha256"]:
            raise RuntimeError(f"Source file hash changed since plan: {source_path}")

        source_path.replace(archive_path)
        moved += 1

    print(f"Archived {moved} files into {ARCHIVE_DIR}")


def restore_manifest(manifest: dict) -> None:
    restored = 0
    for item in manifest["moves"]:
        source_path = ROOT / item["source_rel"]
        archive_path = ROOT / item["archive_rel"]

        if source_path.exists():
            raise RuntimeError(f"Restore target already exists: {source_path}")
        if not archive_path.exists():
            raise RuntimeError(f"Archived file missing: {archive_path}")
        if sha256_file(archive_path) != item["sha256"]:
            raise RuntimeError(f"Archived file hash changed since plan: {archive_path}")

        source_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.replace(source_path)
        restored += 1

    print(f"Restored {restored} files back into {STOCKS_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive old CSI500 component files")
    parser.add_argument("command", choices=["plan", "apply", "restore"])
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="Manifest JSON path")
    args = parser.parse_args()

    manifest_path = Path(args.manifest).resolve()

    if args.command == "plan":
        manifest = build_manifest()
        save_manifest(manifest, manifest_path)
        print(f"Manifest written to: {manifest_path}")
        print(f"Latest asof: {manifest['latest_asof']}")
        print(f"Old component files: {manifest['old_component_file_count']}")
        return

    manifest = load_manifest(manifest_path)

    if args.command == "apply":
        apply_manifest(manifest)
    elif args.command == "restore":
        restore_manifest(manifest)


if __name__ == "__main__":
    main()
