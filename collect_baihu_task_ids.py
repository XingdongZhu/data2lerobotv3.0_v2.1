#!/usr/bin/env python3
"""Collect LeRobot task IDs under BAIHU dataset roots and export to Excel sheets."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


DEFAULT_ROOTS = {
    "BAIHU_v2.0": Path("/qinglong_datasets/qinglong/lerobotv21/BAIHU_v2.0"),
    "BAIHU_v3.0-p2": Path("/qinglong_datasets/qinglong/lerobotv21/BAIHU_v3.0-p2"),
    "BAIHU_v3.0-p3": Path("/qinglong_datasets/qinglong/lerobotv21/BAIHU_v3.0-p3"),
}

TASK_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def is_task_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if not TASK_ID_PATTERN.fullmatch(path.name):
        return False
    return (path / "meta").is_dir() or (path / "data").is_dir()


def collect_task_ids(root: Path) -> list[dict[str, str]]:
    if not root.exists():
        raise FileNotFoundError(f"Dataset root not found: {root}")

    records: list[dict[str, str]] = []
    for robot_dir in sorted(root.iterdir()):
        if not robot_dir.is_dir():
            continue
        for task_dir in sorted(robot_dir.iterdir()):
            if not is_task_dir(task_dir):
                continue
            records.append(
                {
                    "task_id": task_dir.name,
                    "robot_type": robot_dir.name,
                    "path": str(task_dir),
                }
            )
    return records


def export_to_excel(
    datasets: dict[str, list[dict[str, str]]],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, records in datasets.items():
            df = pd.DataFrame(records, columns=["task_id", "robot_type", "path"])
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "logs" / "baihu_task_ids.xlsx",
        help="Output Excel file path",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("/qinglong_datasets/qinglong/lerobotv21"),
        help="Parent directory containing BAIHU_* folders",
    )
    args = parser.parse_args()

    datasets: dict[str, list[dict[str, str]]] = {}
    print("Collecting task IDs...")
    for sheet_name, default_root in DEFAULT_ROOTS.items():
        root = args.base_dir / sheet_name if args.base_dir != DEFAULT_ROOTS[sheet_name].parent else default_root
        if not root.exists():
            root = default_root
        records = collect_task_ids(root)
        datasets[sheet_name] = records
        print(f"  {sheet_name}: {len(records)} task(s)")

    export_to_excel(datasets, args.output)

    total = sum(len(records) for records in datasets.values())
    print(f"\nSaved {total} task IDs to {args.output}")
    for sheet_name, records in datasets.items():
        print(f"  sheet '{sheet_name}': {len(records)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
