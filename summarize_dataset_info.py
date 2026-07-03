#!/usr/bin/env python3
"""Summarize dataset stats from datasets.yaml -> each dataset_path/meta/info.json.

Outputs per-embodiment totals:
  - total_episodes
  - total_frames
  - total_tasks
  - duration by fps
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


def format_hours(seconds: float) -> float:
    return max(float(seconds), 0.0) / 3600.0


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("datasets.yaml root must be a mapping")
    if "datasets" not in data or not isinstance(data["datasets"], list):
        raise ValueError("datasets.yaml must contain key 'datasets' as a list")
    return data


def summarize(config: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    summaries: list[dict[str, Any]] = []
    warnings: list[str] = []

    for idx, item in enumerate(config["datasets"]):
        if not isinstance(item, dict):
            warnings.append(f"[item {idx}] ignored: not a mapping")
            continue
        tag = str(item.get("embodiment_tag", f"unknown_{idx}"))
        paths = item.get("dataset_paths", [])
        if not isinstance(paths, list) or not paths:
            warnings.append(f"[{tag}] ignored: dataset_paths missing/empty")
            continue

        total_episodes = 0
        total_frames = 0
        total_tasks = 0
        total_seconds = 0.0
        num_datasets_ok = 0
        fps_buckets: dict[float, int] = defaultdict(int)

        for p in paths:
            dataset_path = Path(str(p))
            info_path = dataset_path / "meta" / "info.json"
            if not info_path.exists():
                warnings.append(f"[{tag}] missing info.json: {info_path}")
                continue
            try:
                with info_path.open("r", encoding="utf-8") as f:
                    info = json.load(f)
            except Exception as e:  # noqa: BLE001
                warnings.append(f"[{tag}] failed reading {info_path}: {e}")
                continue

            episodes = int(info.get("total_episodes", 0) or 0)
            frames = int(info.get("total_frames", 0) or 0)
            tasks = int(info.get("total_tasks", 0) or 0)
            fps = info.get("fps", None)
            fps_value = None
            try:
                if fps is not None:
                    fps_value = float(fps)
            except Exception:  # noqa: BLE001
                fps_value = None

            total_episodes += episodes
            total_frames += frames
            total_tasks += tasks
            num_datasets_ok += 1
            if fps_value and fps_value > 0:
                total_seconds += frames / fps_value
                fps_buckets[fps_value] += 1
            else:
                warnings.append(f"[{tag}] invalid fps in {info_path}: {fps}")

        summaries.append(
            {
                "embodiment_tag": tag,
                "num_dataset_paths": len(paths),
                "num_info_found": num_datasets_ok,
                "total_episodes": total_episodes,
                "total_frames": total_frames,
                "total_tasks": total_tasks,
                "total_seconds": total_seconds,
                "fps_buckets": dict(sorted(fps_buckets.items(), key=lambda kv: kv[0])),
            }
        )

    return summaries, warnings


def write_report(
    yaml_path: Path,
    out_path: Path,
    summaries: list[dict[str, Any]],
    warnings: list[str],
) -> None:
    lines: list[str] = []
    total_hours_all = sum(format_hours(s["total_seconds"]) for s in summaries)

    # Tab-separated columns for easier paste into Excel.
    lines.append("embodiment_tag\ttasks\tepisodes\tframes\thours\thours_percent")
    for s in sorted(summaries, key=lambda x: x["embodiment_tag"]):
        hours = format_hours(s["total_seconds"])
        percent = (hours / total_hours_all * 100.0) if total_hours_all > 0 else 0.0
        lines.append(
            f"{s['embodiment_tag']}\t{s['total_tasks']}\t{s['total_episodes']}\t"
            f"{s['total_frames']}\t{hours:.2f}\t{percent:.2f}%"
        )

    sum_tasks = sum(s["total_tasks"] for s in summaries)
    sum_episodes = sum(s["total_episodes"] for s in summaries)
    sum_frames = sum(s["total_frames"] for s in summaries)
    lines.append(
        f"sum\t{sum_tasks}\t{sum_episodes}\t{sum_frames}\t"
        f"{total_hours_all:.2f}\t100.00%"
    )

    if warnings:
        lines.append("")
        lines.append("warnings")
        for w in warnings:
            lines.append(w)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize meta/info.json stats by embodiment")
    parser.add_argument(
        "--yaml-path",
        type=Path,
        default=Path("/workspace2/zxd/scripts/datasets_baihu31_fromLJ_20260622.yaml"),
        help="Path to datasets.yaml",
    )
    parser.add_argument(
        "--output-txt",
        type=Path,
        default=Path("/workspace2/zxd/scripts/datasets_baihu31_fromLJ_20260622_info_summary.txt"),
        help="Output txt report path",
    )
    args = parser.parse_args()

    config = load_yaml(args.yaml_path)
    summaries, warnings = summarize(config)
    write_report(args.yaml_path, args.output_txt, summaries, warnings)
    print(f"Report written to: {args.output_txt}")


if __name__ == "__main__":
    main()
