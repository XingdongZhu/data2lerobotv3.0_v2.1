#!/usr/bin/env python3
"""Merge LeRobot txt reports by robot type."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_INPUTS = [
    Path("/mnt/fastdisk/report_shanghai.txt"),
    Path("/mnt/fastdisk/report_shanghai2.txt"),
    Path("/mnt/fastdisk/report_zhengzhou.txt"),
    Path("/mnt/fastdisk/report.txt"),
]
DEFAULT_OUTPUT = Path("/mnt/fastdisk/report_merged.txt")


@dataclass
class RobotStats:
    dataset_count: int = 0
    episodes: int = 0
    tasks: int = 0
    frames: int = 0
    hours: float = 0.0
    minutes: float = 0.0

    def add(self, other: "RobotStats") -> None:
        self.dataset_count += other.dataset_count
        self.episodes += other.episodes
        self.tasks += other.tasks
        self.frames += other.frames
        self.hours += other.hours
        self.minutes += other.minutes


def parse_int(text: str) -> int:
    return int(text.replace(",", ""))


def parse_robot_stats(report_path: Path) -> dict[str, RobotStats]:
    stats: dict[str, RobotStats] = {}
    current_robot: str | None = None
    in_robot_section = False

    for raw_line in report_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()

        if line.strip() == "【机器人类型统计】":
            in_robot_section = True
            current_robot = None
            continue

        if in_robot_section and line.startswith("【") and line.strip() != "【机器人类型统计】":
            break

        if not in_robot_section:
            continue

        robot_match = re.match(r"^\s{2}([^:\s][^:]*):\s*$", line)
        if robot_match:
            current_robot = robot_match.group(1).strip()
            stats.setdefault(current_robot, RobotStats())
            continue

        if current_robot is None:
            continue

        robot_stats = stats[current_robot]

        if match := re.search(r"数据集数量:\s*([\d,]+)", line):
            robot_stats.dataset_count = parse_int(match.group(1))
        elif match := re.search(r"总 Episodes:\s*([\d,]+)", line):
            robot_stats.episodes = parse_int(match.group(1))
        elif match := re.search(r"总 Tasks:\s*([\d,]+)", line):
            robot_stats.tasks = parse_int(match.group(1))
        elif match := re.search(r"总帧数:\s*([\d,]+)", line):
            robot_stats.frames = parse_int(match.group(1))
        elif match := re.search(r"总时长:\s*([\d.]+)\s*小时\s*\(([\d.]+)\s*分钟", line):
            robot_stats.hours = float(match.group(1))
            robot_stats.minutes = float(match.group(2))

    return stats


def merge_reports(report_paths: list[Path]) -> dict[str, RobotStats]:
    merged: dict[str, RobotStats] = {}

    for report_path in report_paths:
        if not report_path.is_file():
            raise FileNotFoundError(f"report file not found: {report_path}")

        for robot_type, stats in parse_robot_stats(report_path).items():
            merged.setdefault(robot_type, RobotStats()).add(stats)

    return merged


def format_report(merged: dict[str, RobotStats], input_paths: list[Path]) -> str:
    total_datasets = sum(stats.dataset_count for stats in merged.values())
    total_episodes = sum(stats.episodes for stats in merged.values())
    total_tasks = sum(stats.tasks for stats in merged.values())
    total_frames = sum(stats.frames for stats in merged.values())
    total_hours = sum(stats.hours for stats in merged.values())
    total_minutes = sum(stats.minutes for stats in merged.values())
    total_seconds = total_minutes * 60

    avg_episodes_per_dataset = total_episodes / total_datasets if total_datasets else 0
    avg_frames_per_episode = total_frames / total_episodes if total_episodes else 0
    avg_seconds_per_episode = total_seconds / total_episodes if total_episodes else 0

    lines = [
        "",
        "=" * 100,
        "LeRobot 数据集统计合并报告",
        "=" * 100,
        "",
        "【输入文件】",
        "-" * 100,
    ]
    lines.extend(f"  - {path}" for path in input_paths)
    lines.extend(
        [
            "",
            "【总体统计】",
            "-" * 100,
            f"  总数据集数量: {total_datasets:,}",
            f"  总 Episodes 数: {total_episodes:,}",
            f"  总 Tasks 数: {total_tasks:,}",
            f"  总帧数: {total_frames:,}",
            f"  总时长: {total_hours:.2f} 小时 ({total_minutes:.1f} 分钟, {total_seconds:.1f} 秒)",
            f"  平均每个数据集的 Episodes: {avg_episodes_per_dataset:.1f}",
            f"  平均每个 Episode 的帧数: {avg_frames_per_episode:.1f}",
            f"  平均每个 Episode 的时长: {avg_seconds_per_episode:.1f} 秒",
            "",
            "【机器人类型统计】",
            "-" * 100,
        ]
    )

    for robot_type, stats in sorted(merged.items()):
        dataset_percent = stats.dataset_count * 100 / total_datasets if total_datasets else 0
        avg_episodes = stats.episodes / stats.dataset_count if stats.dataset_count else 0
        avg_frames = stats.frames / stats.episodes if stats.episodes else 0
        avg_seconds = stats.minutes * 60 / stats.episodes if stats.episodes else 0

        lines.extend(
            [
                f"  {robot_type}:",
                f"    - 数据集数量: {stats.dataset_count:,} ({dataset_percent:.1f}%)",
                f"    - 总 Episodes: {stats.episodes:,}",
                f"    - 总 Tasks: {stats.tasks:,}",
                f"    - 总帧数: {stats.frames:,}",
                f"    - 总时长: {stats.hours:.2f} 小时 ({stats.minutes:.1f} 分钟)",
                f"    - 平均 Episodes/数据集: {avg_episodes:.1f}",
                f"    - 平均帧数/Episode: {avg_frames:.1f}",
                f"    - 平均时长/Episode: {avg_seconds:.1f} 秒",
            ]
        )

    lines.extend(["", "=" * 100, ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge LeRobot report txt files by robot type.")
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        default=DEFAULT_INPUTS,
        help="input report txt paths",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"output merged txt path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    merged = merge_reports(args.inputs)
    output_text = format_report(merged, args.inputs)
    args.output.write_text(output_text, encoding="utf-8")
    print(f"Merged report written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
