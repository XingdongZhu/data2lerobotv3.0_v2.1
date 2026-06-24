#!/usr/bin/env python3
"""Check frame-count consistency across parquet, ffmpeg-decoded videos, and episodes.jsonl."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm


@dataclass
class EpisodeCheckResult:
    task_name: str
    episode_index: int
    meta_length: int | None
    parquet_frames: int | None = None
    video_frames: dict[str, int | None] = field(default_factory=dict)
    video_meta_frames: dict[str, int | None] = field(default_factory=dict)
    missing_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        return not self.missing_files and not self.errors and not self._frame_mismatches()

    def _frame_mismatches(self) -> list[str]:
        issues: list[str] = []
        camera_counts = {k: v for k, v in self.video_frames.items() if v is not None}

        if len(camera_counts) > 1 and len(set(camera_counts.values())) > 1:
            issues.append(
                "camera_frame_mismatch: "
                + ", ".join(f"{k}={v}" for k, v in sorted(camera_counts.items()))
            )

        reference = self.meta_length
        if reference is None:
            return issues

        if self.parquet_frames is not None and self.parquet_frames != reference:
            issues.append(f"parquet_vs_meta: parquet={self.parquet_frames}, meta={reference}")

        for camera, frames in sorted(camera_counts.items()):
            if frames != reference:
                issues.append(f"{camera}_vs_meta: video={frames}, meta={reference}")

        if self.parquet_frames is not None and camera_counts:
            for camera, frames in sorted(camera_counts.items()):
                if frames != self.parquet_frames:
                    issues.append(
                        f"{camera}_vs_parquet: video={frames}, parquet={self.parquet_frames}"
                    )

        for camera, decode_frames in sorted(camera_counts.items()):
            meta_frames = self.video_meta_frames.get(camera)
            if meta_frames is not None and decode_frames != meta_frames:
                issues.append(
                    f"{camera}_meta_vs_decode: meta={meta_frames}, decode={decode_frames}"
                )

        return issues

    def detail_line(self) -> str:
        parts = [f"episode_{self.episode_index:06d}"]
        parts.append(f"meta={self._fmt(self.meta_length)}")
        parts.append(f"parquet={self._fmt(self.parquet_frames)}")
        for camera, frames in sorted(self.video_frames.items()):
            meta_frames = self.video_meta_frames.get(camera)
            if (
                frames is not None
                and meta_frames is not None
                and frames != meta_frames
            ):
                parts.append(f"{camera}={self._fmt(frames)}(meta={meta_frames})")
            else:
                parts.append(f"{camera}={self._fmt(frames)}")

        if self.is_ok:
            parts.append("OK")
        else:
            issues = []
            for path in self.missing_files:
                issues.append(f"missing={path}")
            for err in self.errors:
                issues.append(f"error={err}")
            for issue in self._frame_mismatches():
                issues.append(issue)
            parts.append("FAIL: " + "; ".join(issues))
        return ", ".join(parts)

    @staticmethod
    def _fmt(value: int | None) -> str:
        return "N/A" if value is None else str(value)

    def summary_lines(self) -> list[str]:
        lines = [f"[{self.task_name}] episode {self.episode_index}"]
        lines.append(f"  {self.detail_line()}")
        return lines


def load_info(dataset_root: Path) -> dict:
    info_path = dataset_root / "meta" / "info.json"
    with open(info_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_episodes(dataset_root: Path) -> dict[int, int]:
    episodes_path = dataset_root / "meta" / "episodes.jsonl"
    episodes: dict[int, int] = {}
    with open(episodes_path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            episodes[int(item["episode_index"])] = int(item["length"])
    return episodes


def get_video_cameras(info: dict) -> list[str]:
    return sorted(
        key.removeprefix("observation.images.")
        for key, feature in info["features"].items()
        if key.startswith("observation.images.") and feature.get("dtype") == "video"
    )


def parquet_path(dataset_root: Path, info: dict, episode_index: int) -> Path:
    chunk = episode_index // info["chunks_size"]
    rel = info["data_path"].format(episode_chunk=chunk, episode_index=episode_index)
    return dataset_root / rel


def video_path(dataset_root: Path, info: dict, episode_index: int, camera: str) -> Path:
    chunk = episode_index // info["chunks_size"]
    video_key = f"observation.images.{camera}"
    rel = info["video_path"].format(
        episode_chunk=chunk,
        video_key=video_key,
        episode_index=episode_index,
    )
    return dataset_root / rel


def count_parquet_frames(path: Path) -> int:
    return pq.read_metadata(path).num_rows


def count_video_frames_ffprobe(path: Path) -> int:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_frames",
        "-of",
        "csv=p=0",
        str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    value = result.stdout.strip()
    if not value or value == "N/A":
        fallback_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-count_packets",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=nb_read_packets",
            "-of",
            "csv=p=0",
            str(path),
        ]
        result = subprocess.run(fallback_cmd, capture_output=True, text=True, check=True)
        value = result.stdout.strip()
    if not value or value == "N/A":
        raise ValueError(f"unable to determine metadata frame count for {path}")
    return int(value)


def count_video_frames_ffmpeg(path: Path) -> int:
    cmd = [
        "ffmpeg",
        "-i",
        str(path),
        "-map",
        "0:v:0",
        "-f",
        "null",
        "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    matches = re.findall(r"frame=\s*(\d+)", result.stderr)
    if not matches:
        stderr_tail = result.stderr[-500:] if result.stderr else "no stderr"
        raise ValueError(f"unable to decode/count frames for {path}: {stderr_tail}")
    return int(matches[-1])


def check_episode(
    task_name: str,
    dataset_root: Path,
    info: dict,
    cameras: list[str],
    episode_index: int,
    meta_length: int,
) -> EpisodeCheckResult:
    result = EpisodeCheckResult(
        task_name=task_name,
        episode_index=episode_index,
        meta_length=meta_length,
    )

    parquet_file = parquet_path(dataset_root, info, episode_index)
    if not parquet_file.exists():
        result.missing_files.append(str(parquet_file))
    else:
        try:
            result.parquet_frames = count_parquet_frames(parquet_file)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"parquet: {exc}")

    for camera in cameras:
        video_file = video_path(dataset_root, info, episode_index, camera)
        if not video_file.exists():
            result.missing_files.append(str(video_file))
            result.video_frames[camera] = None
            result.video_meta_frames[camera] = None
            continue
        try:
            result.video_meta_frames[camera] = count_video_frames_ffprobe(video_file)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{camera}_ffprobe: {exc}")
            result.video_meta_frames[camera] = None
        try:
            result.video_frames[camera] = count_video_frames_ffmpeg(video_file)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{camera}_ffmpeg: {exc}")
            result.video_frames[camera] = None

    return result


def check_dataset(task_dir: Path, workers: int) -> list[EpisodeCheckResult]:
    info = load_info(task_dir)
    episodes = load_episodes(task_dir)
    cameras = get_video_cameras(info)
    results: list[EpisodeCheckResult] = []

    jobs = [
        (task_dir.name, task_dir, info, cameras, episode_index, meta_length)
        for episode_index, meta_length in sorted(episodes.items())
    ]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(check_episode, *job)
            for job in jobs
        ]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: item.episode_index)
    return results


def build_task_report(results: list[EpisodeCheckResult]) -> list[str]:
    if not results:
        return []

    task_name = results[0].task_name
    issue_count = sum(1 for result in results if not result.is_ok)
    lines = [
        f"=== {task_name} ===",
        f"episodes: {len(results)}, ok: {len(results) - issue_count}, issues: {issue_count}",
        "",
    ]
    for result in results:
        lines.append(result.detail_line())
    lines.append("")
    return lines


def build_report(
    dataset_root: Path,
    task_dirs: list[Path],
    all_results: dict[str, list[EpisodeCheckResult]],
) -> str:
    total_episodes = sum(len(results) for results in all_results.values())
    total_issues = sum(
        1
        for results in all_results.values()
        for result in results
        if not result.is_ok
    )

    lines = [
        "LeRobot frame consistency report",
        f"dataset_root: {dataset_root}",
        f"tasks_checked: {len(task_dirs)}",
        f"episodes_checked: {total_episodes}",
        f"episodes_ok: {total_episodes - total_issues}",
        f"episodes_with_issues: {total_issues}",
        "",
    ]

    for task_dir in task_dirs:
        task_results = all_results.get(task_dir.name, [])
        lines.extend(build_task_report(task_results))

    return "\n".join(lines)


def discover_task_dirs(root: Path, task: str | None) -> list[Path]:
    if task is not None:
        task_dir = root / task
        if not task_dir.is_dir():
            raise FileNotFoundError(f"Task directory not found: {task_dir}")
        return [task_dir]

    if (root / "meta" / "episodes.jsonl").exists():
        return [root]

    task_dirs = sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and (path / "meta" / "episodes.jsonl").exists()
    )
    if not task_dirs:
        raise FileNotFoundError(f"No LeRobot task directories found under {root}")
    return task_dirs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dataset_root",
        type=Path,
        help="Root directory containing multiple task folders, e.g. .../sim_aloha",
    )
    parser.add_argument(
        "--task",
        type=str,
        default=None,
        help="Only check one task folder name under dataset_root",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers per task (ffmpeg decode is CPU-heavy)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "logs" / "frame_consistency_report.txt",
        help="Path to save the detailed text report",
    )
    args = parser.parse_args()

    task_dirs = discover_task_dirs(args.dataset_root, args.task)
    all_results: dict[str, list[EpisodeCheckResult]] = {}

    print(f"Checking {len(task_dirs)} task(s) under {args.dataset_root}")
    for task_dir in tqdm(task_dirs, desc="tasks"):
        results = check_dataset(task_dir, args.workers)
        all_results[task_dir.name] = results
        issue_count = sum(1 for result in results if not result.is_ok)
        if issue_count:
            print(f"\n{task_dir.name}: {issue_count} issue(s) / {len(results)} episode(s)")

    report = build_report(args.dataset_root, task_dirs, all_results)
    total_issues = sum(
        1
        for results in all_results.values()
        for result in results
        if not result.is_ok
    )
    print("\n" + "\n".join(report.splitlines()[:6]))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"\nDetailed report saved to {args.output}")

    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
