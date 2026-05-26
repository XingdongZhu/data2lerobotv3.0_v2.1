#!/usr/bin/env python3
"""
方舟无限 ARX (acone) 数据 -> LeRobot v3.0

参考 leju_align2lerobot_v30_no_norm.py / qinglongros2_align2lerobot_v30_no_norm.py 结构。

ARX 特化:
  - HDF5 无 joints/action; action 取下一帧 state, 末帧丢弃
  - action.position 末 2 维 > -0.5 -> 0 (沿用原 arx_hdf5_loong 行为)
  - end-effector 朝向是 quaternion(x,y,z,w), 转 Euler('xyz', rad)
  - 双臂 joint 重排为: left_arm(6)|left_grip(1)|right_arm(6)|right_grip(1) = 14d
  - 4 state stream: position / velocity / effort / end
  - 3 路相机: hand_left / hand_right / head, 原生 640x480 RGB

用法:
    python3 arx_align2lerobot_v30_no_norm.py \
        --input  /workspace/align/<task_id> \
        --output /workspace/ceshi_v30/lerobotv30_arx/<task_id> \
        --repo_id arx_loong/<task_id> \
        --task "..." \
        --fps 30 --workers 8 [--vcodec libsvtav1] [--crf 30]
"""
import argparse
import json
import logging
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from datasets import disable_progress_bar
disable_progress_bar()

import cv2
import h5py
import numpy as np
from scipy.spatial.transform import Rotation
from tqdm import tqdm

try:
    import av
    USE_PYAV = True
except ImportError:
    USE_PYAV = False
    print("Warning: PyAV not found, falling back to FFmpeg subprocess")

try:
    from lerobot.common.datasets.lerobot_dataset import LeRobotDataset
    from lerobot.common.datasets.compute_stats import compute_episode_stats
    from lerobot.common.datasets.dataset_tools import merge_datasets
except ImportError:
    from lerobot.datasets.lerobot_dataset import LeRobotDataset
    from lerobot.datasets.compute_stats import compute_episode_stats
    from lerobot.datasets.dataset_tools import merge_datasets

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# 配置
# =============================================================================

ARX_CONFIG = {
    "robot_type": "arx_loong",
    "cameras": ["hand_left", "hand_right", "head"],
}

STATE_DIM = 14
ACTION_DIM = 14
END_DIM = 14

JOINT_AXES_NAMES = [
    "arm_master_l_joint1", "arm_master_l_joint2", "arm_master_l_joint3",
    "arm_master_l_joint4", "arm_master_l_joint5", "arm_master_l_joint6",
    "arm_master_l_joint7",
    "arm_master_r_joint1", "arm_master_r_joint2", "arm_master_r_joint3",
    "arm_master_r_joint4", "arm_master_r_joint5", "arm_master_r_joint6",
    "arm_master_r_joint7",
]

END_EFFECTOR_AXES_NAMES = [
    "left_pos_x", "left_pos_y", "left_pos_z",
    "left_roll", "left_pitch", "left_yaw",
    "left_gripper",
    "right_pos_x", "right_pos_y", "right_pos_z",
    "right_roll", "right_pitch", "right_yaw",
    "right_gripper",
]


# =============================================================================
# 视频编码
# =============================================================================

def encode_video_pyav(images, video_path, fps, vcodec="libsvtav1",
                       pix_fmt="yuv420p", g=2, crf=30, preset=12):
    if not images:
        raise ValueError("No images")
    height, width = images[0].shape[:2]
    if vcodec in ("libsvtav1", "hevc") and pix_fmt == "yuv444p":
        pix_fmt = "yuv420p"
    options = {"g": str(g), "crf": str(crf)}
    if vcodec == "libsvtav1":
        options["preset"] = str(preset)
    video_path.parent.mkdir(parents=True, exist_ok=True)
    with av.open(str(video_path), "w") as out:
        stream = out.add_stream(vcodec, fps, options=options)
        stream.pix_fmt = pix_fmt
        stream.width = width
        stream.height = height
        for arr in images:
            if arr.shape[2] == 4:
                arr = arr[:, :, :3]
            frame = av.VideoFrame.from_ndarray(arr, format="rgb24")
            for pkt in stream.encode(frame):
                out.mux(pkt)
        for pkt in stream.encode():
            out.mux(pkt)


def encode_video_ffmpeg(images, video_path, fps, vcodec="libsvtav1",
                         pix_fmt="yuv420p", gop=2, crf=30):
    if not images:
        raise ValueError("No images")
    height, width = images[0].shape[:2]
    video_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24",
        "-s", f"{width}x{height}", "-r", str(fps), "-i", "-", "-an",
        "-c:v", vcodec, "-pix_fmt", pix_fmt, "-g", str(gop), "-crf", str(crf),
    ]
    cmd += ["-preset", "8"] if vcodec == "libsvtav1" else ["-preset", "fast"]
    cmd.append(str(video_path))
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=10**8)
    try:
        for img in images:
            if img.ndim == 3 and img.shape[2] == 4:
                img = img[:, :, :3]
            p.stdin.write(img.astype(np.uint8).tobytes())
        p.stdin.close()
    except Exception as e:
        p.kill()
        raise RuntimeError(f"FFmpeg encoding failed: {e}")
    p.wait()
    if p.returncode != 0:
        err = p.stderr.read().decode() if p.stderr else ""
        raise RuntimeError(f"FFmpeg exited {p.returncode}: {err}")


def encode_video(images, video_path, fps, vcodec="libsvtav1", crf=30):
    if USE_PYAV:
        encode_video_pyav(images, video_path, fps, vcodec=vcodec, crf=crf)
    else:
        encode_video_ffmpeg(images, video_path, fps, vcodec=vcodec, crf=crf)


# =============================================================================
# 数据读取
# =============================================================================

def _quat_xyzw_to_euler(quat_xyzw: np.ndarray) -> np.ndarray:
    return Rotation.from_quat(quat_xyzw).as_euler("xyz", degrees=False).astype(np.float32)


def _reorder_joint(arm: np.ndarray, eff: np.ndarray) -> np.ndarray:
    return np.concatenate([
        arm[:, :6], eff[:, :1],
        arm[:, 6:], eff[:, 1:],
    ], axis=1).astype(np.float32)


def _build_end_pose(end_pos: np.ndarray, end_orient: np.ndarray,
                     eff_pos: np.ndarray) -> np.ndarray:
    left_euler  = _quat_xyzw_to_euler(end_orient[:, 0, :])
    right_euler = _quat_xyzw_to_euler(end_orient[:, 1, :])
    return np.concatenate([
        end_pos[:, 0, :], left_euler, eff_pos[:, 0:1],
        end_pos[:, 1, :], right_euler, eff_pos[:, 1:2],
    ], axis=1).astype(np.float32)


def load_arx_h5(h5_path: Path) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    with h5py.File(h5_path, "r") as f:
        arm_pos    = f["joints/state/arm/position"][:]
        arm_vel    = f["joints/state/arm/velocity"][:]
        arm_eff    = f["joints/state/arm/effort"][:]
        eff_pos    = f["joints/state/effector/position"][:]
        eff_vel    = f["joints/state/effector/velocity"][:]
        eff_effort = f["joints/state/effector/effort"][:]
        end_pos    = f["joints/state/end/position"][:]
        end_orient = f["joints/state/end/orientation"][:]
        n = arm_pos.shape[0]
        if n < 2:
            raise ValueError(f"only {n} frames in {h5_path}")

        state_position = _reorder_joint(arm_pos, eff_pos)
        state_velocity = _reorder_joint(arm_vel, eff_vel)
        state_effort   = _reorder_joint(arm_eff, eff_effort)
        state_end      = _build_end_pose(end_pos, end_orient, eff_pos)

        n_eff = n - 1
        action_position = state_position[1:].copy()
        action_velocity = state_velocity[1:].copy()
        action_effort   = state_effort[1:].copy()
        action_end      = state_end[1:].copy()

        last2 = action_position[:, -2:]
        last2[last2 > -0.5] = 0.0
        action_position[:, -2:] = last2

        data["frames"] = n_eff
        data["state"]            = state_position[:n_eff]
        data["velocity"]         = state_velocity[:n_eff]
        data["effort"]           = state_effort[:n_eff]
        data["end"]              = state_end[:n_eff]
        data["action"]           = action_position
        data["action_velocity"]  = action_velocity
        data["action_effort"]    = action_effort
        data["action_end"]       = action_end

        images: Dict[str, List[np.ndarray]] = {}
        for cam in ARX_CONFIG["cameras"]:
            key = f"cameras/{cam}/color/data"
            if key not in f:
                logger.warning(f"camera {cam} not found in {h5_path.name}, skip")
                continue
            jpgs = f[key][:n_eff]
            buf = []
            for b in jpgs:
                arr = np.frombuffer(b, np.uint8)
                bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                if bgr is None:
                    continue
                buf.append(cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB))
            if buf:
                images[cam] = buf
        data["images"] = images

        if "metadata.json" in f:
            raw = f["metadata.json"][()]
            try:
                meta = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            except Exception:
                meta = {}
        else:
            meta = {}
        data["task"] = meta.get("task_name", "manipulation_task")
    return data


# =============================================================================
# Episode 写入
# =============================================================================

def _build_features(fps: int, image_shapes: Dict[str, tuple], vcodec: str) -> Dict[str, dict]:
    common = {"axes": JOINT_AXES_NAMES}
    end = {"axes": END_EFFECTOR_AXES_NAMES}
    codec_name = "av1" if "av1" in vcodec else ("h264" if "264" in vcodec else vcodec)
    features = {
        "observation.state":    {"dtype": "float32", "shape": (STATE_DIM,),  "names": common, "fps": fps},
        "observation.velocity": {"dtype": "float32", "shape": (STATE_DIM,),  "names": common, "fps": fps},
        "observation.effort":   {"dtype": "float32", "shape": (STATE_DIM,),  "names": common, "fps": fps},
        "observation.end":      {"dtype": "float32", "shape": (END_DIM,),    "names": end,    "fps": fps},
        "action":               {"dtype": "float32", "shape": (ACTION_DIM,), "names": common, "fps": fps},
        "action.velocity":      {"dtype": "float32", "shape": (ACTION_DIM,), "names": common, "fps": fps},
        "action.effort":        {"dtype": "float32", "shape": (ACTION_DIM,), "names": common, "fps": fps},
        "action.end":           {"dtype": "float32", "shape": (END_DIM,),    "names": end,    "fps": fps},
    }
    for cam, shape in image_shapes.items():
        h, w, c = shape
        features[f"observation.images.{cam}"] = {
            "dtype": "video",
            "shape": (h, w, c),
            "names": ["height", "width", "channels"],
            "info": {
                "video.height": h, "video.width": w, "video.channels": c,
                "video.codec": codec_name, "video.pix_fmt": "yuv420p",
                "video.is_depth_map": False, "video.fps": fps, "has_audio": False,
            },
        }
    return features


def convert_episode(episode_data, output_dir: Path, repo_id: str,
                    episode_index: int, fps: int, vcodec: str, crf: int,
                    task_override: str | None) -> dict:
    result = {"episode_index": episode_index, "success": False,
              "frames": 0, "error": None, "dataset_path": None}
    try:
        num_frames = episode_data["frames"]
        task = task_override or episode_data.get("task", "manipulation_task")
        image_shapes = {c: imgs[0].shape for c, imgs in episode_data["images"].items() if imgs}
        if not image_shapes:
            raise ValueError("no usable image stream")
        features = _build_features(fps, image_shapes, vcodec)

        temp_base = Path(tempfile.mkdtemp())
        video_paths = {}
        try:
            for cam, imgs in episode_data["images"].items():
                if not imgs:
                    continue
                td = Path(tempfile.mkdtemp(dir=temp_base))
                vp = td / f"{cam}.mp4"
                encode_video(imgs, vp, fps, vcodec=vcodec, crf=crf)
                video_paths[cam] = vp
            episode_data["images"] = None

            episode_dir = output_dir / f"episode_{episode_index:04d}"
            if episode_dir.exists():
                shutil.rmtree(episode_dir)
            safe_root = repo_id.replace("/", "_")
            dataset = LeRobotDataset.create(
                repo_id=f"{safe_root}/episode_{episode_index:04d}",
                root=episode_dir,
                robot_type=ARX_CONFIG["robot_type"],
                fps=fps,
                features=features,
                use_videos=True,
                image_writer_threads=0,
            )

            placeholders = {c: np.zeros(s, dtype=np.uint8) for c, s in image_shapes.items()}
            for i in range(num_frames):
                frame = {
                    "observation.state":    episode_data["state"][i],
                    "observation.velocity": episode_data["velocity"][i],
                    "observation.effort":   episode_data["effort"][i],
                    "observation.end":      episode_data["end"][i],
                    "action":               episode_data["action"][i],
                    "action.velocity":      episode_data["action_velocity"][i],
                    "action.effort":        episode_data["action_effort"][i],
                    "action.end":           episode_data["action_end"][i],
                    "task": task,
                }
                for c, ph in placeholders.items():
                    frame[f"observation.images.{c}"] = ph
                dataset.add_frame(frame)

            dataset._wait_image_writer()

            episode_buffer = dataset.episode_buffer
            episode_length = episode_buffer.pop("size")
            tasks_list = episode_buffer.pop("task")
            episode_tasks = list(set(tasks_list))
            episode_buffer["index"] = np.arange(0, episode_length)
            episode_buffer["episode_index"] = np.zeros((episode_length,), dtype=np.int32)
            dataset.meta.save_episode_tasks(episode_tasks)
            episode_buffer["task_index"] = np.array([
                dataset.meta.get_task_index(t) for t in tasks_list
            ])
            for key, ft in dataset.features.items():
                if key in ("index", "episode_index", "task_index"):
                    continue
                if ft["dtype"] in ("image", "video"):
                    continue
                if key in episode_buffer:
                    episode_buffer[key] = np.stack(episode_buffer[key])

            non_video_features = {k: v for k, v in dataset.features.items()
                                  if v["dtype"] not in ("image", "video")}
            non_video_buffer = {k: v for k, v in episode_buffer.items()
                                if k not in dataset.meta.video_keys}
            ep_stats = compute_episode_stats(non_video_buffer, non_video_features)

            for video_key in dataset.meta.video_keys:
                img_dir = dataset._get_image_file_dir(0, video_key)
                if img_dir.exists():
                    shutil.rmtree(img_dir)

            episode_metadata = {}
            for cam, vp in video_paths.items():
                key = f"observation.images.{cam}"
                episode_metadata.update(
                    dataset._save_episode_video(video_key=key, episode_index=0, temp_path=vp)
                )

            for video_key in list(episode_buffer.keys()):
                if video_key in dataset.meta.video_keys:
                    del episode_buffer[video_key]

            episode_metadata.update(dataset._save_episode_data(episode_buffer))
            dataset.meta.save_episode(0, episode_length, episode_tasks, ep_stats, episode_metadata)
            for video_key in dataset.meta.video_keys:
                dataset.meta.update_video_info(video_key)
            dataset.clear_episode_buffer(delete_images=False)
            dataset.finalize()

            result["success"] = True
            result["frames"] = num_frames
            result["dataset_path"] = str(episode_dir)
        finally:
            shutil.rmtree(temp_base, ignore_errors=True)
    except Exception as e:
        result["error"] = f"{e}\n{traceback.format_exc()}"
        logger.error(f"Episode {episode_index} failed: {e}")
    return result


def convert_episode_wrapper(args: tuple) -> dict:
    h5_path, output_dir, repo_id, episode_index, fps, vcodec, crf, task_override = args
    try:
        data = load_arx_h5(h5_path)
        return convert_episode(data, output_dir, repo_id, episode_index,
                               fps, vcodec, crf, task_override)
    except Exception as e:
        return {"episode_index": episode_index, "success": False, "frames": 0,
                "error": f"{e}\n{traceback.format_exc()}", "dataset_path": None}


# =============================================================================
# 入口
# =============================================================================

def find_episodes(data_dir: Path) -> List[Path]:
    episodes = []
    for sub in sorted(data_dir.iterdir()):
        if sub.is_dir():
            h5s = list(sub.glob("*.h5"))
            if h5s:
                episodes.append(h5s[0])
    for h5_file in sorted(data_dir.glob("*.h5")):
        if h5_file not in episodes:
            episodes.append(h5_file)
    return episodes


def main():
    parser = argparse.ArgumentParser(description="ARX (acone) -> LeRobot v3.0")
    parser.add_argument("--input",  type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repo_id", type=str, default=None)
    parser.add_argument("--task", type=str, nargs="+", default=["manipulation_task"])
    parser.add_argument("--fps",     type=int, default=30)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--vcodec",  type=str, default="libsvtav1")
    parser.add_argument("--crf",     type=int, default=30)
    args = parser.parse_args()

    if isinstance(args.task, list):
        args.task = " ".join(args.task)
    if args.repo_id is None:
        args.repo_id = args.output.name

    episodes = find_episodes(args.input)
    if not episodes:
        print(f"No .h5 in {args.input}")
        sys.exit(1)
    print(f"Found {len(episodes)} episodes | workers={args.workers} | vcodec={args.vcodec} | PyAV={USE_PYAV}")

    output_root = args.output
    if output_root.exists():
        shutil.rmtree(output_root)
    separate_dir = output_root.parent / f"{output_root.name}_separate_episodes"
    if separate_dir.exists():
        shutil.rmtree(separate_dir)
    separate_dir.mkdir(parents=True, exist_ok=True)

    tasks = [
        (p, separate_dir, args.repo_id, i, args.fps, args.vcodec, args.crf, args.task)
        for i, p in enumerate(episodes)
    ]

    success, failed = [], []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(convert_episode_wrapper, t): t for t in tasks}
        with tqdm(total=len(episodes), desc="Converting") as pbar:
            for f in as_completed(futs):
                r = f.result()
                if r["success"]:
                    success.append(Path(r["dataset_path"]))
                    tqdm.write(f"✓ ep {r['episode_index']}: {r['frames']} frames")
                else:
                    failed.append(f"ep {r['episode_index']}: {r['error']}")
                    tqdm.write(f"✗ ep {r['episode_index']}")
                pbar.update(1)

    dt = time.time() - t0
    print(f"\nDone in {dt:.0f}s | success {len(success)}/{len(episodes)} | failed {len(failed)}")
    if failed:
        print("First few failures:")
        for s in failed[:3]:
            print(f"  - {s[:300]}")
    if not success:
        sys.exit(1)

    print("\nMerging...")
    datasets = []
    for p in sorted(success):
        try:
            datasets.append(LeRobotDataset(root=p, repo_id=p.name))
        except Exception as e:
            print(f"skip {p}: {e}")
    if not datasets:
        print("no dataset to merge")
        sys.exit(1)
    merged = merge_datasets(
        datasets=datasets,
        output_repo_id=args.repo_id,
        output_dir=output_root,
    )
    print(f"Merged -> {output_root}")
    print(f"Episodes: {merged.meta.total_episodes}, Frames: {merged.meta.total_frames}")

    if separate_dir.exists():
        shutil.rmtree(separate_dir)


if __name__ == "__main__":
    main()
