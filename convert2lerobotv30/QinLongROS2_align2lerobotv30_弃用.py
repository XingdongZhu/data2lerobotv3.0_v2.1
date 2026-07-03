#!/usr/bin/env python3
"""
青龙机器人 (QinLongROS2) 数据转换脚本 - 转换为 LeRobot v3.0 格式

数据结构 (ROS2, 基于 H5 metadata ver 2.1.0):
    State/Action (33维, 不做归一化):
        - arm:      14维  (left_arm 7 + right_arm 7)
        - effector:  2维  (left_gripper + right_gripper)
        - head:      2维  (yaw + pitch)
        - waist:     3维  (pitch + roll + yaw)
        - leg:      12维  (left 6 + right 6)

    相机 (RGB only, depth/tactile 暂不导出):
        - head:       640x480 RGB (jpg)
        - hand_left:  640x480 RGB (jpg)
        - hand_right: 640x480 RGB (jpg)

    跳过项:
        - cameras/head/depth (PNG 16bit 深度)
        - tactile/* (触觉传感器)
        - joints/state/*/effort, velocity (仅使用 position)

Usage:
    python qinglongros2_align2lerobot_v30_no_norm.py \\
        --input ./raw_data \\
        --output ./lerobot_dataset \\
        --task "Pick up the book" \\
        --fps 30 --workers 4
"""

import argparse
import json
import logging
import shutil
import sys
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import cv2
import h5py
import numpy as np
from tqdm import tqdm

# Video encoding
try:
    import av
    av.logging.set_level(av.logging.ERROR)
    USE_PYAV = True
except ImportError:
    USE_PYAV = False
    import subprocess
    print("Warning: PyAV not found, falling back to FFmpeg subprocess")

# LeRobot imports
try:
    from lerobot.common.datasets.lerobot_dataset import LeRobotDataset
    from lerobot.common.datasets.compute_stats import compute_episode_stats
except ImportError:
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
        from lerobot.datasets.compute_stats import compute_episode_stats
    except ImportError:
        print("Error: 'lerobot' package not found. Please install it first.")
        sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# 配置常量
# =============================================================================

QINGLONG_CONFIG = {
    "robot_type": "QinLongROS2",
    "arm_dim": 14,
    "effector_dim": 2,
    "head_dim": 2,
    "waist_dim": 3,
    "leg_dim": 12,
    "cameras": ["head", "hand_left", "hand_right"],
    "target_image_size": (640, 480),  # (width, height)
    "motor_names": [
        # arm (14)
        "J_arm_l_01", "J_arm_l_02", "J_arm_l_03", "J_arm_l_04", "J_arm_l_05", "J_arm_l_06", "J_arm_l_07",
        "J_arm_r_01", "J_arm_r_02", "J_arm_r_03", "J_arm_r_04", "J_arm_r_05", "J_arm_r_06", "J_arm_r_07",
        # effector (2)
        "left_gripper", "right_gripper",
        # head (2)
        "J_head_yaw", "J_head_pitch",
        # waist (3)
        "J_waist_pitch", "J_waist_roll", "J_waist_yaw",
        # leg (12)
        "J_hip_l_roll", "J_hip_l_yaw", "J_hip_l_pitch", "J_knee_l_pitch", "J_ankle_l_pitch", "J_ankle_l_roll",
        "J_hip_r_roll", "J_hip_r_yaw", "J_hip_r_pitch", "J_knee_r_pitch", "J_ankle_r_pitch", "J_ankle_r_roll",
    ],
}

STATE_DIM = (
    QINGLONG_CONFIG["arm_dim"]
    + QINGLONG_CONFIG["effector_dim"]
    + QINGLONG_CONFIG["head_dim"]
    + QINGLONG_CONFIG["waist_dim"]
    + QINGLONG_CONFIG["leg_dim"]
)  # 33
ACTION_DIM = STATE_DIM
assert len(QINGLONG_CONFIG["motor_names"]) == STATE_DIM, (
    f"motor_names ({len(QINGLONG_CONFIG['motor_names'])}) != STATE_DIM ({STATE_DIM})"
)


# =============================================================================
# 视频编码工具
# =============================================================================

def encode_video_pyav(
    images: List[np.ndarray],
    video_path: Path,
    fps: int,
    vcodec: str = "libsvtav1",
    pix_fmt: str = "yuv420p",
    g: int = 2,
    crf: int = 30,
    preset: int = 12,
) -> None:
    if len(images) == 0:
        raise ValueError("No images provided for video encoding")

    height, width = images[0].shape[:2]

    if vcodec in ("libsvtav1", "hevc") and pix_fmt == "yuv444p":
        pix_fmt = "yuv420p"

    video_options = {"g": str(g), "crf": str(crf)}
    if vcodec == "libsvtav1":
        video_options["preset"] = str(preset)

    video_path.parent.mkdir(parents=True, exist_ok=True)

    with av.open(str(video_path), "w") as output:
        stream = output.add_stream(vcodec, fps, options=video_options)
        stream.pix_fmt = pix_fmt
        stream.width = width
        stream.height = height

        for img_array in images:
            if img_array.shape[2] == 4:
                img_array = img_array[:, :, :3]
            frame = av.VideoFrame.from_ndarray(img_array, format='rgb24')
            for packet in stream.encode(frame):
                output.mux(packet)

        for packet in stream.encode():
            output.mux(packet)


def encode_video_ffmpeg(
    images: List[np.ndarray],
    video_path: Path,
    fps: int,
    vcodec: str = "libsvtav1",
    pix_fmt: str = "yuv420p",
    gop: int = 2,
    crf: int = 30,
) -> None:
    if len(images) == 0:
        raise ValueError("No images provided")

    height, width = images[0].shape[:2]
    video_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg", "-y",
        "-loglevel", "error",
        "-f", "rawvideo",
        "-pix_fmt", "rgb24",
        "-s", f"{width}x{height}",
        "-r", str(fps),
        "-i", "-",
        "-an",
        "-c:v", vcodec,
        "-pix_fmt", pix_fmt,
        "-g", str(gop),
        "-crf", str(crf),
    ]

    if vcodec == "libsvtav1":
        cmd.extend(["-preset", "8"])
    else:
        cmd.extend(["-preset", "fast"])

    cmd.append(str(video_path))

    process = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, bufsize=10**8
    )

    try:
        for img in images:
            if len(img.shape) == 3 and img.shape[2] == 4:
                img = img[:, :, :3]
            process.stdin.write(img.astype(np.uint8).tobytes())
        process.stdin.close()
    except Exception as e:
        process.kill()
        raise RuntimeError(f"FFmpeg encoding failed: {e}")

    process.wait()
    if process.returncode != 0:
        stderr = process.stderr.read().decode() if process.stderr else ""
        raise RuntimeError(f"FFmpeg exited with code {process.returncode}: {stderr}")


def encode_video(images: List[np.ndarray], video_path: Path, fps: int,
                 vcodec: str = "libsvtav1", crf: int = 30) -> None:
    if USE_PYAV:
        encode_video_pyav(images, video_path, fps, vcodec=vcodec, crf=crf)
    else:
        encode_video_ffmpeg(images, video_path, fps, vcodec=vcodec, crf=crf)


# =============================================================================
# 数据读取工具
# =============================================================================

def _read_position(f: h5py.File, group_path: str, expected_dim: int, num_frames: int) -> np.ndarray:
    """读取 joints 下某个子组的 position；缺失时以 0 填充。"""
    key = f"{group_path}/position"
    if key in f:
        arr = f[key][:].astype(np.float32)
        if arr.shape[0] != num_frames:
            raise ValueError(f"{key} frame count {arr.shape[0]} != {num_frames}")
        if arr.shape[1] != expected_dim:
            raise ValueError(f"{key} dim {arr.shape[1]} != {expected_dim}")
        return arr
    logger.warning(f"{key} not found, filling zeros ({num_frames}, {expected_dim})")
    return np.zeros((num_frames, expected_dim), dtype=np.float32)


def load_qinglong_h5(h5_path: Path) -> Dict[str, Any]:
    """
    从青龙 ROS2 H5 文件加载数据。

    State/Action 拼接顺序: arm | effector | head | waist | leg  (合计 33)
    """
    data: Dict[str, Any] = {}

    with h5py.File(h5_path, 'r') as f:
        # metadata
        try:
            metadata = json.loads(f['metadata.json'][()])
        except Exception:
            metadata = {}
        # 元数据里没有 task_name 字段，留给外部 --task 覆盖
        data['task'] = metadata.get('task_name', 'manipulation_task')

        # 时间戳
        timestamps = f['timestamp'][:]
        num_frames = len(timestamps)
        data['frames'] = num_frames
        data['timestamps'] = timestamps.astype(np.float64)

        # ---------- State (position) ----------
        s_arm      = _read_position(f, 'joints/state/arm',      QINGLONG_CONFIG['arm_dim'],      num_frames)
        s_effector = _read_position(f, 'joints/state/effector', QINGLONG_CONFIG['effector_dim'], num_frames)
        s_effector = np.clip(s_effector, 0, 90)
        s_head     = _read_position(f, 'joints/state/head',     QINGLONG_CONFIG['head_dim'],     num_frames)
        s_waist    = _read_position(f, 'joints/state/waist',    QINGLONG_CONFIG['waist_dim'],    num_frames)
        s_leg      = _read_position(f, 'joints/state/leg',      QINGLONG_CONFIG['leg_dim'],      num_frames)
        data['state'] = np.concatenate(
            [s_arm, s_effector, s_head, s_waist, s_leg], axis=1
        ).astype(np.float32)

        # ---------- Action ----------
        a_arm      = _read_position(f, 'joints/action/arm',      QINGLONG_CONFIG['arm_dim'],      num_frames)
        a_effector = _read_position(f, 'joints/action/effector', QINGLONG_CONFIG['effector_dim'], num_frames)
        a_effector = np.clip(a_effector, 0, 90)
        a_head     = _read_position(f, 'joints/action/head',     QINGLONG_CONFIG['head_dim'],     num_frames)
        a_waist    = _read_position(f, 'joints/action/waist',    QINGLONG_CONFIG['waist_dim'],    num_frames)
        a_leg      = _read_position(f, 'joints/action/leg',      QINGLONG_CONFIG['leg_dim'],      num_frames)
        data['action'] = np.concatenate(
            [a_arm, a_effector, a_head, a_waist, a_leg], axis=1
        ).astype(np.float32)

        # ---------- 图像 (RGB only) ----------
        images: Dict[str, List[np.ndarray]] = {}
        image_shapes: Dict[str, tuple] = {}
        target_width, target_height = QINGLONG_CONFIG['target_image_size']

        for cam_id in QINGLONG_CONFIG['cameras']:
            cam_key = f'cameras/{cam_id}/color/data'
            if cam_key not in f:
                logger.warning(f"camera {cam_id} not found at {cam_key}, skip")
                continue
            img_bytes_array = f[cam_key][:]
            img_list = []
            for img_bytes in img_bytes_array:
                try:
                    np_arr = np.frombuffer(img_bytes, np.uint8)
                    img_bgr = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    if img_bgr is None:
                        continue
                    img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
                    img_resized = cv2.resize(
                        img_rgb, (target_width, target_height),
                        interpolation=cv2.INTER_LINEAR,
                    )
                    img_list.append(img_resized)
                    if cam_id not in image_shapes:
                        image_shapes[cam_id] = img_resized.shape
                except Exception as e:
                    logger.warning(f"Failed to decode image for {cam_id}: {e}")

            if img_list:
                images[cam_id] = img_list

        data['images'] = images
        data['image_shapes'] = image_shapes

    return data


# =============================================================================
# Episode 转换
# =============================================================================

def convert_episode(
    episode_data: Dict[str, Any],
    output_dir: Path,
    repo_id: str,
    episode_index: int,
    fps: int,
    vcodec: str = "libsvtav1",
    crf: int = 30,
    task_override: str = None,
) -> dict:
    """将单个 episode 数据转换为 LeRobot 数据集格式"""
    result = {
        'episode_index': episode_index,
        'success': False,
        'frames': 0,
        'error': None,
        'dataset_path': None,
    }

    try:
        num_frames = episode_data['frames']
        task = task_override if task_override else episode_data.get('task', 'manipulation_task')

        features = {
            "observation.state": {
                "dtype": "float32",
                "shape": (STATE_DIM,),
                "names": {"motors": QINGLONG_CONFIG["motor_names"]},
                "fps": fps,
            },
            "action": {
                "dtype": "float32",
                "shape": (ACTION_DIM,),
                "names": {"motors": QINGLONG_CONFIG["motor_names"]},
                "fps": fps,
            },
        }

        for cam_id, shape in episode_data['image_shapes'].items():
            h, w, c = shape
            features[f"observation.images.{cam_id}"] = {
                "dtype": "video",
                "shape": (h, w, c),
                "names": ["height", "width", "channels"],
                "info": {
                    "video.height": h,
                    "video.width": w,
                    "video.codec": "av1",
                    "video.pix_fmt": "yuv420p",
                    "video.is_depth_map": False,
                    "video.fps": fps,
                    "video.channels": c,
                    "has_audio": False,
                },
            }

        # 临时目录用于视频编码
        temp_base_dir = Path(tempfile.mkdtemp())
        video_paths = {}

        for cam_id, img_list in episode_data['images'].items():
            if img_list:
                temp_video_dir = Path(tempfile.mkdtemp(dir=temp_base_dir))
                video_path = temp_video_dir / f"{cam_id}.mp4"
                encode_video(img_list, video_path, fps, vcodec=vcodec, crf=crf)
                video_paths[cam_id] = video_path
                logger.debug(f"Encoded {len(img_list)} frames for camera {cam_id}")

        # 释放图像内存
        del episode_data['images']

        # 创建数据集
        episode_dir = output_dir / f"episode_{episode_index:04d}"
        if episode_dir.exists():
            shutil.rmtree(episode_dir)

        dataset = LeRobotDataset.create(
            repo_id=f"{repo_id}/episode_{episode_index:04d}",
            root=episode_dir,
            robot_type=QINGLONG_CONFIG["robot_type"],
            fps=fps,
            features=features,
            use_videos=True,
            image_writer_threads=0,
        )

        logger.info(f"Adding {num_frames} frames...")
        for i in range(num_frames):
            frame_dict = {
                "observation.state": episode_data['state'][i],
                "action": episode_data['action'][i],
                "task": task,
            }
            for cam_id, shape in episode_data['image_shapes'].items():
                h, w, c = shape
                frame_dict[f"observation.images.{cam_id}"] = np.zeros((h, w, c), dtype=np.uint8)
            dataset.add_frame(frame_dict)

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
            if key in ["index", "episode_index", "task_index"]:
                continue
            if ft["dtype"] in ["image", "video"]:
                continue
            if key in episode_buffer:
                episode_buffer[key] = np.stack(episode_buffer[key])

        non_video_features = {
            k: v for k, v in dataset.features.items()
            if v["dtype"] not in ["image", "video"]
        }
        non_video_buffer = {
            k: v for k, v in episode_buffer.items()
            if k not in dataset.meta.video_keys
        }
        ep_stats = compute_episode_stats(non_video_buffer, non_video_features)

        episode_metadata = {}
        for cam_id, temp_video_path in video_paths.items():
            video_key = f"observation.images.{cam_id}"
            video_metadata = dataset._save_episode_video(
                video_key=video_key,
                episode_index=0,
                temp_path=temp_video_path,
            )
            episode_metadata.update(video_metadata)

        for video_key in list(episode_buffer.keys()):
            if video_key in dataset.meta.video_keys:
                del episode_buffer[video_key]

        ep_data_metadata = dataset._save_episode_data(episode_buffer)
        episode_metadata.update(ep_data_metadata)

        dataset.meta.save_episode(0, episode_length, episode_tasks, ep_stats, episode_metadata)

        for video_key in dataset.meta.video_keys:
            dataset.meta.update_video_info(video_key)

        dataset.clear_episode_buffer(delete_images=False)
        dataset.finalize()

        shutil.rmtree(temp_base_dir, ignore_errors=True)

        result['success'] = True
        result['frames'] = num_frames
        result['dataset_path'] = str(episode_dir)

    except Exception as e:
        result['error'] = f"{e}\n{traceback.format_exc()}"
        logger.error(f"Failed processing episode {episode_index}: {e}")

    return result


def convert_episode_wrapper(args: tuple) -> dict:
    h5_path, output_dir, repo_id, episode_index, fps, vcodec, crf, task_override = args
    try:
        episode_data = load_qinglong_h5(h5_path)
        return convert_episode(
            episode_data=episode_data,
            output_dir=output_dir,
            repo_id=repo_id,
            episode_index=episode_index,
            fps=fps,
            vcodec=vcodec,
            crf=crf,
            task_override=task_override,
        )
    except Exception as e:
        return {
            'episode_index': episode_index,
            'success': False,
            'frames': 0,
            'error': f"{e}\n{traceback.format_exc()}",
            'dataset_path': None,
        }


# =============================================================================
# 主入口
# =============================================================================

def find_episodes(data_path: Path) -> List[Path]:
    """查找所有可转换的 episode H5 文件。

    支持:
        1) data_path 直接是 .h5 文件
        2) data_path 是目录，递归收集子目录中的 .h5
        3) data_path 是目录，目录下直接放 .h5
    """
    if data_path.is_file() and data_path.suffix == '.h5':
        return [data_path]

    episodes: List[Path] = []
    if not data_path.is_dir():
        return episodes

    for subdir in sorted(data_path.iterdir()):
        if subdir.is_dir():
            h5_files = sorted(subdir.glob("*.h5"))
            if h5_files:
                episodes.append(h5_files[0])

    for h5_file in sorted(data_path.glob("*.h5")):
        if h5_file not in episodes:
            episodes.append(h5_file)

    return episodes


def main():
    parser = argparse.ArgumentParser(
        description="青龙机器人 (QinLongROS2) 数据转换为 LeRobot v3.0 格式"
    )
    parser.add_argument("--input", type=Path, required=True,
                        help="数据目录或单个 .h5 文件")
    parser.add_argument("--output", type=Path, required=True,
                        help="输出目录")
    parser.add_argument("--repo_id", type=str, default=None,
                        help="HuggingFace 仓库 ID (默认使用输出目录名)")
    parser.add_argument("--task", type=str, nargs='+', default=["manipulation_task"],
                        help="任务描述 (可不加引号，多词自动拼接)")
    parser.add_argument("--fps", type=int, default=30,
                        help="数据集帧率 (默认: 30)")
    parser.add_argument("--workers", type=int, default=8,
                        help="并行进程数 (默认: 8)")
    parser.add_argument("--vcodec", type=str, default="libsvtav1",
                        help="视频编码器 (默认: libsvtav1)")
    parser.add_argument("--crf", type=int, default=30,
                        help="视频质量 CRF (默认: 30)")

    args = parser.parse_args()

    if isinstance(args.task, list):
        args.task = ' '.join(args.task)

    if args.repo_id is None:
        args.repo_id = args.output.name

    episodes = find_episodes(args.input)

    if not episodes:
        print(f"No .h5 files found in {args.input}")
        sys.exit(1)

    print(f"Found {len(episodes)} episodes:")
    for path in episodes:
        print(f"  - {path.name}")

    print(f"\nUsing {args.workers} workers, PyAV: {USE_PYAV}")
    print(f"Robot: {QINGLONG_CONFIG['robot_type']}, State/Action dim: {STATE_DIM}")

    output_root = args.output
    if output_root.exists():
        shutil.rmtree(output_root)
    separate_dir = output_root.parent / f"{output_root.name}_separate_episodes"
    separate_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    for i, path in enumerate(episodes):
        tasks.append((
            path, separate_dir, args.repo_id, i,
            args.fps, args.vcodec, args.crf, args.task
        ))

    success_datasets = []
    failed_episodes = []

    start_time = time.time()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(convert_episode_wrapper, t): t for t in tasks}

        with tqdm(total=len(episodes), desc="Converting Episodes") as pbar:
            for future in as_completed(futures):
                res = future.result()
                if res['success']:
                    success_datasets.append(Path(res['dataset_path']))
                    tqdm.write(f"✓ Episode {res['episode_index']}: {res['frames']} frames")
                else:
                    failed_episodes.append(f"Episode {res['episode_index']}: {res['error']}")
                    tqdm.write(f"✗ Episode {res['episode_index']}: FAILED")
                pbar.update(1)

    elapsed = time.time() - start_time

    print(f"\n{'='*70}")
    print(f"Conversion completed in {elapsed:.1f}s")
    print(f"Success: {len(success_datasets)} / {len(episodes)}")
    print(f"Failed: {len(failed_episodes)}")
    print(f"{'='*70}")

    if failed_episodes:
        print("\nFailed episodes:")
        for f in failed_episodes[:5]:
            print(f"  - {f[:200]}...")

    if not success_datasets:
        print("No valid datasets. Exiting.")
        sys.exit(1)

    # 合并数据集
    try:
        from lerobot.common.datasets.dataset_tools import merge_datasets
    except ImportError:
        try:
            from lerobot.datasets.dataset_tools import merge_datasets
        except ImportError:
            print("Warning: Cannot import merge_datasets. Skipping merge.")
            print(f"Individual datasets saved in: {separate_dir}")
            sys.exit(0)

    print("\nMerging into final dataset...")
    datasets_to_merge = []
    for dpath in sorted(success_datasets):
        try:
            ds = LeRobotDataset(root=dpath, repo_id=dpath.name)
            datasets_to_merge.append(ds)
        except Exception as e:
            print(f"Failed to load {dpath}: {e}")

    if datasets_to_merge:
        merged = merge_datasets(
            datasets=datasets_to_merge,
            output_dir=output_root,
            output_repo_id=args.repo_id,
        )

        print(f"\nMerge Complete!")
        print(f"Output: {output_root}")
        print(f"Total Episodes: {merged.meta.total_episodes}")
        print(f"Total Frames: {merged.meta.total_frames}")

        if separate_dir.exists():
            shutil.rmtree(separate_dir)
            print("Cleaned up temporary episode datasets.")


if __name__ == "__main__":
    main()
