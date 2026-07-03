#!/usr/bin/env python3
"""
松灵 COBOTMAGIC V2.0 (Songling Aloha) 数据转换脚本 - 转换为 LeRobot v3.0 格式
【no_norm 版本：所有维度保留原始物理量，effector 不做归一化】

厂商: 松灵机器人, model=COBOTMAGICV2.0, metadata.ver=2.1.0
对应 Excel "采集设备" 字段: "松灵Aloha"
适用于已对齐的 h5 文件 (*_align.h5)。

数据结构 (基于 metadata.json + 实际 h5 dataset shape):
    State / Action: 20 维
        - arm:       12 维 (left_arm 6 + right_arm 6), 单位 rad
                     [l-j1..l-j6, r-j1..r-j6]
                     (motor_names 中下划线化: l_j1..r_j6)
        - effector:   2 维 (left_gripper + right_gripper), 单位 m，clip 到 [0, 0.08]
                     【本脚本保留原始 m 物理量，不做归一化】

    备注:
        - h5 中 joints/{state,action}/robot/{angular,velocity} 均为 (N, 3) 全 0
          (底盘静止)，本平台静止通常全 0，现已纳入 state/action 对齐 config（共 20 维）。

    相机:
        - head:       原 720x1280 RGB, resize 到 640x480
        - hand_left:  原 720x1280 RGB, resize 到 640x480
        - hand_right: 原 720x1280 RGB, resize 到 640x480

Usage:
    python aloha_align2lerobot_v30_no_norm.py \\
        --input ./raw_data \\
        --output ./lerobot_dataset \\
        --task "Spell English word with alphabet blocks" \\
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

try:
    from datasets import disable_progress_bar
    disable_progress_bar()
except ImportError:
    pass

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

ALOHA_CONFIG = {
    "robot_type": "cobotmagic",
    "arm_dim": 12,        # 左臂6 + 右臂6
    "effector_dim": 2,    # left_gripper + right_gripper
    "robot_angular_dim": 3,   # 底盘角速度 robot_angular_x/y/z（平台静止时全 0）
    "robot_velocity_dim": 3,  # 底盘线速度 robot_vel_x/y/z（平台静止时全 0）
    "cameras": ["head", "hand_left", "hand_right"],
    "target_image_size": (640, 480),
}

MOTOR_NAMES = [
    # arm 12 (metadata: 'l-j1'..'l-j6', 'r-j1'..'r-j6'；按标准数据用连字符 l-j1)
    "l-j1", "l-j2", "l-j3", "l-j4", "l-j5", "l-j6",
    "r-j1", "r-j2", "r-j3", "r-j4", "r-j5", "r-j6",
    # effector 2
    "left_gripper", "right_gripper",
    # robot base 6（底盘角速度+线速度；该平台底盘静止、通常全 0，仅为对齐 spec 维度）
    "robot_angular_x", "robot_angular_y", "robot_angular_z",
    "robot_vel_x", "robot_vel_y", "robot_vel_z",
]

STATE_DIM = (
    ALOHA_CONFIG["arm_dim"] + ALOHA_CONFIG["effector_dim"]
    + ALOHA_CONFIG["robot_angular_dim"] + ALOHA_CONFIG["robot_velocity_dim"]
)
ACTION_DIM = STATE_DIM
assert len(MOTOR_NAMES) == STATE_DIM, (
    f"MOTOR_NAMES ({len(MOTOR_NAMES)}) != STATE_DIM ({STATE_DIM})"
)

# Effector(夹爪) 归一化参数：原始单位 m，物理范围约 [0, 0.07] → 归一化到 [0, 1]
EFFECTOR_MIN_M = 0.0
EFFECTOR_MAX_M = 0.07


def _normalize_effector(arr: np.ndarray) -> np.ndarray:
    """将 effector 从 m 归一化到 [0, 1]"""
    norm = (arr - EFFECTOR_MIN_M) / (EFFECTOR_MAX_M - EFFECTOR_MIN_M)
    return np.clip(norm, 0.0, 1.0).astype(np.float32)


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
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "rawvideo", "-pix_fmt", "rgb24",
        "-s", f"{width}x{height}", "-r", str(fps),
        "-i", "-", "-an",
        "-c:v", vcodec, "-pix_fmt", pix_fmt,
        "-g", str(gop), "-crf", str(crf),
    ]
    if vcodec == "libsvtav1":
        cmd.extend(["-preset", "8"])
    else:
        cmd.extend(["-preset", "fast"])
    cmd.append(str(video_path))
    process = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, bufsize=10**8,
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
# 数据读取
# =============================================================================

def _read_position(f: h5py.File, group: str, expect_dim: int, num_frames: int) -> np.ndarray:
    key = f"{group}/position"
    if key in f:
        arr = f[key][:]
        if arr.ndim != 2 or arr.shape[1] != expect_dim:
            logger.warning(
                f"{key} shape={arr.shape} 与期望 (N, {expect_dim}) 不一致，截断/补零"
            )
            arr = arr.reshape(arr.shape[0], -1)
            if arr.shape[1] > expect_dim:
                arr = arr[:, :expect_dim]
            elif arr.shape[1] < expect_dim:
                pad = np.zeros((arr.shape[0], expect_dim - arr.shape[1]), dtype=arr.dtype)
                arr = np.concatenate([arr, pad], axis=1)
        return arr.astype(np.float32)
    logger.warning(f"{key} 不存在，使用零填充 (N={num_frames}, dim={expect_dim})")
    return np.zeros((num_frames, expect_dim), dtype=np.float32)


def _read_dataset(f: h5py.File, key: str, expect_dim: int, num_frames: int) -> np.ndarray:
    """直接读取数据集（非 group/position 结构，如 joints/state/robot/angular）；缺失则零填充并 warn。"""
    if key in f:
        arr = np.asarray(f[key][:])
        if arr.ndim != 2 or arr.shape[1] != expect_dim:
            logger.warning(f"{key} shape={arr.shape} 与期望 (N,{expect_dim}) 不一致，截断/补零")
            arr = arr.reshape(arr.shape[0], -1)
            if arr.shape[1] > expect_dim:
                arr = arr[:, :expect_dim]
            elif arr.shape[1] < expect_dim:
                pad = np.zeros((arr.shape[0], expect_dim - arr.shape[1]), dtype=arr.dtype)
                arr = np.concatenate([arr, pad], axis=1)
        return arr.astype(np.float32)
    logger.warning(f"{key} 不存在，使用零填充 (N={num_frames}, dim={expect_dim})")
    return np.zeros((num_frames, expect_dim), dtype=np.float32)


def load_aloha_h5(h5_path: Path) -> Dict[str, Any]:
    """
    从松灵 Aloha COBOTMAGICV2.0 H5 文件加载数据

    Returns:
        {
            'frames': int,
            'state':  np.ndarray (N, 14) = arm12 + effector2
            'action': np.ndarray (N, 14)
            'images': {camera_id: List[np.ndarray]},
            'image_shapes': {camera_id: (H, W, C)},
            'task': str,
        }
    """
    data: Dict[str, Any] = {}

    with h5py.File(h5_path, 'r') as f:
        # metadata
        task_name = "manipulation_task"
        if 'metadata.json' in f:
            try:
                metadata = json.loads(f['metadata.json'][()])
                task_name = (
                    metadata.get('task_name')
                    or metadata.get('task')
                    or task_name
                )
            except Exception as e:
                logger.warning(f"读取 metadata.json 失败: {e}")
        data['task'] = task_name

        timestamps = f['timestamp'][:]
        num_frames = len(timestamps)
        data['frames'] = num_frames

        # ===== State / Action (no_norm: 保留原始 m / rad 物理量) =====
        s_arm      = _read_position(f, 'joints/state/arm',      ALOHA_CONFIG['arm_dim'],      num_frames)
        s_effector = _read_position(f, 'joints/state/effector', ALOHA_CONFIG['effector_dim'], num_frames)
        s_effector = np.clip(s_effector, 0.0, 0.08)  # prismatic 夹爪 m，clip 到 [0, 0.08]
        s_angular  = _read_dataset(f, 'joints/state/robot/angular',  ALOHA_CONFIG['robot_angular_dim'],  num_frames)
        s_velocity = _read_dataset(f, 'joints/state/robot/velocity', ALOHA_CONFIG['robot_velocity_dim'], num_frames)
        data['state'] = np.concatenate([s_arm, s_effector, s_angular, s_velocity], axis=1).astype(np.float32)

        a_arm      = _read_position(f, 'joints/action/arm',      ALOHA_CONFIG['arm_dim'],      num_frames)
        a_effector = _read_position(f, 'joints/action/effector', ALOHA_CONFIG['effector_dim'], num_frames)
        a_effector = np.clip(a_effector, 0.0, 0.08)  # prismatic 夹爪 m，clip 到 [0, 0.08]
        a_angular  = _read_dataset(f, 'joints/action/robot/angular',  ALOHA_CONFIG['robot_angular_dim'],  num_frames)
        a_velocity = _read_dataset(f, 'joints/action/robot/velocity', ALOHA_CONFIG['robot_velocity_dim'], num_frames)
        data['action'] = np.concatenate([a_arm, a_effector, a_angular, a_velocity], axis=1).astype(np.float32)

        # ===== 相机图像 =====
        images: Dict[str, List[np.ndarray]] = {}
        image_shapes: Dict[str, tuple] = {}
        target_width, target_height = ALOHA_CONFIG["target_image_size"]

        for cam_id in ALOHA_CONFIG["cameras"]:
            cam_key = f'cameras/{cam_id}/color/data'
            if cam_key not in f:
                logger.warning(f"相机数据不存在: {cam_key}，跳过")
                continue

            img_list: List[np.ndarray] = []
            img_bytes_array = f[cam_key][:]
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
                    logger.warning(f"Decode image failed ({cam_id}): {e}")

            if img_list:
                if len(img_list) > num_frames:
                    img_list = img_list[:num_frames]
                elif len(img_list) < num_frames and len(img_list) > 0:
                    img_list += [img_list[-1]] * (num_frames - len(img_list))
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
                "names": {"motors": MOTOR_NAMES},
                "fps": fps,
            },
            "action": {
                "dtype": "float32",
                "shape": (ACTION_DIM,),
                "names": {"motors": MOTOR_NAMES},
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

        temp_base_dir = Path(tempfile.mkdtemp())
        video_paths: Dict[str, Path] = {}

        for cam_id, img_list in episode_data['images'].items():
            if img_list:
                temp_video_dir = Path(tempfile.mkdtemp(dir=temp_base_dir))
                video_path = temp_video_dir / f"{cam_id}.mp4"
                encode_video(img_list, video_path, fps, vcodec=vcodec, crf=crf)
                video_paths[cam_id] = video_path

        del episode_data['images']

        episode_dir = output_dir / f"episode_{episode_index:04d}"
        if episode_dir.exists():
            shutil.rmtree(episode_dir)

        dataset = LeRobotDataset.create(
            repo_id=f"{repo_id}/episode_{episode_index:04d}",
            root=episode_dir,
            robot_type=ALOHA_CONFIG["robot_type"],
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

        episode_metadata: Dict[str, Any] = {}
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
        episode_data = load_aloha_h5(h5_path)
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

def find_episodes(data_dir: Path) -> List[Path]:
    if data_dir.is_file() and data_dir.suffix == ".h5":
        return [data_dir]

    episodes: List[Path] = []
    seen = set()

    for subdir in sorted(data_dir.iterdir()):
        if subdir.is_dir():
            h5_files = sorted(subdir.glob("*.h5"))
            if h5_files:
                episodes.append(h5_files[0])
                seen.add(h5_files[0])

    for h5_file in sorted(data_dir.glob("*.h5")):
        if h5_file not in seen:
            episodes.append(h5_file)

    return episodes


def main():
    parser = argparse.ArgumentParser(
        description="松灵 COBOTMAGIC V2.0 (Songling Aloha) 数据转换为 LeRobot v3.0 格式"
    )

    parser.add_argument("--input", type=Path, required=True,
                        help="数据目录或单个 H5 文件")
    parser.add_argument("--output", type=Path, required=True,
                        help="输出目录")
    parser.add_argument("--repo_id", type=str, default=None,
                        help="HuggingFace 仓库 ID（默认使用输出目录名）")
    parser.add_argument("--task", type=str, nargs='+', default=["manipulation_task"],
                        help="任务描述（多个词会自动拼接）")
    parser.add_argument("--fps", type=int, default=30,
                        help="数据集帧率 (默认: 30)")
    parser.add_argument("--workers", type=int, default=8,
                        help="并行工作进程数 (默认: 8)")
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

    print(f"\nRobot: {ALOHA_CONFIG['robot_type']}")
    print(f"State/Action dim: {STATE_DIM} (arm12 + effector2 + robot_angular3 + robot_velocity3)")
    print(f"Cameras: {ALOHA_CONFIG['cameras']}")
    print(f"Workers: {args.workers}, PyAV: {USE_PYAV}")
    print(f"Task: {args.task}")
    print(f"Effector: no_norm（保留原始 m 单位，范围约 [0, 0.07]）")

    output_root = args.output
    if output_root.exists():
        shutil.rmtree(output_root)
    separate_dir = output_root.parent / f"{output_root.name}_separate_episodes"
    separate_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    for i, path in enumerate(episodes):
        tasks.append((
            path, separate_dir, args.repo_id, i,
            args.fps, args.vcodec, args.crf, args.task,
        ))

    success_datasets: List[Path] = []
    failed_episodes: List[str] = []

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

    print(f"\n{'=' * 70}")
    print(f"Conversion completed in {elapsed:.1f}s")
    print(f"Success: {len(success_datasets)} / {len(episodes)}")
    print(f"Failed: {len(failed_episodes)}")
    print(f"{'=' * 70}")

    if failed_episodes:
        print("\nFailed episodes:")
        for f in failed_episodes[:5]:
            print(f"  - {f[:200]}...")

    if not success_datasets:
        print("No valid datasets. Exiting.")
        sys.exit(1)

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
