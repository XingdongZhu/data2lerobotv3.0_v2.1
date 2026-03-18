#!/usr/bin/env python3
"""
星海图 R1 机器人数据转换脚本 - 转换为 LeRobot 格式

使用已对齐的 h5 文件 (*_align.h5) 进行转换

星海图 R1 机器人数据结构:
- 关节: 12维 (左臂6维 + 右臂6维)
- 夹爪: 2维 (左夹爪 + 右夹爪)
- 相机: head (rgb), hand_left (rgbd), hand_right (rgbd)
- 图像分辨率: 480x640 (统一 resize 到此尺寸)

"""

import argparse
import logging
import shutil
import sys
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

from datasets import disable_progress_bar
disable_progress_bar()

import cv2
import h5py
import numpy as np
from tqdm import tqdm

# Video encoding
try:
    import av
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

# 星海图 R1 机器人配置
R1_CONFIG = {
    "robot_type": "xinghaitu_r1",
    "arm_dim": 12,           # 左臂6 + 右臂6
    "gripper_dim": 2,        # 左夹爪 + 右夹爪
    "cameras": ["head", "hand_left", "hand_right"],
    "image_shape": (480, 640, 3),
}

# State 和 Action 维度: 12 (arm) + 2 (gripper) = 14
STATE_DIM = R1_CONFIG["arm_dim"] + R1_CONFIG["gripper_dim"]
ACTION_DIM = STATE_DIM


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
    """使用 PyAV 编码视频"""
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
            if img_array.shape[2] == 4:  # RGBA -> RGB
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
    """使用 FFmpeg 子进程编码视频"""
    if len(images) == 0:
        raise ValueError("No images provided")
    
    height, width = images[0].shape[:2]
    video_path.parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "ffmpeg", "-y",
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
    """使用最佳可用方法编码视频"""
    if USE_PYAV:
        encode_video_pyav(images, video_path, fps, vcodec=vcodec, crf=crf)
    else:
        encode_video_ffmpeg(images, video_path, fps, vcodec=vcodec, crf=crf)


# =============================================================================
# 数据读取工具
# =============================================================================

def decode_compressed_rgb(image_bytes: bytes) -> np.ndarray:
    """解码压缩图像为 RGB numpy 数组"""
    np_arr = np.frombuffer(image_bytes, np.uint8)
    img_bgr = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if img_bgr is None:
        raise ValueError("Failed to decode compressed image")
    return cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)


def extract_task_from_filename(filename: str, default_task: str = "manipulation_task") -> str:
    """从文件名提取任务名称"""
    # 格式1: 桌面餐具整理_s121df5c5e3540528e8168a17970f9b3_align.h5 -> 桌面餐具整理
    # 格式2: s1a513c999a841c8a7b875ea89d15582.h5 -> 使用默认任务名
    parts = filename.replace('.h5', '').split('_')
    if len(parts) >= 2 and not parts[0].startswith('s'):
        return parts[0]
    return default_task


def load_aligned_h5(h5_path: Path, task: str = None) -> Dict[str, Any]:
    """
    从已对齐的 h5 文件加载数据
    
    Args:
        h5_path: h5 文件路径
        task: 任务名称 (如果指定，则使用该值；否则从文件名提取)
    
    Returns:
        {
            'frames': int,
            'timestamps': np.ndarray,
            'state': np.ndarray,  # (N, 14) = arm(12) + gripper(2)
            'action': np.ndarray, # (N, 14) = arm(12) + gripper(2)
            'images': {
                'camera_id': List[np.ndarray],  # RGB images
                ...
            },
            'task': str,
            'image_shape': (H, W, C),
        }
    """
    data = {}
    
    with h5py.File(h5_path, 'r') as f:
        # 时间戳
        timestamps = f['timestamp'][:]
        data['frames'] = len(timestamps)
        data['timestamps'] = timestamps
        
        # 任务名称 (优先使用传入的 task 参数)
        data['task'] = task if task else extract_task_from_filename(h5_path.name)
        
        # State: arm position (12) + effector position (2)
        arm_pos = f['joints/state/arm/position'][:]  # (N, 12)
        effector_pos = f['joints/state/effector/position'][:]  # (N, 2)
        # 归一化夹爪到 [0, 1] (原始范围约 0-100mm)
        effector_pos_normalized = effector_pos / 100.0
        data['state'] = np.concatenate([arm_pos, effector_pos_normalized], axis=1).astype(np.float32)
        
        # Action: arm position (12) + effector position (2)
        action_arm = f['joints/action/arm/position'][:]  # (N, 12)
        action_effector = f['joints/action/effector/position'][:]  # (N, 2)
        action_effector_normalized = action_effector / 100.0
        data['action'] = np.concatenate([action_arm, action_effector_normalized], axis=1).astype(np.float32)
        
        # 图像
        images = {}
        target_h, target_w, target_c = R1_CONFIG["image_shape"]
        
        for cam_id in R1_CONFIG["cameras"]:
            cam_key = f'cameras/{cam_id}/color/data'
            if cam_key in f:
                img_list = []
                img_bytes_array = f[cam_key][:]
                for img_bytes in img_bytes_array:
                    try:
                        img = decode_compressed_rgb(img_bytes)
                        # Resize 到目标尺寸
                        if img.shape[:2] != (target_h, target_w):
                            img = cv2.resize(img, (target_w, target_h), interpolation=cv2.INTER_LINEAR)
                        img_list.append(img)
                    except Exception as e:
                        logger.warning(f"Failed to decode image: {e}")
                        # 使用黑色图像作为占位符
                        img_list.append(np.zeros((target_h, target_w, target_c), dtype=np.uint8))
                
                if img_list:
                    images[cam_id] = img_list
        
        data['images'] = images
        data['image_shape'] = R1_CONFIG["image_shape"]
    
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
) -> dict:
    """
    将单个 episode 数据转换为 LeRobot 数据集格式
    """
    result = {
        'episode_index': episode_index,
        'success': False,
        'frames': 0,
        'error': None,
        'dataset_path': None,
    }
    
    try:
        num_frames = episode_data['frames']
        task = episode_data.get('task', 'manipulation_task')
        
        # 定义 features
        h, w, c = episode_data['image_shape']
        features = {
            "observation.state": {
                "dtype": "float32",
                "shape": (STATE_DIM,),
                "names": None,
            },
            "action": {
                "dtype": "float32",
                "shape": (ACTION_DIM,),
                "names": None,
            },
        }
        
        # 添加图像 features
        for cam_id in episode_data['images'].keys():
            features[f"observation.images.{cam_id}"] = {
                "dtype": "video",
                "shape": (h, w, c),
                "names": ["height", "width", "channels"],
            }
        
        # 创建临时目录用于视频编码
        temp_base_dir = Path(tempfile.mkdtemp())
        video_paths = {}
        
        # 编码视频
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
            robot_type=R1_CONFIG["robot_type"],
            fps=fps,
            features=features,
            use_videos=True,
            image_writer_threads=0,
        )
        
        # 添加帧
        logger.info(f"Adding {num_frames} frames...")
        for i in range(num_frames):
            frame_dict = {
                "observation.state": episode_data['state'][i],
                "action": episode_data['action'][i],
                "task": task,
            }
            
            # 添加占位符图像
            for cam_id in video_paths.keys():
                frame_dict[f"observation.images.{cam_id}"] = np.zeros((h, w, c), dtype=np.uint8)
            
            dataset.add_frame(frame_dict)
        
        # 保存 episode
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
        
        # Stack features
        for key, ft in dataset.features.items():
            if key in ["index", "episode_index", "task_index"]:
                continue
            if ft["dtype"] in ["image", "video"]:
                continue
            if key in episode_buffer:
                episode_buffer[key] = np.stack(episode_buffer[key])
        
        # 计算统计量
        non_video_features = {
            k: v for k, v in dataset.features.items()
            if v["dtype"] not in ["image", "video"]
        }
        non_video_buffer = {
            k: v for k, v in episode_buffer.items()
            if k not in dataset.meta.video_keys
        }
        ep_stats = compute_episode_stats(non_video_buffer, non_video_features)
        
        # 保存视频
        episode_metadata = {}
        for cam_id, temp_video_path in video_paths.items():
            video_key = f"observation.images.{cam_id}"
            video_metadata = dataset._save_episode_video(
                video_key=video_key,
                episode_index=0,
                temp_path=temp_video_path,
            )
            episode_metadata.update(video_metadata)
        
        # 删除视频 keys
        for video_key in list(episode_buffer.keys()):
            if video_key in dataset.meta.video_keys:
                del episode_buffer[video_key]
        
        # 保存数据
        ep_data_metadata = dataset._save_episode_data(episode_buffer)
        episode_metadata.update(ep_data_metadata)
        
        # 保存元数据
        dataset.meta.save_episode(0, episode_length, episode_tasks, ep_stats, episode_metadata)
        
        # 更新视频信息
        for video_key in dataset.meta.video_keys:
            dataset.meta.update_video_info(video_key)
        
        dataset.clear_episode_buffer(delete_images=False)
        dataset.finalize()
        
        # 清理临时目录
        shutil.rmtree(temp_base_dir, ignore_errors=True)
        
        result['success'] = True
        result['frames'] = num_frames
        result['dataset_path'] = str(episode_dir)
        
    except Exception as e:
        result['error'] = f"{e}\n{traceback.format_exc()}"
        logger.error(f"Failed processing episode {episode_index}: {e}")
    
    return result


def convert_episode_wrapper(args: tuple) -> dict:
    """进程池包装器"""
    h5_path, output_dir, repo_id, episode_index, fps, vcodec, crf, task = args
    
    try:
        episode_data = load_aligned_h5(h5_path, task=task)
        return convert_episode(
            episode_data=episode_data,
            output_dir=output_dir,
            repo_id=repo_id,
            episode_index=episode_index,
            fps=fps,
            vcodec=vcodec,
            crf=crf,
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

def find_episodes(data_dir: Path, default_task: str = "manipulation_task") -> List[Tuple[Path, str]]:
    """
    查找所有可转换的 episode
    
    支持两种目录结构:
    1. data_dir/*_align.h5  (原格式)
    2. data_dir/序列号/序列号.h5  (新格式)
    
    Returns:
        List of (path, task_name) tuples
    """
    episodes = []
    
    # 格式1: 直接在目录下的 *_align.h5 文件
    for h5_file in sorted(data_dir.glob("*_align.h5")):
        task = extract_task_from_filename(h5_file.name, default_task)
        episodes.append((h5_file, task))
    
    # 格式2: 子目录中的 .h5 文件 (序列号/序列号.h5)
    for subdir in sorted(data_dir.iterdir()):
        if subdir.is_dir():
            # 查找子目录中的 h5 文件
            h5_files = list(subdir.glob("*.h5"))
            if h5_files:
                h5_file = h5_files[0]  # 取第一个
                task = extract_task_from_filename(h5_file.name, default_task)
                episodes.append((h5_file, task))
    
    return episodes


def main():
    parser = argparse.ArgumentParser(
        description="星海图 R1 机器人数据转换为 LeRobot 格式"
    )
    
    parser.add_argument("--input", type=Path, required=True,
                        help="数据目录 (包含 *_align.h5 文件)")
    parser.add_argument("--output", type=Path, required=True,
                        help="输出目录")
    parser.add_argument("--repo_id", type=str, default=None,
                        help="HuggingFace 仓库 ID")
    parser.add_argument("--fps", type=int, default=30,
                        help="数据集帧率 (默认: 30)")
    parser.add_argument("--workers", type=int, default=8,
                        help="并行工作进程数 (默认: 4)")
    parser.add_argument("--vcodec", type=str, default="libsvtav1",
                        help="视频编码器 (默认: libsvtav1)")
    parser.add_argument("--crf", type=int, default=30,
                        help="视频质量 CRF (默认: 30)")
    parser.add_argument("--task", type=str, nargs='+', default=["manipulation_task"],
                        help="任务描述 (默认: manipulation_task，可以不用引号，多个词会自动拼接)")
    
    args = parser.parse_args()


     # 处理 task 参数：如果是列表，拼接成字符串
    if isinstance(args.task, list):
        args.task = ' '.join(args.task)
    
    if args.repo_id is None:
        args.repo_id = args.output.name  # 使用输出目录名作为默认 repo_id


    # 查找 episodes
    default_task = args.task or "manipulation_task"
    episodes = find_episodes(args.input, default_task)
    
    if not episodes:
        print(f"No .h5 files found in {args.input}")
        sys.exit(1)
    
    print(f"Found {len(episodes)} episodes:")
    for path, task in episodes:
        print(f"  - {path.name} ({task})")
    
    print(f"\nUsing {args.workers} workers, PyAV: {USE_PYAV}")
    
    # 设置输出目录
    output_root = args.output
    separate_dir = output_root.parent / f"{output_root.name}_separate_episodes"
    separate_dir.mkdir(parents=True, exist_ok=True)
    
    # 构建任务
    tasks = []
    for i, (path, task) in enumerate(episodes):
        tasks.append((
            path, separate_dir, args.repo_id, i,
            args.fps, args.vcodec, args.crf, task  # 传递 task 参数
        ))
    
    # 并行处理
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
    
    # 总结
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
    
    if output_root.exists():
        shutil.rmtree(output_root)
    
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
        
        # 清理临时目录
        if separate_dir.exists():
            shutil.rmtree(separate_dir)
            print("Cleaned up temporary episode datasets.")


if __name__ == "__main__":
    main()
