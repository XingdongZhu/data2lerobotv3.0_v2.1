#!/usr/bin/env python

# Copyright 2025 The HuggingFace Inc. team. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import io
import json
import logging
import os
import shutil
import tempfile
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import av
import h5py
import numpy as np
from PIL import Image
from scipy.spatial.transform import Rotation
from tqdm import tqdm

from lerobot.datasets.dataset_tools import merge_datasets
from lerobot.datasets.lerobot_dataset import LeRobotDataset
from lerobot.utils.utils import get_elapsed_time_in_days_hours_minutes_seconds

ARX_LOONG_FPS = 30
ARX_LOONG_ROBOT_TYPE = "arx_loong"
# Image dimensions from actual data inspection
IMAGE_HEIGHT = 480
IMAGE_WIDTH = 640
IMAGE_CHANNELS = 3

# Joint axes names for 14-dim combined features:
# left_arm(6) + left_gripper(1) + right_arm(6) + right_gripper(1)
JOINT_AXES_NAMES = [
    # left arm (6)
    "arm_master_l_joint1",
    "arm_master_l_joint2",
    "arm_master_l_joint3",
    "arm_master_l_joint4",
    "arm_master_l_joint5",
    "arm_master_l_joint6",
    # left gripper (1)
    "arm_master_l_joint7",
    # right arm (6)
    "arm_master_r_joint1",
    "arm_master_r_joint2",
    "arm_master_r_joint3",
    "arm_master_r_joint4",
    "arm_master_r_joint5",
    "arm_master_r_joint6",
    # right gripper (1)
    "arm_master_r_joint7",
]

# End-effector axes names (flattened):
# left_pos(3) + left_euler(3) + left_gripper(1) +
# right_pos(3) + right_euler(3) + right_gripper(1) = 14
END_EFFECTOR_AXES_NAMES = [
    "left_pos_x", "left_pos_y", "left_pos_z",
    "left_roll", "left_pitch", "left_yaw",
    "left_gripper",
    "right_pos_x", "right_pos_y", "right_pos_z",
    "right_roll", "right_pitch", "right_yaw",
    "right_gripper",
]

# Define LeRobot Dataset features
ARX_LOONG_FEATURES = {
    # Combined state position (left_arm 6 + left_gripper 1 + right_arm 6 + right_gripper 1 = 14)
    "observation.state": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # Combined state velocity
    "observation.velocity": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # Combined state effort
    "observation.effort": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # End-effector poses (position + euler angles + gripper, flattened)
    "observation.end": {
        "dtype": "float32",
        "shape": (14,),  # left_pos(3) + left_euler(3) + left_gripper(1) + right_pos(3) + right_euler(3) + right_gripper(1)
        "names": {"axes": END_EFFECTOR_AXES_NAMES},
    },
    # Action (derived from next frame)
    "action": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # Action velocity
    "action.velocity": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # Action effort
    "action.effort": {
        "dtype": "float32",
        "shape": (14,),
        "names": {"axes": JOINT_AXES_NAMES},
    },
    # Action end-effector poses (flattened)
    "action.end": {
        "dtype": "float32",
        "shape": (14,),  # left_pos(3) + left_euler(3) + left_gripper(1) + right_pos(3) + right_euler(3) + right_gripper(1)
        "names": {"axes": END_EFFECTOR_AXES_NAMES},
    },
    # Image features
    "observation.images.hand_left": {
        "dtype": "video",
        "shape": (IMAGE_HEIGHT, IMAGE_WIDTH, IMAGE_CHANNELS),
        "names": ["height", "width", "channels"],
    },
    "observation.images.hand_right": {
        "dtype": "video",
        "shape": (IMAGE_HEIGHT, IMAGE_WIDTH, IMAGE_CHANNELS),
        "names": ["height", "width", "channels"],
    },
    "observation.images.head": {
        "dtype": "video",
        "shape": (IMAGE_HEIGHT, IMAGE_WIDTH, IMAGE_CHANNELS),
        "names": ["height", "width", "channels"],
    },
}

def load_dataset_from_dir(dataset_dir: Path) -> LeRobotDataset:
    """Load a LeRobotDataset from a directory."""
    dataset_dir = Path(dataset_dir)

    if not dataset_dir.exists():
        raise ValueError(f"Dataset directory does not exist: {dataset_dir}")

    meta_dir = dataset_dir / "meta"
    if not meta_dir.exists():
        raise ValueError(f"Not a valid LeRobot dataset: {dataset_dir} (no meta directory found)")

    try:
        info_path = meta_dir / "info.json"
        if info_path.exists():
            with info_path.open("r", encoding="utf-8") as f:
                info = json.load(f)
            repo_id = info.get("repo_id", dataset_dir.name)
        else:
            repo_id = dataset_dir.name

        return LeRobotDataset(repo_id=repo_id, root=dataset_dir)
    except Exception as e:
        raise ValueError(f"Failed to load dataset from {dataset_dir}: {e}")


def find_separate_datasets(source_dir: Path) -> list[LeRobotDataset]:
    """Find all separate episode dataset directories and load them."""
    datasets = []
    source_dir = Path(source_dir)

    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")

    for subdir in sorted(source_dir.iterdir()):
        if not subdir.is_dir():
            continue

        meta_dir = subdir / "meta"
        if not meta_dir.exists():
            logging.warning(f"Skipping {subdir.name}: no meta directory found")
            continue

        try:
            dataset = load_dataset_from_dir(subdir)
            datasets.append(dataset)
            logging.info(
                f"Found dataset: {subdir.name} "
                f"({dataset.meta.total_episodes} episodes, {dataset.meta.total_frames} frames)"
            )
        except Exception as e:
            logging.warning(f"Failed to load dataset from {subdir.name}: {e}")
            logging.debug(traceback.format_exc())

    if not datasets:
        raise ValueError(f"No valid datasets found in {source_dir}")

    return datasets


def collect_datasets(
    source_dir: Path | None = None,
    dataset_dirs: list[Path] | None = None,
) -> list[LeRobotDataset]:
    """Collect datasets from either a separate-episode directory or explicit dataset dirs."""
    if source_dir is None and dataset_dirs is None:
        raise ValueError("Either source_dir or dataset_dirs must be provided")

    if source_dir is not None and dataset_dirs is not None:
        raise ValueError("Cannot specify both source_dir and dataset_dirs. Use one or the other.")

    if source_dir is not None:
        logging.info(f"Searching for separate datasets in: {source_dir}")
        return find_separate_datasets(source_dir)

    datasets = []
    logging.info(f"Loading {len(dataset_dirs)} datasets from specified directories")
    for dataset_dir in dataset_dirs:
        dataset = load_dataset_from_dir(dataset_dir)
        datasets.append(dataset)
        logging.info(
            f"Loaded dataset: {dataset_dir} "
            f"({dataset.meta.total_episodes} episodes, {dataset.meta.total_frames} frames)"
        )

    if not datasets:
        raise ValueError("No valid datasets found")

    return datasets


def merge_datasets_from_paths(
    source_dir: Path | None = None,
    dataset_dirs: list[Path] | None = None,
    output_dir: Path | None = None,
    output_repo_id: str | None = None,
    push_to_hub: bool = False,
) -> LeRobotDataset:
    """Merge multiple LeRobot datasets into a single unified dataset."""
    datasets = collect_datasets(source_dir=source_dir, dataset_dirs=dataset_dirs)

    logging.info(f"\n{'='*70}")
    logging.info(f"Found {len(datasets)} datasets to merge")
    logging.info(f"{'='*70}\n")

    total_episodes = sum(ds.meta.total_episodes for ds in datasets)
    total_frames = sum(ds.meta.total_frames for ds in datasets)
    logging.info(f"Total episodes: {total_episodes}")
    logging.info(f"Total frames: {total_frames}")
    logging.info(f"\nMerging datasets into: {output_repo_id}")
    logging.info(f"Output directory: {output_dir}")

    merged_dataset = merge_datasets(
        datasets=datasets,
        output_repo_id=output_repo_id,
        output_dir=output_dir,
    )

    logging.info(f"\n{'='*70}")
    logging.info("Merge completed successfully!")
    logging.info(f"{'='*70}")
    logging.info(f"Merged dataset: {output_repo_id}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Total episodes: {merged_dataset.meta.total_episodes}")
    logging.info(f"Total frames: {merged_dataset.meta.total_frames}")

    if push_to_hub:
        logging.info(f"\nPushing merged dataset to HuggingFace Hub as {output_repo_id}")
        merged_dataset.push_to_hub()
        logging.info("Push completed!")

    return merged_dataset

def decode_image(img_bytes):
    """Decode JPG binary image data to numpy array."""
    img = Image.open(io.BytesIO(img_bytes))
    return np.array(img, dtype=np.uint8)


def quaternion_to_euler(quat: np.ndarray) -> np.ndarray:
    """
    Convert quaternion (x, y, z, w) to Euler angles (roll, pitch, yaw).
    
    Args:
        quat: Quaternion array of shape (4,) in (x, y, z, w) format
    
    Returns:
        Euler angles (roll, pitch, yaw) in radians
    """
    # scipy expects (x, y, z, w) format
    rot = Rotation.from_quat(quat)
    euler = rot.as_euler('xyz', degrees=False)  # Returns (roll, pitch, yaw)
    return euler


def encode_video_from_memory(
    images: list[np.ndarray],
    video_path: Path,
    fps: int,
    vcodec: str = "libsvtav1",
    pix_fmt: str = "yuv420p",
    g: int = 2,
    crf: int = 30,
    fast_decode: int = 0,
    preset: int = 12,
) -> None:
    """Encode video directly from memory images (numpy arrays).
    
    Args:
        images: List of numpy arrays (H, W, 3) in uint8 format
        video_path: Output video file path
        fps: Frames per second
        vcodec: Video codec (libsvtav1, h264, hevc)
        pix_fmt: Pixel format
        g: Group of pictures size
        crf: Constant rate factor
        fast_decode: Fast decode tuning
        preset: Encoder preset
    """
    if len(images) == 0:
        raise ValueError("No images provided for video encoding")
    
    # Get image dimensions from first frame
    height, width = images[0].shape[:2]
    
    # Encoders/pixel formats incompatibility check
    if (vcodec == "libsvtav1" or vcodec == "hevc") and pix_fmt == "yuv444p":
        logging.warning(
            f"Incompatible pixel format 'yuv444p' for codec {vcodec}, auto-selecting format 'yuv420p'"
        )
        pix_fmt = "yuv420p"
    
    # Define video codec options
    video_options = {}
    if g is not None:
        video_options["g"] = str(g)
    if crf is not None:
        video_options["crf"] = str(crf)
    if fast_decode:
        key = "svtav1-params" if vcodec == "libsvtav1" else "tune"
        value = f"fast-decode={fast_decode}" if vcodec == "libsvtav1" else "fastdecode"
        video_options[key] = value
    if vcodec == "libsvtav1":
        video_options["preset"] = str(preset)
    
    video_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create and open output file
    with av.open(str(video_path), "w") as output:
        output_stream = output.add_stream(vcodec, fps, options=video_options)
        output_stream.pix_fmt = pix_fmt
        output_stream.width = width
        output_stream.height = height
        
        # Loop through images and encode them
        for img_array in images:
            # Convert numpy array to PIL Image
            img = Image.fromarray(img_array)
            img = img.convert("RGB")
            
            # Create video frame from image
            input_frame = av.VideoFrame.from_image(img)
            packet = output_stream.encode(input_frame)
            if packet:
                output.mux(packet)
        
        # Flush the encoder
        packet = output_stream.encode()
        if packet:
            output.mux(packet)
    
    if not video_path.exists():
        raise OSError(f"Video encoding did not work. File not found: {video_path}.")


def _combine_joint_data(h5, frame_idx: int, data_type: str) -> np.ndarray:
    """Combine joint data from arm and effector into a single array.
    
    Args:
        h5: HDF5 file handle
        frame_idx: Frame index to read
        data_type: Type of data to read (position, velocity, effort)
    
    Returns:
        np.ndarray: Combined array of shape (14,) in order:
            left_arm(6) + left_effector(1) + right_arm(6) + right_effector(1)
    """
    arm = h5[f'joints/state/arm/{data_type}'][frame_idx]  # (12,) -> [left_arm(6), right_arm(6)]
    effector = h5[f'joints/state/effector/{data_type}'][frame_idx]  # (2,) -> [left, right]

    left_arm = arm[:6]
    right_arm = arm[6:]
    left_eff = effector[:1]
    right_eff = effector[1:]

    return np.concatenate([left_arm, left_eff, right_arm, right_eff]).astype(np.float32)


def _convert_end_effector_pose(h5, frame_idx: int) -> np.ndarray:
    """
    Convert end-effector pose from position + quaternion to position + euler angles.
    
    Args:
        h5: HDF5 file handle
        frame_idx: Frame index to read
    
    Returns:
        np.ndarray: Array of shape (14,) containing:
            [left_pos(3) + left_euler(3) + left_gripper(1) +
             right_pos(3) + right_euler(3) + right_gripper(1)]
    """
    # Read position (N, 2, 3) and orientation (N, 2, 4)
    end_pos = h5['joints/state/end/position'][frame_idx]  # (2, 3)
    end_orient = h5['joints/state/end/orientation'][frame_idx]  # (2, 4)
    # Read gripper (effector) position (N, 2)
    effector_pos = h5['joints/state/effector/position'][frame_idx]  # (2,)
    
    # Convert quaternions to Euler angles
    left_euler = quaternion_to_euler(end_orient[0])  # (3,)
    right_euler = quaternion_to_euler(end_orient[1])  # (3,)
    
    # Combine position + euler for each end-effector
    left_pose = np.concatenate([end_pos[0], left_euler])  # (6,)
    right_pose = np.concatenate([end_pos[1], right_euler])  # (6,)

    left_gripper = effector_pos[:1]   # (1,)
    right_gripper = effector_pos[1:]  # (1,)
    
    # Flatten to (14,) for PyArrow compatibility:
    # left_pos(3) + left_euler(3) + left_gripper(1) +
    # right_pos(3) + right_euler(3) + right_gripper(1)
    return np.concatenate([left_pose, left_gripper, right_pose, right_gripper]).astype(np.float32)


def generate_lerobot_frames(h5_path: Path, task_description: str, task_instructions: str | None = None, fps_ratio: float = 1.0):
    """Generate LeRobot format frames from HDF5 file.
    
    Uses adjacent frame logic: state from frame i, action from frame i+1.
    
    Args:
        h5_path: Path to HDF5 file
        task_description: Task description string (required)
        task_instructions: Optional detailed task instructions
        fps_ratio: Frame sampling ratio (e.g., 0.5 means take every other frame)
    
    Yields:
        dict: Frame dictionary in LeRobot format
    """
    with h5py.File(h5_path, 'r') as h5:
        # Load metadata.json if available
        metadata = None
        if 'metadata.json' in h5:
            metadata_bytes = h5['metadata.json'][()]
            if isinstance(metadata_bytes, bytes):
                metadata = json.loads(metadata_bytes.decode('utf-8'))
            else:
                metadata = json.loads(metadata_bytes)
        
        # Get number of frames
        num_frames = h5['joints/state/arm/position'].shape[0]
        
        # Calculate frame indices based on fps_ratio
        if fps_ratio == 1.0:
            frame_indices = range(num_frames - 1)  # -1 because we need next frame for action
        else:
            step = 1.0 / fps_ratio
            max_idx = int((num_frames - 1) * fps_ratio)
            frame_indices = [int(i * step) for i in range(max_idx) if int(i * step) < num_frames - 1]
        
        # Process each frame
        for frame_idx in frame_indices:
            next_frame_idx = frame_idx + 1
            
            # Read state data from current frame
            state_position = _combine_joint_data(h5, frame_idx, 'position')
            state_velocity = _combine_joint_data(h5, frame_idx, 'velocity')
            state_effort = _combine_joint_data(h5, frame_idx, 'effort')
            state_end = _convert_end_effector_pose(h5, frame_idx)
            
            # Read action data from next frame
            action_position = _combine_joint_data(h5, next_frame_idx, 'position')
            action_velocity = _combine_joint_data(h5, next_frame_idx, 'velocity')
            action_effort = _combine_joint_data(h5, next_frame_idx, 'effort')
            action_end = _convert_end_effector_pose(h5, next_frame_idx)
            
            # Filter effector position: if position > -0.5, set to 0
            # Effector is the last 2 elements of action_position
            action_effector_pos = action_position[-2:].copy()
            action_effector_pos[action_effector_pos > -0.5] = 0.0
            action_position[-2:] = action_effector_pos
            
            # Decode images from current frame
            hand_left_img = decode_image(h5['cameras/hand_left/color/data'][frame_idx])
            hand_right_img = decode_image(h5['cameras/hand_right/color/data'][frame_idx])
            head_img = decode_image(h5['cameras/head/color/data'][frame_idx])
            
            # Create frame dictionary with all features
            frame = {
                # State features
                "observation.state": state_position,
                "observation.velocity": state_velocity,
                "observation.effort": state_effort,
                "observation.end": state_end,
                # Action features (from next frame)
                "action": action_position,
                "action.velocity": action_velocity,
                "action.effort": action_effort,
                "action.end": action_end,
                # Images
                "observation.images.hand_left": hand_left_img,
                "observation.images.hand_right": hand_right_img,
                "observation.images.head": head_img,
                "task": task_description,
            }
            
            # Add optional task.instructions
            if task_instructions:
                frame["task.instructions"] = task_instructions
            
            yield frame


def check_episode_exists_separate(episode_output_dir: Path) -> bool:
    """Check if a separate episode dataset already exists and is complete.
    
    Args:
        episode_output_dir: Directory where the episode dataset should be stored
    
    Returns:
        bool: True if episode exists and appears complete, False otherwise
    """
    if not episode_output_dir.exists():
        return False
    
    # Check for essential files
    meta_dir = episode_output_dir / "meta"
    if not meta_dir.exists():
        return False
    
    info_file = meta_dir / "info.json"
    if not info_file.exists():
        return False
    
    # Check if data directory exists and has files
    data_dir = episode_output_dir / "data"
    if data_dir.exists():
        parquet_files = list(data_dir.rglob("*.parquet"))
        if len(parquet_files) == 0:
            return False
    
    return True


def check_episode_exists_normal(dataset: LeRobotDataset, episode_index: int) -> bool:
    """Check if an episode already exists in a normal mode dataset.
    
    Args:
        dataset: LeRobotDataset instance
        episode_index: Index of the episode to check
    
    Returns:
        bool: True if episode exists, False otherwise
    """
    try:
        # Check if metadata has this episode
        if dataset.meta.episodes is None:
            return False
        
        if episode_index >= len(dataset.meta.episodes):
            return False
        
        # Check if data file exists
        data_path = dataset.root / dataset.meta.get_data_file_path(episode_index)
        if not data_path.exists():
            return False
        
        # Check if video files exist (if applicable)
        for video_key in dataset.meta.video_keys:
            video_path = dataset.root / dataset.meta.get_video_file_path(episode_index, video_key)
            if not video_path.exists():
                return False
        
        return True
    except (IndexError, KeyError, FileNotFoundError):
        return False


def convert_single_episode(
    h5_path: Path,
    output_dir: Path,
    task: str,
    task_instructions: str | None = None,
    episode_id: str | None = None,
    fps_ratio: float = 1.0,
) -> int:
    """Convert a single HDF5 episode to an independent LeRobot dataset.
    
    This function is used in separate mode where each episode is saved as a separate dataset.
    
    Args:
        h5_path: Path to HDF5 file
        output_dir: Output directory for the dataset
        task: Task description string
        task_instructions: Optional detailed task instructions
        episode_id: Optional episode identifier (used for repo_id)
        fps_ratio: Frame sampling ratio
    
    Returns:
        int: Number of frames converted
    """
    # Generate episode ID from file name if not provided
    if episode_id is None:
        episode_id = h5_path.stem
    
    # Create repo_id for this episode
    repo_id = f"arx_loong/{episode_id}"
    
    # Calculate effective FPS based on fps_ratio
    effective_fps = int(ARX_LOONG_FPS * fps_ratio)
    
    # Build features dictionary
    features = ARX_LOONG_FEATURES.copy()
    if task_instructions:
        features["task.instructions"] = {
            "dtype": "string",
            "shape": (1,),
            "names": None,
        }
    
    # Create dataset for this episode
    episode_output_dir = output_dir / episode_id
    lerobot_dataset = LeRobotDataset.create(
        repo_id=repo_id,
        robot_type=ARX_LOONG_ROBOT_TYPE,
        fps=effective_fps,
        features=features,
        root=episode_output_dir,
        use_videos=True,
    )
    
    # Process episode: encode videos and generate frames with images
    temp_base_dir = Path(tempfile.mkdtemp(dir=lerobot_dataset.root))
    try:
        # Load all images into memory first (with frame sampling)
        with h5py.File(h5_path, 'r') as h5:
            num_frames = h5['joints/state/arm/position'].shape[0]
            
            # Calculate frame indices based on fps_ratio
            if fps_ratio == 1.0:
                frame_indices = range(num_frames - 1)  # -1 because we need next frame for action
            else:
                step = 1.0 / fps_ratio
                max_idx = int((num_frames - 1) * fps_ratio)
                frame_indices = [int(i * step) for i in range(max_idx) if int(i * step) < num_frames - 1]
            
            # Load sampled images
            hand_left_images = []
            hand_right_images = []
            head_images = []
            for frame_idx in frame_indices:
                hand_left_images.append(decode_image(h5['cameras/hand_left/color/data'][frame_idx]))
                hand_right_images.append(decode_image(h5['cameras/hand_right/color/data'][frame_idx]))
                head_images.append(decode_image(h5['cameras/head/color/data'][frame_idx]))
        
        # Encode videos immediately to release memory
        video_paths = {}
        for video_key, images in [
            ('observation.images.hand_left', hand_left_images),
            ('observation.images.hand_right', hand_right_images),
            ('observation.images.head', head_images),
        ]:
            # Create a separate temp directory for each video
            temp_video_dir = Path(tempfile.mkdtemp(dir=temp_base_dir))
            temp_video_path = temp_video_dir / f"{video_key}_000.mp4"
            
            encode_video_from_memory(
                images=images,
                video_path=temp_video_path,
                fps=effective_fps,
            )
            video_paths[video_key] = temp_video_path
        
        # Release image memory after encoding
        del hand_left_images, hand_right_images, head_images
        
        # Generate frames with images (needed for add_frame validation)
        # We'll delete the images after add_frame saves them, then use pre-encoded videos
        num_frames = 0
        for frame in generate_lerobot_frames(h5_path, task, task_instructions, fps_ratio):
            lerobot_dataset.add_frame(frame)
            num_frames += 1
        
        # Wait for image writer
        lerobot_dataset._wait_image_writer()
        
        # Save episode (compute stats before deleting images)
        from lerobot.datasets.compute_stats import compute_episode_stats
        
        episode_buffer = lerobot_dataset.episode_buffer
        episode_length = episode_buffer.pop("size")
        tasks_list = episode_buffer.pop("task")
        episode_tasks = list(set(tasks_list))
        
        episode_buffer["index"] = np.arange(0, episode_length)
        episode_buffer["episode_index"] = np.zeros((episode_length,), dtype=np.int32)
        
        lerobot_dataset.meta.save_episode_tasks(episode_tasks)
        episode_buffer["task_index"] = np.array([
            lerobot_dataset.meta.get_task_index(task) for task in tasks_list
        ])
        
        # Stack non-video features for saving
        for key, ft in lerobot_dataset.features.items():
            # index, episode_index, task_index are already processed above, and image and video
            # are processed separately by storing image path and frame info as meta data
            if key in ["index", "episode_index", "task_index"] or ft["dtype"] in ["image", "video"]:
                continue
            if key in episode_buffer:
                episode_buffer[key] = np.stack(episode_buffer[key])
        
        # Compute stats (images are still in episode_buffer as paths, compute_episode_stats will handle them)
        ep_stats = compute_episode_stats(episode_buffer, lerobot_dataset.features)
        
        # Delete image directories (after computing stats)
        for video_key in video_paths.keys():
            img_dir = lerobot_dataset._get_image_file_dir(0, video_key)
            if img_dir.exists():
                shutil.rmtree(img_dir)
        
        # Save videos
        episode_metadata = {}
        for video_key, temp_video_path in video_paths.items():
            if not temp_video_path.exists():
                raise FileNotFoundError(f"Video file not found: {temp_video_path}")
            
            video_metadata = lerobot_dataset._save_episode_video(
                video_key=video_key,
                episode_index=0,
                temp_path=temp_video_path,
            )
            episode_metadata.update(video_metadata)
        
        # Save episode data to parquet files
        ep_data_metadata = lerobot_dataset._save_episode_data(episode_buffer)
        episode_metadata.update(ep_data_metadata)
        
        # Save episode metadata
        lerobot_dataset.meta.save_episode(0, episode_length, episode_tasks, ep_stats, episode_metadata)
        
        # Update video info
        for video_key in video_paths.keys():
            lerobot_dataset.meta.update_video_info(video_key)
        
        # Clear buffer
        lerobot_dataset.clear_episode_buffer(delete_images=False)
        
        # Clean up temp directories
        for temp_video_path in video_paths.values():
            if temp_video_path.parent.exists():
                try:
                    shutil.rmtree(temp_video_path.parent)
                except Exception as e:
                    logging.warning(f"Failed to clean up temp directory {temp_video_path.parent}: {e}")
        
        # Finalize dataset
        lerobot_dataset.finalize()
        
        return num_frames
    finally:
        if temp_base_dir.exists():
            try:
                shutil.rmtree(temp_base_dir)
            except Exception as e:
                logging.warning(f"Failed to clean up temp base directory {temp_base_dir}: {e}")


def convert_worker_separate(args: tuple) -> dict:
    """Worker function for separate mode - converts a single episode to independent dataset.
    
    Args:
        args: Tuple (h5_path, output_dir, task, task_instructions, episode_id, worker_id, fps_ratio)
    
    Returns:
        dict: Result dictionary with success, error, frames, duration
    """
    h5_path, output_dir, task, task_instructions, episode_id, worker_id, fps_ratio = args
    
    result = {
        'h5_path': str(h5_path),
        'episode_id': episode_id,
        'success': False,
        'error': None,
        'frames': 0,
        'duration': 0,
    }
    
    try:
        start_time = datetime.now()
        
        frames = convert_single_episode(
            h5_path=h5_path,
            output_dir=output_dir,
            task=task,
            task_instructions=task_instructions,
            episode_id=episode_id,
            fps_ratio=fps_ratio,
        )
        
        end_time = datetime.now()
        
        result['success'] = True
        result['frames'] = frames
        result['duration'] = (end_time - start_time).total_seconds()
        
    except Exception as e:
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()
    
    return result


def convert_worker(args: tuple) -> dict:
    """Worker function for normal mode - converts a single episode.
    
    Args:
        args: Tuple (h5_path, episode_index, task, task_instructions, temp_dir, worker_id, fps_ratio)
    
    Returns:
        dict: Result dictionary with success, error, frames, duration, video_paths, frame_data
    """
    h5_path, episode_index, task, task_instructions, temp_dir, worker_id, fps_ratio = args
    
    result = {
        'h5_path': str(h5_path),
        'episode_index': episode_index,
        'success': False,
        'error': None,
        'frames': 0,
        'duration': 0,
        'video_paths': None,
        'frame_data': None,
    }
    
    try:
        start_time = datetime.now()
        
        # Calculate effective FPS based on fps_ratio
        effective_fps = int(ARX_LOONG_FPS * fps_ratio)
        
        # Load all images into memory first (we'll encode and release immediately)
        with h5py.File(h5_path, 'r') as h5:
            num_frames = h5['joints/state/arm/position'].shape[0]
            
            # Calculate frame indices based on fps_ratio
            if fps_ratio == 1.0:
                frame_indices = range(num_frames - 1)  # -1 because we need next frame for action
            else:
                step = 1.0 / fps_ratio
                max_idx = int((num_frames - 1) * fps_ratio)
                frame_indices = [int(i * step) for i in range(max_idx) if int(i * step) < num_frames - 1]
            
            # Load sampled images
            hand_left_images = []
            hand_right_images = []
            head_images = []
            for frame_idx in frame_indices:
                hand_left_images.append(decode_image(h5['cameras/hand_left/color/data'][frame_idx]))
                hand_right_images.append(decode_image(h5['cameras/hand_right/color/data'][frame_idx]))
                head_images.append(decode_image(h5['cameras/head/color/data'][frame_idx]))
        
        # Encode videos immediately to release memory
        video_paths = {}
        for video_key, images in [
            ('observation.images.hand_left', hand_left_images),
            ('observation.images.hand_right', hand_right_images),
            ('observation.images.head', head_images),
        ]:
            # Create a separate temp directory for each video
            temp_video_dir = Path(tempfile.mkdtemp(dir=temp_dir))
            temp_video_path = temp_video_dir / f"{video_key}_{episode_index:03d}.mp4"
            
            encode_video_from_memory(
                images=images,
                video_path=temp_video_path,
                fps=effective_fps,
            )
            video_paths[video_key] = temp_video_path
        
        # Release image memory immediately after encoding
        del hand_left_images, hand_right_images, head_images
        
        # Generate frames without images (only state, action, task data)
        frames = []
        for frame in generate_lerobot_frames(h5_path, task, task_instructions, fps_ratio):
            # Remove image data from frame (we've already encoded videos)
            frame.pop("observation.images.hand_left", None)
            frame.pop("observation.images.hand_right", None)
            frame.pop("observation.images.head", None)
            frames.append(frame)
        
        end_time = datetime.now()
        
        result['success'] = True
        result['frames'] = len(frames)
        result['duration'] = (end_time - start_time).total_seconds()
        result['video_paths'] = video_paths
        result['frame_data'] = frames
        
    except Exception as e:
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()
    
    return result


def port_arx_hdf5_loong(
    source_dir: Path,
    target_dir: Path,
    repo_id: str | None,
    task: str,
    task_instructions: str | None = None,
    push_to_hub: bool = False,
    num_workers: int | None = None,
    resume: bool = True,
    fps_ratio: float = 1.0,
):
    """Convert HDF5 format ARX-AC1 data to LeRobot Dataset v3.0 format.
    
    The function automatically chooses the processing mode:
    - If num_workers > 1: Uses separate mode (each episode saved independently) for parallel processing, then automatically merges
    - If num_workers == 1 or None: Uses normal mode (all episodes in one dataset) for serial processing
    
    Args:
        source_dir: Directory containing HDF5 files (each episode in a subdirectory or directly)
        target_dir: Target directory for LeRobot dataset
        repo_id: Repository identifier for the dataset. If None, generated from target_dir name.
        task: Task description string (required)
        task_instructions: Optional detailed task instructions
        push_to_hub: Whether to push dataset to Hugging Face Hub
        num_workers: Number of parallel workers. If None, auto-detect CPU count. If 1, use serial processing.
                    If > 1, automatically uses separate mode and merges at the end.
        resume: If True, skip episodes that already exist in the target directory. Defaults to True.
        fps_ratio: Frame sampling ratio (e.g., 0.5 means take every other frame for 60fps->30fps conversion).
                   Defaults to 1.0 (take all frames).
    """
    if repo_id is None:
        repo_id = f"arx_loong/{target_dir.name}"
        logging.info(f"repo_id not provided, using default: {repo_id}")

    # Find all HDF5 files
    h5_files = []
    episode_ids = []
    
    # Check if source_dir contains HDF5 files directly or in subdirectories
    h5_files_direct = list(source_dir.glob("*.hdf5")) + list(source_dir.glob("*.h5"))
    if h5_files_direct:
        # HDF5 files are directly in source_dir
        for h5_file in sorted(h5_files_direct):
            h5_files.append(h5_file)
            episode_ids.append(h5_file.stem)
    else:
        # Look for HDF5 files in subdirectories
        for subdir in sorted(source_dir.iterdir()):
            if subdir.is_dir():
                h5_file = list(subdir.glob("*.hdf5")) + list(subdir.glob("*.h5"))
                if h5_file:
                    h5_files.append(h5_file[0])
                    episode_ids.append(subdir.name)  # Use subdirectory name as episode ID
    
    if not h5_files:
        raise ValueError(f"No HDF5 files found in {source_dir}")
    
    logging.info(f"Found {len(h5_files)} HDF5 files")
    
    # Calculate effective FPS based on fps_ratio
    effective_fps = int(ARX_LOONG_FPS * fps_ratio)
    
    if fps_ratio != 1.0:
        logging.info(f"Frame sampling enabled: fps_ratio={fps_ratio}, original FPS={ARX_LOONG_FPS}, effective FPS={effective_fps}")
    
    # Determine number of workers and processing mode
    if num_workers is None:
        num_workers = 1
        logging.info(f"Not setting num_workers, using {num_workers} workers")
    
    # Auto-select mode: use separate mode if num_workers > 1 for parallel processing
    use_separate_mode = num_workers > 1
    
    if use_separate_mode:
        logging.info(f"Using separate mode with {num_workers} parallel workers (will auto-merge at the end)")
    else:
        logging.info("Using normal mode (serial processing)")
    
    # Handle separate mode (for parallel processing)
    if use_separate_mode:
        # Each episode is saved as a separate dataset first, then merged into final target_dir.
        if target_dir is None:
            final_target_dir = source_dir.parent / f"{source_dir.name}_lerobot"
        else:
            final_target_dir = Path(target_dir)
        
        separate_target_dir = final_target_dir.parent / f"{final_target_dir.name}_separate_episodes"
        separate_target_dir.mkdir(parents=True, exist_ok=True)
        
        if num_workers <= 0:
            num_workers = 1
            logging.warning(f"Invalid num_workers ({num_workers}), using 1 (serial processing)")
        
        logging.info(f"\n{'='*70}")
        logging.info(f"Separate Mode: Processing {len(h5_files)} episodes")
        logging.info(f"Separate episode directory: {separate_target_dir}")
        logging.info(f"Final output directory: {final_target_dir}")
        logging.info(f"Parallel workers: {num_workers}")
        logging.info(f"{'='*70}\n")
        
        if num_workers > 1:
            # Parallel processing
            # Filter out episodes that already exist if resume is enabled
            tasks = []
            results = []
            skipped_count = 0
            for i, (h5_path, episode_id) in enumerate(zip(h5_files, episode_ids)):
                episode_output_dir = separate_target_dir / episode_id
                if resume and check_episode_exists_separate(episode_output_dir):
                    logging.info(f"Skipping existing episode: {episode_id}")
                    skipped_count += 1
                    results.append({
                        'h5_path': str(h5_path),
                        'episode_id': episode_id,
                        'success': True,
                        'error': None,
                        'frames': 0,
                        'duration': 0,
                        'skipped': True,
                    })
                    continue
                tasks.append((h5_path, separate_target_dir, task, task_instructions, episode_id, i % num_workers, fps_ratio))
            
            if skipped_count > 0:
                logging.info(f"Skipped {skipped_count} existing episodes (resume mode enabled)")
            if len(tasks) == 0:
                logging.info("All episodes already exist. Continue to merge existing separate datasets.")
            else:
                with ProcessPoolExecutor(max_workers=num_workers) as executor:
                    future_to_task = {
                        executor.submit(convert_worker_separate, task): task
                        for task in tasks
                    }
                    
                    completed = 0
                    with tqdm(total=len(tasks), desc="Processing episodes") as pbar:
                        for future in as_completed(future_to_task):
                            task = future_to_task[future]
                            completed += 1
                            
                            try:
                                result = future.result()
                                results.append(result)
                                
                                if result['success']:
                                    logging.info(
                                        f"[{completed}/{len(tasks)}] ✓ {result['episode_id']}: "
                                        f"{result['frames']} frames, {result['duration']:.1f}s"
                                    )
                                else:
                                    logging.error(
                                        f"[{completed}/{len(tasks)}] ✗ {result['episode_id']}: "
                                        f"{result['error']}"
                                    )
                            except Exception as e:
                                logging.error(
                                    f"[{completed}/{len(tasks)}] ✗ {task[4]}: 执行异常 - {e}"
                                )
                                results.append({
                                    'h5_path': str(task[0]),
                                    'episode_id': task[4],
                                    'success': False,
                                    'error': str(e),
                                    'frames': 0,
                                    'duration': 0,
                                })
                            
                            pbar.update(1)
        else:
            # Serial processing
            results = []
            skipped_count = 0
            for i, (h5_path, episode_id) in enumerate(zip(h5_files, episode_ids), 1):
                episode_output_dir = separate_target_dir / episode_id
                if resume and check_episode_exists_separate(episode_output_dir):
                    logging.info(f"\n[{i}/{len(h5_files)}] Skipping existing episode: {episode_id}")
                    skipped_count += 1
                    results.append({
                        'h5_path': str(h5_path),
                        'episode_id': episode_id,
                        'success': True,
                        'error': None,
                        'frames': 0,
                        'duration': 0,
                        'skipped': True,
                    })
                    continue
                
                logging.info(f"\n[{i}/{len(h5_files)}] Processing: {episode_id}")
                try:
                    result = convert_worker_separate((
                        h5_path, separate_target_dir, task, task_instructions, episode_id, 0, fps_ratio
                    ))
                    result['skipped'] = False
                    results.append(result)
                    
                    if result['success']:
                        logging.info(f"  ✓ Complete: {result['frames']} frames, {result['duration']:.1f}s")
                    else:
                        logging.error(f"  ✗ Failed: {result['error']}")
                except Exception as e:
                    logging.error(f"  ✗ Exception: {e}")
                    results.append({
                        'h5_path': str(h5_path),
                        'episode_id': episode_id,
                        'success': False,
                        'error': str(e),
                        'frames': 0,
                        'duration': 0,
                    })
        
        # Generate summary report
        logging.info(f"\n{'='*70}")
        logging.info("转换完成汇总")
        logging.info(f"{'='*70}")
        
        success_count = sum(1 for r in results if r['success'])
        total_frames = sum(r.get('frames', 0) for r in results)
        total_duration = sum(r.get('duration', 0) for r in results)
        
        logging.info(f"成功: {success_count}/{len(results)}")
        logging.info(f"总帧数: {total_frames}")
        logging.info(f"总耗时: {total_duration:.1f}s")
        
        if success_count < len(results):
            logging.warning(f"\n失败的转换:")
            for r in results:
                if not r['success']:
                    logging.warning(f"  - {r['episode_id']}: {r['error']}")
        
        if success_count == 0:
            raise RuntimeError("All episodes failed to process. Cannot merge datasets.")
        
        # Auto-merge separate datasets into one unified dataset
        logging.info(f"\n{'='*70}")
        logging.info("Auto-merging separate datasets into unified dataset")
        logging.info(f"{'='*70}\n")
        
        # Merge into the user-provided final output directory.
        if final_target_dir.exists():
            shutil.rmtree(final_target_dir)
        
        try:
            merged_dataset = merge_datasets_from_paths(
                source_dir=separate_target_dir,
                output_dir=final_target_dir,
                output_repo_id=repo_id,
                push_to_hub=push_to_hub,
            )
            
            logging.info(f"\n{'='*70}")
            logging.info("Auto-merge completed successfully!")
            logging.info(f"{'='*70}")
            logging.info(f"Merged dataset saved to: {final_target_dir}")
            logging.info(f"Total episodes: {merged_dataset.meta.total_episodes}")
            logging.info(f"Total frames: {merged_dataset.meta.total_frames}")
            if separate_target_dir.exists():
                shutil.rmtree(separate_target_dir)
                logging.info(f"Cleaned up separate episode directory: {separate_target_dir}")
            
        except Exception as e:
            logging.error(f"Failed to merge datasets: {e}")
            import traceback
            logging.error(traceback.format_exc())
            raise
        
        return
    
    # Build features dictionary (add task.instructions if provided)
    features = ARX_LOONG_FEATURES.copy()
    if task_instructions:
        features["task.instructions"] = {
            "dtype": "string",
            "shape": (1,),
            "names": None,
        }
    
    # Check if dataset already exists (for resume mode)
    dataset_exists = False
    existing_episodes = set()
    if resume and target_dir.exists():
        try:
            # Try to load existing dataset
            existing_dataset = LeRobotDataset(repo_id=repo_id, root=target_dir)
            dataset_exists = True
            existing_episodes = set(range(existing_dataset.meta.total_episodes))
            logging.info(f"Found existing dataset with {len(existing_episodes)} episodes")
        except Exception as e:
            logging.info(f"Could not load existing dataset: {e}. Will create new dataset.")
            dataset_exists = False
    
    # Create or load LeRobotDataset
    if dataset_exists:
        lerobot_dataset = existing_dataset
        logging.info(f"Resuming conversion. Existing episodes: {sorted(existing_episodes)}")
    else:
        lerobot_dataset = LeRobotDataset.create(
            repo_id=repo_id,
            robot_type=ARX_LOONG_ROBOT_TYPE,
            fps=effective_fps,
            features=features,
            root=target_dir,
            use_videos=True,
        )
    
    start_time = time.time()
    
    # Ensure num_workers is valid (should already be set, but double-check)
    if num_workers is None:
        num_workers = 1
    elif num_workers <= 0:
        num_workers = 1
        logging.warning(f"Invalid num_workers, using 1 (serial processing)")
    
    # Process episodes
    if num_workers > 1:
        # Parallel processing
        logging.info(f"\n{'='*70}")
        logging.info(f"Processing {len(h5_files)} episodes with {num_workers} parallel workers")
        logging.info(f"{'='*70}\n")
        
        # Create temporary directory for worker processes to store encoded videos
        temp_base_dir = Path(tempfile.mkdtemp(dir=lerobot_dataset.root))
        try:
            # Prepare task arguments
            # Filter out episodes that already exist if resume is enabled
            tasks = []
            skipped_count = 0
            episode_index_mapping = {}  # Map original index to new episode index
            new_episode_index = len(existing_episodes) if dataset_exists else 0
            
            for i, (original_idx, h5_path) in enumerate(enumerate(h5_files)):
                if resume and dataset_exists and original_idx in existing_episodes:
                    if check_episode_exists_normal(lerobot_dataset, original_idx):
                        logging.info(f"Skipping existing episode {original_idx}: {h5_path.name}")
                        skipped_count += 1
                        continue
                
                # Use new episode index for new episodes
                episode_index_mapping[original_idx] = new_episode_index
                tasks.append((h5_path, new_episode_index, task, task_instructions, temp_base_dir, i % num_workers, fps_ratio))
                new_episode_index += 1
            
            if skipped_count > 0:
                logging.info(f"Skipped {skipped_count} existing episodes (resume mode enabled)")
            if len(tasks) == 0:
                logging.info("All episodes already exist. Nothing to process.")
                return
            
            results = []
            
            # Use ProcessPoolExecutor for parallel processing
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                future_to_task = {
                    executor.submit(convert_worker, task): task
                    for task in tasks
                }
                
                completed = 0
                with tqdm(total=len(tasks), desc="Processing episodes") as pbar:
                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        completed += 1
                        
                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result['success']:
                                logging.info(
                                    f"[{completed}/{len(tasks)}] ✓ {Path(result['h5_path']).name}: "
                                    f"{result['frames']} frames, {result['duration']:.1f}s"
                                )
                            else:
                                logging.error(
                                    f"[{completed}/{len(tasks)}] ✗ {Path(result['h5_path']).name}: "
                                    f"{result['error']}"
                                )
                        except Exception as e:
                            logging.error(
                                f"[{completed}/{len(tasks)}] ✗ {task[0].name}: 执行异常 - {e}"
                            )
                            results.append({
                                'h5_path': str(task[0]),
                                'episode_index': task[1],
                                'success': False,
                                'error': str(e),
                                'frames': 0,
                                'duration': 0,
                            })
                        
                        pbar.update(1)
            
            # Generate summary report
            logging.info(f"\n{'='*70}")
            logging.info("转换完成汇总")
            logging.info(f"{'='*70}")
            
            success_count = sum(1 for r in results if r['success'])
            total_frames = sum(r.get('frames', 0) for r in results)
            total_duration = sum(r.get('duration', 0) for r in results)
            
            logging.info(f"成功: {success_count}/{len(results)}")
            logging.info(f"总帧数: {total_frames}")
            logging.info(f"总耗时: {total_duration:.1f}s")
            
            if success_count < len(results):
                logging.warning(f"\n失败的转换:")
                for r in results:
                    if not r['success']:
                        logging.warning(f"  - {Path(r['h5_path']).name}: {r['error']}")
            
            if success_count == 0:
                raise RuntimeError("All episodes failed to process")
            
            # Process episodes in order: add frames, save videos, save episode, release memory
            # Store results by episode index to maintain order
            episode_results = {}
            for result in results:
                if result['success']:
                    episode_results[result['episode_index']] = result
            
            # Get sorted episode indices from results
            sorted_episode_indices = sorted(episode_results.keys())
            for episode_index in sorted_episode_indices:
                result = episode_results[episode_index]
                frames = result['frame_data']
                video_paths = result['video_paths']
                
                # Add frames to dataset (without images, as videos are already encoded)
                for frame in frames:
                    lerobot_dataset.add_frame(frame)
                
                # Wait for image writer to finish (if async)
                lerobot_dataset._wait_image_writer()
                
                # Manually save episode (replicating save_episode logic but skipping video encoding)
                from lerobot.datasets.compute_stats import compute_episode_stats
                
                episode_buffer = lerobot_dataset.episode_buffer
                episode_length = episode_buffer.pop("size")
                tasks_list = episode_buffer.pop("task")
                episode_tasks = list(set(tasks_list))
                episode_index_val = episode_buffer["episode_index"]
                
                episode_buffer["index"] = np.arange(
                    lerobot_dataset.meta.total_frames,
                    lerobot_dataset.meta.total_frames + episode_length
                )
                episode_buffer["episode_index"] = np.full((episode_length,), episode_index_val)
                
                lerobot_dataset.meta.save_episode_tasks(episode_tasks)
                episode_buffer["task_index"] = np.array([
                    lerobot_dataset.meta.get_task_index(task) for task in tasks_list
                ])
                
                # Stack non-video features for saving
                for key, ft in lerobot_dataset.features.items():
                    # index, episode_index, task_index are already processed above, and image and video
                    # are processed separately by storing image path and frame info as meta data
                    if key in ["index", "episode_index", "task_index"] or ft["dtype"] in ["image", "video"]:
                        continue
                    if key in episode_buffer:
                        episode_buffer[key] = np.stack(episode_buffer[key])
                
                # Compute stats (images are still in episode_buffer as paths, compute_episode_stats will handle them)
                ep_stats = compute_episode_stats(episode_buffer, lerobot_dataset.features)
                
                # Delete image directories if they exist (after computing stats)
                for video_key in video_paths.keys():
                    img_dir = lerobot_dataset._get_image_file_dir(episode_index, video_key)
                    if img_dir.exists():
                        shutil.rmtree(img_dir)
                
                # Manually save videos using pre-encoded video files
                episode_metadata = {}
                for video_key, temp_video_path in video_paths.items():
                    if not temp_video_path.exists():
                        raise FileNotFoundError(f"Video file not found: {temp_video_path}")
                    
                    video_metadata = lerobot_dataset._save_episode_video(
                        video_key=video_key,
                        episode_index=episode_index,
                        temp_path=temp_video_path,
                    )
                    episode_metadata.update(video_metadata)
                
                # Save episode data to parquet files
                ep_data_metadata = lerobot_dataset._save_episode_data(episode_buffer)
                episode_metadata.update(ep_data_metadata)
                
                # Save episode metadata
                lerobot_dataset.meta.save_episode(
                    episode_index_val, episode_length, episode_tasks, ep_stats, episode_metadata
                )
                
                # Update video info if first episode
                if episode_index_val == 0:
                    for video_key in video_paths.keys():
                        lerobot_dataset.meta.update_video_info(video_key)
                
                # Clear episode buffer and release memory
                lerobot_dataset.clear_episode_buffer(delete_images=False)
                
                # Clean up temp video directories
                for temp_video_path in video_paths.values():
                    if temp_video_path.parent.exists():
                        try:
                            shutil.rmtree(temp_video_path.parent)
                        except Exception as e:
                            logging.warning(f"Failed to clean up temp directory {temp_video_path.parent}: {e}")
                
                logging.info(f"Saved episode {episode_index + 1}")
        finally:
            # Clean up temp base directory
            if temp_base_dir.exists():
                try:
                    shutil.rmtree(temp_base_dir)
                except Exception as e:
                    logging.warning(f"Failed to clean up temp base directory {temp_base_dir}: {e}")
    else:
        # Serial processing (using same logic as parallel for memory efficiency)
        logging.info(f"Processing {len(h5_files)} episodes serially")
        
        # Create temporary directory for storing encoded videos
        temp_base_dir = Path(tempfile.mkdtemp(dir=lerobot_dataset.root))
        try:
            skipped_count = 0
            new_episode_index = len(existing_episodes) if dataset_exists else 0
            
            for original_index, h5_path in enumerate(h5_files):
                # Check if episode already exists (for resume mode)
                if resume and dataset_exists and original_index in existing_episodes:
                    if check_episode_exists_normal(lerobot_dataset, original_index):
                        logging.info(
                            f"Skipping existing episode {original_index + 1} / {len(h5_files)}: {h5_path.name}"
                        )
                        skipped_count += 1
                        continue
                
                # Use new episode index for new episodes
                episode_index = new_episode_index
                new_episode_index += 1
                
                elapsed_time = time.time() - start_time
                d, h, m, s = get_elapsed_time_in_days_hours_minutes_seconds(elapsed_time)
                
                logging.info(
                    f"Processing episode {episode_index + 1} / {len(h5_files)}: {h5_path.name} "
                    f"(after {d} days, {h} hours, {m} minutes, {s:.3f} seconds)"
                )
                
                # Process episode using worker function (for consistency)
                result = convert_worker((h5_path, episode_index, task, task_instructions, temp_base_dir, 0, fps_ratio))
                
                if not result['success']:
                    logging.error(f"Failed to process episode {episode_index + 1}: {result['error']}")
                    continue
                
                frames = result['frame_data']
                video_paths = result['video_paths']
                
                # Add frames to dataset (without images, as videos are already encoded)
                for frame in frames:
                    lerobot_dataset.add_frame(frame)
                
                # Wait for image writer to finish (if async)
                lerobot_dataset._wait_image_writer()
                
                # Manually save episode (replicating save_episode logic but skipping video encoding)
                from lerobot.datasets.compute_stats import compute_episode_stats
                
                episode_buffer = lerobot_dataset.episode_buffer
                episode_length = episode_buffer.pop("size")
                tasks_list = episode_buffer.pop("task")
                episode_tasks = list(set(tasks_list))
                episode_index_val = episode_buffer["episode_index"]
                
                episode_buffer["index"] = np.arange(
                    lerobot_dataset.meta.total_frames,
                    lerobot_dataset.meta.total_frames + episode_length
                )
                episode_buffer["episode_index"] = np.full((episode_length,), episode_index_val)
                
                lerobot_dataset.meta.save_episode_tasks(episode_tasks)
                episode_buffer["task_index"] = np.array([
                    lerobot_dataset.meta.get_task_index(task) for task in tasks_list
                ])
                
                # Stack non-video features for saving
                for key, ft in lerobot_dataset.features.items():
                    # index, episode_index, task_index are already processed above, and image and video
                    # are processed separately by storing image path and frame info as meta data
                    if key in ["index", "episode_index", "task_index"] or ft["dtype"] in ["image", "video"]:
                        continue
                    if key in episode_buffer:
                        episode_buffer[key] = np.stack(episode_buffer[key])
                
                # Compute stats (images are still in episode_buffer as paths, compute_episode_stats will handle them)
                ep_stats = compute_episode_stats(episode_buffer, lerobot_dataset.features)
                
                # Delete image directories if they exist (after computing stats)
                for video_key in video_paths.keys():
                    img_dir = lerobot_dataset._get_image_file_dir(episode_index, video_key)
                    if img_dir.exists():
                        shutil.rmtree(img_dir)
                
                # Manually save videos using pre-encoded video files
                episode_metadata = {}
                for video_key, temp_video_path in video_paths.items():
                    if not temp_video_path.exists():
                        raise FileNotFoundError(f"Video file not found: {temp_video_path}")
                    
                    video_metadata = lerobot_dataset._save_episode_video(
                        video_key=video_key,
                        episode_index=episode_index,
                        temp_path=temp_video_path,
                    )
                    episode_metadata.update(video_metadata)
                
                # Save episode data to parquet files
                ep_data_metadata = lerobot_dataset._save_episode_data(episode_buffer)
                episode_metadata.update(ep_data_metadata)
                
                # Save episode metadata
                lerobot_dataset.meta.save_episode(
                    episode_index_val, episode_length, episode_tasks, ep_stats, episode_metadata
                )
                
                # Update video info if first episode
                if episode_index_val == 0:
                    for video_key in video_paths.keys():
                        lerobot_dataset.meta.update_video_info(video_key)
                
                # Clear episode buffer and release memory
                lerobot_dataset.clear_episode_buffer(delete_images=False)
                
                # Clean up temp video directories
                for temp_video_path in video_paths.values():
                    if temp_video_path.parent.exists():
                        try:
                            shutil.rmtree(temp_video_path.parent)
                        except Exception as e:
                            logging.warning(f"Failed to clean up temp directory {temp_video_path.parent}: {e}")
                
                logging.info(f"Saved episode {episode_index + 1}")
        finally:
            # Clean up temp base directory
            if temp_base_dir.exists():
                try:
                    shutil.rmtree(temp_base_dir)
                except Exception as e:
                    logging.warning(f"Failed to clean up temp base directory {temp_base_dir}: {e}")
    
    # Finalize dataset
    lerobot_dataset.finalize()
    logging.info("Dataset finalized")
    
    if push_to_hub:
        lerobot_dataset.push_to_hub(
            tags=["arx_loong"],
            private=False,
        )
        logging.info("Dataset pushed to Hugging Face Hub")


def main():
    parser = argparse.ArgumentParser(
        description="Convert HDF5 format ARX-AC1 data to LeRobot Dataset v3.0 format"
    )
    
    parser.add_argument(
        "--input",
        "--source-dir",
        dest="input",
        type=Path,
        required=True,
        help="Directory containing HDF5 files (each episode in a subdirectory or directly)",
    )
    parser.add_argument(
        "--output",
        "--target-dir",
        dest="output",
        type=Path,
        required=True,
        help="Target directory for LeRobot dataset",
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        default=None,
        help="Repository identifier for the dataset (default: arx_loong/<output_dir_name>)",
    )
    parser.add_argument(
        "--task",
        type=str,
        nargs='+',
        default=["manipulation_task"],
        help="Task description string (default: manipulation_task; multiple words can be passed without quotes)",
    )
    parser.add_argument(
        "--task-instructions",
        type=str,
        default=None,
        help="Optional detailed task instructions",
    )
    parser.add_argument(
        "--push-to-hub",
        action="store_true",
        help="Upload dataset to Hugging Face Hub",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=8,
        help="Number of parallel workers for processing episodes. If None, auto-detect CPU count. "
             "If 1, uses serial processing (normal mode). If > 1, automatically uses separate mode "
             "for parallel processing and merges at the end.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Skip episodes that already exist in the target directory (default: True). "
             "Use --no-resume to disable and overwrite existing episodes.",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Disable resume mode and overwrite existing episodes.",
    )
    parser.add_argument(
        "--fps-ratio",
        type=float,
        default=1.0,
        help="Frame sampling ratio (e.g., 0.5 means take every other frame for 60fps->30fps conversion). "
             "Defaults to 1.0 (take all frames).",
    )
    
    args = parser.parse_args()
    if isinstance(args.task, list):
        args.task = ' '.join(args.task)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    port_arx_hdf5_loong(
        source_dir=args.input,
        target_dir=args.output,
        repo_id=args.repo_id,
        task=args.task,
        task_instructions=args.task_instructions,
        push_to_hub=args.push_to_hub,
        num_workers=args.num_workers,
        resume=args.resume,
        fps_ratio=args.fps_ratio,
    )


if __name__ == "__main__":
    main()
