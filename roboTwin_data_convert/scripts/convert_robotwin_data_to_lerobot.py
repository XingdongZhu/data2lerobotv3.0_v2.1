"""
Script to convert processed RoboTwin HDF5 data to the LeRobot dataset v2.1 format.
Supports multiple robot platforms via --robot-type (e.g. aloha, arx_x5).
"""

import dataclasses
from pathlib import Path
import re
import shutil
from typing import Literal

import h5py

try:
    from lerobot.common.datasets.lerobot_dataset import HF_LEROBOT_HOME, LeRobotDataset
except ImportError:
    from lerobot.datasets.lerobot_dataset import HF_LEROBOT_HOME, LeRobotDataset

import numpy as np
import torch
import tqdm
import tyro
import json
import os
import fnmatch


RobotPlatform = Literal["aloha", "arx_x5", "franka", "ur5", "piper"]

ROBOT_TYPE_BY_PLATFORM: dict[RobotPlatform, tuple[str, str]] = {
    "aloha": ("sim_aloha", "sim_mobile_aloha"),
    "arx_x5": ("sim_arx_x5", "sim_mobile_arx_x5"),
    "franka": ("sim_franka", "sim_mobile_franka"),
    "ur5": ("sim_ur5", "sim_mobile_ur5"),
    "piper": ("sim_piper", "sim_mobile_piper"),
}


@dataclasses.dataclass(frozen=True)
class DatasetConfig:
    use_videos: bool = True
    tolerance_s: float = 0.0001
    image_writer_processes: int = 10
    image_writer_threads: int = 5
    video_backend: str | None = None


DEFAULT_DATASET_CONFIG = DatasetConfig()


def get_dataset_root(repo_id: str, output_root: Path | None = None) -> Path:
    if output_root is not None:
        return Path(output_root) / repo_id
    return HF_LEROBOT_HOME / repo_id


def resolve_robot_type(robot_platform: RobotPlatform, *, is_mobile: bool) -> str:
    fixed, mobile = ROBOT_TYPE_BY_PLATFORM[robot_platform]
    return mobile if is_mobile else fixed


JOINT_NAMES = [
    "left_waist",
    "left_shoulder",
    "left_elbow",
    "left_forearm_roll",
    "left_wrist_angle",
    "left_wrist_rotate",
    "left_gripper",
    "right_waist",
    "right_shoulder",
    "right_elbow",
    "right_forearm_roll",
    "right_wrist_angle",
    "right_wrist_rotate",
    "right_gripper",
]

ENDPOSE_NAMES = [
    "left_x",
    "left_y",
    "left_z",
    "left_qw",
    "left_qx",
    "left_qy",
    "left_qz",
    "right_x",
    "right_y",
    "right_z",
    "right_qw",
    "right_qx",
    "right_qy",
    "right_qz",
]

CAMERA_PARAM_KEYS = {
    "cam2world_gl": (4, 4),
    "extrinsic_cv": (3, 4),
    "intrinsic_cv": (3, 3),
}


def vector_names(dim: int) -> list[str]:
    names = JOINT_NAMES + ENDPOSE_NAMES
    if dim <= len(names):
        return names[:dim]
    extra = [f"dim_{i}" for i in range(len(names), dim)]
    return names + extra


def sort_hdf5_files(hdf5_files: list[str]) -> list[str]:
    def episode_key(path: str) -> int:
        match = re.search(r"episode_(\d+)", path)
        return int(match.group(1)) if match else 10**9

    return sorted(hdf5_files, key=episode_key)


def get_image_cameras(ep: h5py.File) -> list[str]:
    return sorted(
        key for key in ep["/observations/images"].keys() if "depth" not in key  # noqa: SIM118
    )


def get_camera_param_cameras(ep: h5py.File) -> list[str]:
    if "/observations/cameras" not in ep:
        return []
    return sorted(ep["/observations/cameras"].keys())  # noqa: SIM118


def has_velocity(hdf5_files: list[str]) -> bool:
    with h5py.File(hdf5_files[0], "r") as ep:
        return "/observations/qvel" in ep


def has_effort(hdf5_files: list[str]) -> bool:
    with h5py.File(hdf5_files[0], "r") as ep:
        return "/observations/effort" in ep


def build_features(
    sample_ep_path: str,
    mode: Literal["video", "image"],
    *,
    has_velocity: bool = False,
    has_effort: bool = False,
) -> dict:
    with h5py.File(sample_ep_path, "r") as ep:
        state_dim = ep["/observations/qpos"].shape[1]
        action_dim = ep["/action"].shape[1]
        image_cameras = get_image_cameras(ep)
        camera_param_cameras = get_camera_param_cameras(ep)

    names = vector_names(max(state_dim, action_dim))
    features = {
        "observation.state": {
            "dtype": "float32",
            "shape": (state_dim,),
            "names": [names[:state_dim]],
        },
        "action": {
            "dtype": "float32",
            "shape": (action_dim,),
            "names": [names[:action_dim]],
        },
    }

    if has_velocity:
        features["observation.velocity"] = {
            "dtype": "float32",
            "shape": (state_dim,),
            "names": [names[:state_dim]],
        }

    if has_effort:
        features["observation.effort"] = {
            "dtype": "float32",
            "shape": (state_dim,),
            "names": [names[:state_dim]],
        }

    for cam in image_cameras:
        features[f"observation.images.{cam}"] = {
            "dtype": mode,
            "shape": (3, 480, 640),
            "names": ["channels", "height", "width"],
        }

    for cam in camera_param_cameras:
        for key, shape in CAMERA_PARAM_KEYS.items():
            features[f"observation.cameras.{cam}.{key}"] = {
                "dtype": "float32",
                "shape": shape,
                "names": None,
            }

    return features


def create_empty_dataset(
    repo_id: str,
    sample_ep_path: str,
    robot_type: str,
    mode: Literal["video", "image"] = "video",
    *,
    output_root: Path | None = None,
    has_velocity: bool = False,
    has_effort: bool = False,
    dataset_config: DatasetConfig = DEFAULT_DATASET_CONFIG,
) -> LeRobotDataset:
    features = build_features(
        sample_ep_path,
        mode,
        has_velocity=has_velocity,
        has_effort=has_effort,
    )

    dataset_root = get_dataset_root(repo_id, output_root)
    if dataset_root.exists():
        shutil.rmtree(dataset_root)

    return LeRobotDataset.create(
        repo_id=repo_id,
        fps=15,
        robot_type=robot_type,
        features=features,
        root=dataset_root,
        use_videos=dataset_config.use_videos and mode == "video",
        tolerance_s=dataset_config.tolerance_s,
        image_writer_processes=dataset_config.image_writer_processes,
        image_writer_threads=dataset_config.image_writer_threads,
        video_backend=dataset_config.video_backend,
        batch_encoding_size=1,
    )


def load_raw_images_per_camera(ep: h5py.File, cameras: list[str]) -> dict[str, np.ndarray]:
    import cv2

    imgs_per_cam = {}
    for camera in cameras:
        dataset = ep[f"/observations/images/{camera}"]
        if dataset.ndim == 4:
            imgs_array = dataset[:]
        else:
            imgs_array = []
            for data in dataset:
                data = np.frombuffer(data, np.uint8)
                imgs_array.append(cv2.imdecode(data, cv2.IMREAD_COLOR))
            imgs_array = np.array(imgs_array)

        if imgs_array.ndim == 4:
            imgs_array = np.stack(
                [cv2.cvtColor(img, cv2.COLOR_BGR2RGB) for img in imgs_array]
            )
        elif imgs_array.ndim == 3:
            imgs_array = cv2.cvtColor(imgs_array, cv2.COLOR_BGR2RGB)

        imgs_per_cam[camera] = imgs_array

    return imgs_per_cam


def load_camera_params_per_camera(ep: h5py.File, cameras: list[str]) -> dict[str, dict[str, np.ndarray]]:
    cam_params = {}
    for camera in cameras:
        cam_group = ep[f"/observations/cameras/{camera}"]
        cam_params[camera] = {
            key: cam_group[key][:].astype(np.float32) for key in CAMERA_PARAM_KEYS
        }
    return cam_params


def load_raw_episode_data(
    ep_path: Path,
) -> tuple[
    dict[str, np.ndarray],
    dict[str, dict[str, np.ndarray]],
    torch.Tensor,
    torch.Tensor,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    with h5py.File(ep_path, "r") as ep:
        state = torch.from_numpy(ep["/observations/qpos"][:])
        action = torch.from_numpy(ep["/action"][:])

        velocity = None
        if "/observations/qvel" in ep:
            velocity = torch.from_numpy(ep["/observations/qvel"][:])

        effort = None
        if "/observations/effort" in ep:
            effort = torch.from_numpy(ep["/observations/effort"][:])

        image_cameras = get_image_cameras(ep)
        imgs_per_cam = load_raw_images_per_camera(ep, image_cameras)

        camera_param_cameras = get_camera_param_cameras(ep)
        cam_params = load_camera_params_per_camera(ep, camera_param_cameras)

    return imgs_per_cam, cam_params, state, action, velocity, effort


def populate_dataset(
    dataset: LeRobotDataset,
    hdf5_files: list[str],
    task: str,
    episodes: list[int] | None = None,
) -> LeRobotDataset:
    if episodes is None:
        episodes = range(len(hdf5_files))

    for ep_idx in tqdm.tqdm(episodes):
        ep_path = Path(hdf5_files[ep_idx])

        imgs_per_cam, cam_params, state, action, velocity, effort = load_raw_episode_data(ep_path)
        num_frames = state.shape[0]

        dir_path = os.path.dirname(ep_path)
        json_path = f"{dir_path}/instructions.json"
        with open(json_path, "r") as f_instr:
            instruction_dict = json.load(f_instr)
            instructions = instruction_dict["instructions"]
            instruction = np.random.choice(instructions)

        for i in range(num_frames):
            frame = {
                "observation.state": state[i],
                "action": action[i],
            }

            for camera, img_array in imgs_per_cam.items():
                frame[f"observation.images.{camera}"] = img_array[i]

            for camera, params in cam_params.items():
                for key in CAMERA_PARAM_KEYS:
                    frame[f"observation.cameras.{camera}.{key}"] = params[key][i]

            if velocity is not None:
                frame["observation.velocity"] = velocity[i]
            if effort is not None:
                frame["observation.effort"] = effort[i]

            dataset.add_frame(frame, task=instruction)
        dataset.save_episode()

    return dataset


def cleanup_images_dir(dataset_root: Path) -> None:
    """Remove empty images/ directory left after video encoding."""
    images_dir = dataset_root / "images"
    if not images_dir.exists():
        return

    for path in sorted(images_dir.rglob("*"), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()

    if images_dir.exists() and not any(images_dir.iterdir()):
        images_dir.rmdir()


def port_robotwin(
    raw_dir: Path,
    repo_id: str,
    raw_repo_id: str | None = None,
    task: str = "DEBUG",
    *,
    robot_type: RobotPlatform = "aloha",
    output_root: Path | None = None,
    episodes: list[int] | None = None,
    push_to_hub: bool = False,
    is_mobile: bool = False,
    mode: Literal["video", "image"] = "video",
    dataset_config: DatasetConfig = DEFAULT_DATASET_CONFIG,
):
    dataset_root = get_dataset_root(repo_id, output_root)
    if dataset_root.exists():
        shutil.rmtree(dataset_root)

    if not raw_dir.exists():
        if raw_repo_id is None:
            raise ValueError("raw_repo_id must be provided if raw_dir does not exist")

    hdf5_files = []
    for root, _, files in os.walk(raw_dir):
        for filename in fnmatch.filter(files, "*.hdf5"):
            hdf5_files.append(os.path.join(root, filename))

    if not hdf5_files:
        raise ValueError(f"No HDF5 files found under {raw_dir}")

    hdf5_files = sort_hdf5_files(hdf5_files)

    dataset = create_empty_dataset(
        repo_id,
        hdf5_files[0],
        robot_type=resolve_robot_type(robot_type, is_mobile=is_mobile),
        mode=mode,
        output_root=output_root,
        has_effort=has_effort(hdf5_files),
        has_velocity=has_velocity(hdf5_files),
        dataset_config=dataset_config,
    )
    dataset = populate_dataset(
        dataset,
        hdf5_files,
        task=task,
        episodes=episodes,
    )

    if mode == "video":
        cleanup_images_dir(dataset_root)

    if push_to_hub:
        dataset.push_to_hub()


if __name__ == "__main__":
    tyro.cli(port_robotwin)
