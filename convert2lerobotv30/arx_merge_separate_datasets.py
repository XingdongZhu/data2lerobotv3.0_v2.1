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

"""
Script to merge multiple LeRobot datasets into a single unified dataset.

This script supports two modes:
1. Separate mode: Merge multiple separate episode datasets from a single directory
   (each episode is saved as an independent dataset in its own subdirectory)
2. Multiple datasets mode: Merge multiple complete datasets from different directories
   (each directory is a complete dataset with multiple episodes)
"""

import argparse
import logging
from pathlib import Path

from lerobot.datasets.dataset_tools import merge_datasets
from lerobot.datasets.lerobot_dataset import LeRobotDataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_dataset_from_dir(dataset_dir: Path) -> LeRobotDataset:
    """Load a LeRobotDataset from a directory.
    
    Args:
        dataset_dir: Directory containing a LeRobot dataset
        
    Returns:
        LeRobotDataset object
    """
    dataset_dir = Path(dataset_dir)
    
    if not dataset_dir.exists():
        raise ValueError(f"Dataset directory does not exist: {dataset_dir}")
    
    meta_dir = dataset_dir / "meta"
    if not meta_dir.exists():
        raise ValueError(f"Not a valid LeRobot dataset: {dataset_dir} (no meta directory found)")
    
    # Try to load as LeRobotDataset
    try:
        # Read repo_id from info.json if available
        info_path = meta_dir / "info.json"
        if info_path.exists():
            import json
            with open(info_path, 'r') as f:
                info = json.load(f)
                repo_id = info.get("repo_id", dataset_dir.name)
        else:
            # Fallback: use directory name as repo_id
            repo_id = dataset_dir.name
        
        # Load dataset
        dataset = LeRobotDataset(repo_id=repo_id, root=dataset_dir)
        return dataset
    except Exception as e:
        raise ValueError(f"Failed to load dataset from {dataset_dir}: {e}")


def find_separate_datasets(source_dir: Path) -> list[LeRobotDataset]:
    """Find all separate dataset directories and load them as LeRobotDatasets.
    
    This function looks for subdirectories within source_dir, where each subdirectory
    is a separate episode dataset (typically from separate mode conversion).
    
    Args:
        source_dir: Directory containing separate dataset subdirectories
        
    Returns:
        List of LeRobotDataset objects
    """
    datasets = []
    source_dir = Path(source_dir)
    
    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")
    
    # Find all subdirectories that contain a meta directory (indicating a LeRobot dataset)
    for subdir in sorted(source_dir.iterdir()):
        if not subdir.is_dir():
            continue
            
        meta_dir = subdir / "meta"
        if not meta_dir.exists():
            logging.warning(f"Skipping {subdir.name}: no meta directory found")
            continue
        
        # Try to load as LeRobotDataset
        try:
            dataset = load_dataset_from_dir(subdir)
            datasets.append(dataset)
            logging.info(f"Found dataset: {subdir.name} ({dataset.meta.total_episodes} episodes, {dataset.meta.total_frames} frames)")
        except Exception as e:
            logging.warning(f"Failed to load dataset from {subdir.name}: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            continue
    
    if not datasets:
        raise ValueError(f"No valid datasets found in {source_dir}")
    
    return datasets


def collect_datasets(
    source_dir: Path | None = None,
    dataset_dirs: list[Path] | None = None,
) -> list[LeRobotDataset]:
    """Collect datasets from either source_dir (separate mode) or dataset_dirs (multiple datasets mode).
    
    Args:
        source_dir: Optional directory containing separate dataset subdirectories
        dataset_dirs: Optional list of directories, each containing a complete dataset
        
    Returns:
        List of LeRobotDataset objects
        
    Raises:
        ValueError: If neither source_dir nor dataset_dirs is provided, or if both are provided
    """
    if source_dir is None and dataset_dirs is None:
        raise ValueError("Either --source-dir or --dataset-dirs must be provided")
    
    if source_dir is not None and dataset_dirs is not None:
        raise ValueError("Cannot specify both --source-dir and --dataset-dirs. Use one or the other.")
    
    datasets = []
    
    if source_dir is not None:
        # Mode 1: Find separate datasets in source_dir
        logging.info(f"Searching for separate datasets in: {source_dir}")
        datasets = find_separate_datasets(source_dir)
    elif dataset_dirs is not None:
        # Mode 2: Load multiple complete datasets
        logging.info(f"Loading {len(dataset_dirs)} datasets from specified directories")
        for dataset_dir in dataset_dirs:
            try:
                dataset = load_dataset_from_dir(dataset_dir)
                datasets.append(dataset)
                logging.info(
                    f"Loaded dataset: {dataset_dir} "
                    f"({dataset.meta.total_episodes} episodes, {dataset.meta.total_frames} frames)"
                )
            except Exception as e:
                logging.error(f"Failed to load dataset from {dataset_dir}: {e}")
                raise
    
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
    """Merge multiple datasets into a single unified dataset.
    
    This function supports two modes:
    1. Separate mode: source_dir contains multiple subdirectories, each is a separate episode dataset
    2. Multiple datasets mode: dataset_dirs is a list of directories, each is a complete dataset
    
    Args:
        source_dir: Optional directory containing separate dataset subdirectories
        dataset_dirs: Optional list of directories, each containing a complete dataset
        output_dir: Output directory for merged dataset
        output_repo_id: Repository ID for merged dataset
        push_to_hub: Whether to push merged dataset to HuggingFace Hub
        
    Returns:
        Merged LeRobotDataset
    """
    # Collect all datasets
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
    
    # Merge datasets
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


def main():
    parser = argparse.ArgumentParser(
        description="Merge multiple LeRobot datasets into a single unified dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mode 1: Merge all separate datasets in a directory (separate mode)
  python merge_separate_datasets.py \\
      --source-dir /home/ailab/lerobot/datasets/astribot/pouring_water \\
      --target-dir /home/ailab/lerobot/datasets/astribot/pouring_water_merged \\
      --repo-id astribot/pouring_water

  # Mode 2: Merge multiple complete datasets from different directories
  python merge_separate_datasets.py \\
      --dataset-dirs /path/to/dataset1 /path/to/dataset2 /path/to/dataset3 \\
      --target-dir /home/ailab/lerobot/datasets/astribot/merged \\
      --repo-id astribot/merged

  # Merge and push to HuggingFace Hub
  python merge_separate_datasets.py \\
      --source-dir /home/ailab/lerobot/datasets/astribot/pouring_water \\
      --target-dir /home/ailab/lerobot/datasets/astribot/pouring_water_merged \\
      --repo-id astribot/pouring_water \\
      --push-to-hub
        """,
    )
    
    # Create mutually exclusive group for source options
    source_group = parser.add_mutually_exclusive_group(required=True)
    
    source_group.add_argument(
        "--source-dir",
        type=Path,
        help="Directory containing separate dataset subdirectories (separate mode). "
             "Each subdirectory should be a separate episode dataset.",
    )
    
    source_group.add_argument(
        "--dataset-dirs",
        type=Path,
        nargs="+",
        help="List of directories, each containing a complete dataset (multiple datasets mode). "
             "Each directory should be a complete LeRobot dataset with multiple episodes.",
    )
    
    parser.add_argument(
        "--target-dir",
        type=Path,
        required=True,
        help="Output directory for merged dataset",
    )
    
    parser.add_argument(
        "--repo-id",
        type=str,
        required=True,
        help="Repository ID for merged dataset (e.g., 'astribot/pouring_water')",
    )
    
    parser.add_argument(
        "--push-to-hub",
        action="store_true",
        help="Push merged dataset to HuggingFace Hub",
    )
    
    args = parser.parse_args()
    
    merge_datasets_from_paths(
        source_dir=args.source_dir,
        dataset_dirs=args.dataset_dirs,
        output_dir=args.target_dir,
        output_repo_id=args.repo_id,
        push_to_hub=args.push_to_hub,
    )


if __name__ == "__main__":
    main()

