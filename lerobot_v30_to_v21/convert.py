#!/usr/bin/env python3
"""LeRobot Dataset v3.0 -> v2.1 Converter

Wrapper script that converts one or more LeRobot v3.0 datasets to v2.1 format.
Output directories are organized as: <output-dir>/<robot_type>/<dataset_id>/

Features:
    - 单个数据集转换
    - 批量并行转换（使用多进程）
    - Resume 支持（自动跳过已转换的数据集）

Usage:
    # Convert a single dataset
    python convert.py --input /path/to/lerobot_v30/<dataset_id> --output-dir /path/to/output

    # Batch convert all datasets under a directory (parallel with 4 workers)
    python convert.py --input /path/to/lerobot_v30 --output-dir /path/to/output --batch

    # Batch convert with 8 parallel workers
    python convert.py --input /path/to/lerobot_v30 --output-dir /path/to/output --batch --workers 8

    # Disable robot_type grouping (flat output)
    python convert.py --input /path/to/lerobot_v30 --output-dir /path/to/output --batch --no-group-by-robot
    
    # Verbose mode (show detailed logs)
    python convert.py --input /path/to/lerobot_v30 --output-dir /path/to/output --batch --verbose
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# 禁用进度条和详细日志
os.environ['TQDM_DISABLE'] = '1'

from datasets import disable_progress_bar
disable_progress_bar()

from convert_dataset_v30_to_v21 import convert_dataset

# 设置日志级别为 WARNING，隐藏 INFO 级别的输出
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_dataset_info(path: Path) -> dict | None:
    """Load info.json from a dataset directory. Returns None if not found or invalid."""
    info_json = path / "meta" / "info.json"
    if not info_json.exists():
        return None
    try:
        with open(info_json) as f:
            return json.load(f)
    except (json.JSONDecodeError, KeyError):
        return None


def is_v30_dataset(path: Path) -> bool:
    """Check if a directory looks like a valid v3.0 dataset."""
    info = load_dataset_info(path)
    return info is not None and info.get("codebase_version") == "v3.0"


def get_robot_type(path: Path) -> str:
    """Extract robot_type from a dataset's info.json. Returns 'unknown' if not found."""
    info = load_dataset_info(path)
    if info is None:
        return "unknown"
    return info.get("robot_type", "unknown")


def discover_datasets(input_dir: Path) -> list[Path]:
    """Discover all v3.0 datasets under a directory."""
    datasets = []
    for child in sorted(input_dir.iterdir()):
        if child.is_dir() and is_v30_dataset(child):
            datasets.append(child)
    return datasets


def is_dataset_converted(dataset_id: str, output_dir: Path, robot_type: str = None) -> bool:
    """
    检查数据集是否已经转换完成
    
    Args:
        dataset_id: 数据集ID
        output_dir: 输出根目录
        robot_type: 机器人类型（如果按类型分组）
    
    Returns:
        True 如果数据集已存在且有效
    """
    if robot_type:
        output_path = output_dir / robot_type / dataset_id
    else:
        output_path = output_dir / dataset_id
    
    # 检查目录是否存在以及是否有 meta/info.json
    info_file = output_path / "meta" / "info.json"
    if info_file.exists():
        try:
            info = load_dataset_info(output_path)
            # 检查是否是 v2.1 格式
            return info is not None and info.get("codebase_version") == "v2.1"
        except:
            return False
    return False


def convert_single(input_path: Path, output_dir: Path, repo_id_prefix: str, group_by_robot: bool = True) -> bool:
    """Convert a single dataset. Returns True on success."""
    dataset_id = input_path.name
    robot_type = get_robot_type(input_path) if group_by_robot else None

    if robot_type:
        output_path = output_dir / robot_type / dataset_id
    else:
        output_path = output_dir / dataset_id

    repo_id = f"{repo_id_prefix}/{dataset_id}"

    # 静默处理，只在出错时输出
    start_time = time.time()
    try:
        convert_dataset(
            repo_id=repo_id,
            root=str(input_path),
            output_root=str(output_path),
        )
        elapsed = time.time() - start_time
        print(f"✓ [{dataset_id}] 转换完成 ({elapsed:.1f}秒)")
        return True
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"✗ [{dataset_id}] 转换失败 ({elapsed:.1f}秒): {e}")
        logger.exception("Failed [%s] after %.1f seconds", dataset_id, elapsed)
        return False


def convert_single_wrapper(args: tuple) -> dict:
    """
    进程池包装器
    
    Args:
        args: (input_path, output_dir, repo_id_prefix, group_by_robot, index, total)
    
    Returns:
        结果字典
    """
    input_path, output_dir, repo_id_prefix, group_by_robot, index, total = args
    dataset_id = input_path.name
    
    result = {
        'dataset_id': dataset_id,
        'index': index,
        'total': total,
        'success': False,
        'elapsed': 0.0,
        'error': None,
    }
    
    start_time = time.time()
    try:
        robot_type = get_robot_type(input_path) if group_by_robot else None
        if robot_type:
            output_path = output_dir / robot_type / dataset_id
        else:
            output_path = output_dir / dataset_id
        
        repo_id = f"{repo_id_prefix}/{dataset_id}"
        
        convert_dataset(
            repo_id=repo_id,
            root=str(input_path),
            output_root=str(output_path),
        )
        
        result['success'] = True
        result['elapsed'] = time.time() - start_time
    except Exception as e:
        result['success'] = False
        result['elapsed'] = time.time() - start_time
        result['error'] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Convert LeRobot dataset(s) from v3.0 to v2.1 format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  # Single dataset (output: <output-dir>/astribot_s1/8d85f98d.../...)
  python convert.py \\
    --input /data/lerobot_v30/8d85f98d687942d28af78efea1257f32 \\
    --output-dir /data/lerobot_v21

  # Batch convert all datasets (parallel with 4 workers)
  python convert.py \\
    --input /data/lerobot_v30 \\
    --output-dir /data/lerobot_v21 \\
    --batch

  # Batch convert with 8 parallel workers
  python convert.py \\
    --input /data/lerobot_v30 \\
    --output-dir /data/lerobot_v21 \\
    --batch --workers 8

  # Flat output without robot_type grouping
  python convert.py \\
    --input /data/lerobot_v30 \\
    --output-dir /data/lerobot_v21 \\
    --batch --no-group-by-robot
""",
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to a single v3.0 dataset directory, or a parent directory containing multiple datasets (with --batch).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output base directory. Each dataset will be saved as <output-dir>/<dataset_id>/.",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Batch mode: scan --input for all v3.0 datasets and convert each one.",
    )
    parser.add_argument(
        "--repo-id-prefix",
        type=str,
        default="astribot",
        help="Repo ID prefix for the dataset (default: astribot).",
    )
    parser.add_argument(
        "--no-group-by-robot",
        action="store_true",
        help="Disable grouping by robot_type. Output directly as <output-dir>/<dataset_id>/.",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="并行工作进程数（默认: 4）",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="显示详细的转换进度和日志信息",
    )
    args = parser.parse_args()
    
    # 如果用户指定 verbose，则重新配置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        os.environ['TQDM_DISABLE'] = '0'

    input_path = Path(args.input).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not input_path.exists():
        logger.error("Input path does not exist: %s", input_path)
        sys.exit(1)

    group_by_robot = not args.no_group_by_robot
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.batch:
        # Batch mode: scan for datasets
        all_datasets = discover_datasets(input_path)
        if not all_datasets:
            print(f"❌ 未找到 v3.0 数据集: {input_path}")
            sys.exit(1)

        print(f"📦 找到 {len(all_datasets)} 个 v3.0 数据集")
        
        # 过滤掉已转换的数据集（Resume 功能）
        datasets = []
        skipped = []
        for ds_path in all_datasets:
            dataset_id = ds_path.name
            robot_type = get_robot_type(ds_path) if group_by_robot else None
            
            if is_dataset_converted(dataset_id, output_dir, robot_type):
                skipped.append(dataset_id)
            else:
                datasets.append(ds_path)
        
        if skipped:
            print(f"⏭  跳过 {len(skipped)} 个已转换的数据集")
        
        if not datasets:
            print(f"✅ 所有数据集已转换完成！")
            sys.exit(0)
        
        print(f"📋 待转换: {len(datasets)} 个数据集")
        print(f"⚙️  使用 {args.workers} 个并行工作进程")
        print(f"开始转换...\n")
        
        # 构建任务列表
        tasks = []
        for i, ds_path in enumerate(datasets):
            tasks.append((
                ds_path,
                output_dir,
                args.repo_id_prefix,
                group_by_robot,
                i + 1,
                len(datasets)
            ))
        
        # 并行处理
        succeeded, failed = 0, 0
        failed_datasets = []
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            # 提交所有任务
            futures = {executor.submit(convert_single_wrapper, task): task for task in tasks}
            
            # 收集结果
            completed = 0
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                
                if result['success']:
                    succeeded += 1
                    print(f"✓ [{completed}/{len(datasets)}] {result['dataset_id']} - 完成 ({result['elapsed']:.1f}秒)")
                else:
                    failed += 1
                    failed_datasets.append((result['dataset_id'], result['error']))
                    print(f"✗ [{completed}/{len(datasets)}] {result['dataset_id']} - 失败 ({result['elapsed']:.1f}秒)")
        
        total_elapsed = time.time() - start_time
        
        print("\n" + "=" * 80)
        print(f"批量转换完成！")
        print(f"  总数据集: {len(all_datasets)}")
        print(f"  已跳过: {len(skipped)}")
        print(f"  本次转换: {len(datasets)}")
        print(f"  成功: {succeeded}")
        print(f"  失败: {failed}")
        print(f"  总耗时: {total_elapsed:.1f} 秒 ({total_elapsed/60:.1f} 分钟)")
        if succeeded > 0:
            print(f"  平均耗时: {total_elapsed/succeeded:.1f} 秒/数据集")
        print("=" * 80)
        
        if failed_datasets:
            print("\n失败的数据集:")
            for dataset_id, error in failed_datasets[:10]:
                print(f"  - {dataset_id}: {error[:100]}")
            if len(failed_datasets) > 10:
                print(f"  ... 还有 {len(failed_datasets) - 10} 个失败")
        
        if failed > 0:
            sys.exit(1)
    else:
        # Single mode
        if not is_v30_dataset(input_path):
            print(f"❌ 不是有效的 v3.0 数据集: {input_path}")
            sys.exit(1)

        print(f"转换单个数据集: {input_path.name}")
        if not convert_single(input_path, output_dir, args.repo_id_prefix, group_by_robot):
            sys.exit(1)

    print("\n✅ 所有转换完成！")


if __name__ == "__main__":
    main()
