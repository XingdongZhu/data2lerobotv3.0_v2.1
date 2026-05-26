#!/usr/bin/env python3
"""
LeRobot v3.0 数据集统计分析脚本

功能:
    - 统计数据格式版本
    - 统计不同机器人类型
    - 统计任务数量和 episodes 数量
    - 统计帧数、FPS、相机类型等信息
    - 生成详细的统计报告

用法:
    python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai
    python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai --output report.txt
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List


def load_dataset_info(dataset_path: Path) -> Dict[str, Any]:
    """
    加载单个数据集的 info.json
    
    Args:
        dataset_path: 数据集路径（如 /path/to/task_id/）
    
    Returns:
        info.json 的内容字典，如果加载失败则返回 None
    """
    info_file = dataset_path / "meta" / "info.json"
    
    if not info_file.exists():
        return None
    
    try:
        with open(info_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠ 加载 {info_file} 失败: {e}")
        return None


def extract_cameras_from_features(features: dict) -> List[str]:
    """从 features 中提取相机列表"""
    cameras = []
    for key in features.keys():
        if key.startswith("observation.images."):
            cam_name = key.replace("observation.images.", "")
            cameras.append(cam_name)
    return sorted(cameras)


def analyze_lerobot_datasets(base_dir: Path) -> Dict[str, Any]:
    """
    分析 LeRobot 数据集目录
    
    Args:
        base_dir: 数据集根目录（如 /mnt/fastdisk/lerobotv3_shanghai）
    
    Returns:
        统计信息字典
    """
    stats = {
        'total_datasets': 0,
        'failed_datasets': 0,
        'versions': defaultdict(int),
        'robot_types': defaultdict(int),
        'robot_type_details': defaultdict(lambda: {
            'count': 0,
            'total_episodes': 0,
            'total_frames': 0,
            'total_tasks': 0,
            'total_duration_seconds': 0.0,
            'datasets': []
        }),
        'total_episodes_all': 0,
        'total_frames_all': 0,
        'total_tasks_all': 0,
        'total_duration_seconds_all': 0.0,
        'fps_distribution': defaultdict(int),
        'cameras_distribution': defaultdict(int),
        'state_action_dims': defaultdict(int),
        'image_resolutions': defaultdict(int),
        'video_codecs': defaultdict(int),
        'datasets_info': [],
    }
    
    # 遍历所有子目录
    subdirs = sorted([d for d in base_dir.iterdir() if d.is_dir()])
    
    print(f"📂 扫描目录: {base_dir}")
    print(f"找到 {len(subdirs)} 个子目录\n")
    print("=" * 100)
    
    for i, dataset_dir in enumerate(subdirs, 1):
        info = load_dataset_info(dataset_dir)
        
        if info is None:
            stats['failed_datasets'] += 1
            print(f"[{i}/{len(subdirs)}] ✗ {dataset_dir.name} - 无法加载 info.json")
            continue
        
        stats['total_datasets'] += 1
        
        # 基本信息
        version = info.get('codebase_version', 'unknown')
        robot_type = info.get('robot_type', 'unknown')
        total_episodes = info.get('total_episodes', 0)
        total_frames = info.get('total_frames', 0)
        total_tasks = info.get('total_tasks', 0)
        fps = info.get('fps', 0)
        
        # 计算时长（秒）
        duration_seconds = total_frames / fps if fps > 0 else 0
        
        stats['versions'][version] += 1
        stats['robot_types'][robot_type] += 1
        stats['total_episodes_all'] += total_episodes
        stats['total_frames_all'] += total_frames
        stats['total_tasks_all'] += total_tasks
        stats['total_duration_seconds_all'] += duration_seconds
        stats['fps_distribution'][fps] += 1
        
        # 机器人类型详细信息
        robot_detail = stats['robot_type_details'][robot_type]
        robot_detail['count'] += 1
        robot_detail['total_episodes'] += total_episodes
        robot_detail['total_frames'] += total_frames
        robot_detail['total_tasks'] += total_tasks
        robot_detail['total_duration_seconds'] += duration_seconds
        robot_detail['datasets'].append(dataset_dir.name)
        
        # 提取相机信息
        features = info.get('features', {})
        cameras = extract_cameras_from_features(features)
        cameras_str = ','.join(cameras)
        stats['cameras_distribution'][cameras_str] += 1
        
        # State/Action 维度
        state_shape = features.get('observation.state', {}).get('shape', [])
        action_shape = features.get('action', {}).get('shape', [])
        state_dim = state_shape[0] if state_shape else 0
        action_dim = action_shape[0] if action_shape else 0
        dim_str = f"state:{state_dim},action:{action_dim}"
        stats['state_action_dims'][dim_str] += 1
        
        # 图像分辨率
        for cam in cameras:
            cam_key = f"observation.images.{cam}"
            if cam_key in features:
                shape = features[cam_key].get('shape', [])
                if len(shape) >= 2:
                    resolution = f"{shape[0]}x{shape[1]}"
                    stats['image_resolutions'][resolution] += 1
                    
                    # 视频编码器
                    cam_info = features[cam_key].get('info', {})
                    codec = cam_info.get('video.codec', 'unknown')
                    stats['video_codecs'][codec] += 1
        
        # 保存数据集详细信息
        dataset_info = {
            'task_id': dataset_dir.name,
            'version': version,
            'robot_type': robot_type,
            'episodes': total_episodes,
            'frames': total_frames,
            'tasks': total_tasks,
            'duration_seconds': duration_seconds,
            'duration_hours': duration_seconds / 3600,
            'fps': fps,
            'cameras': cameras,
            'state_dim': state_dim,
            'action_dim': action_dim,
        }
        stats['datasets_info'].append(dataset_info)
        
        print(f"[{i}/{len(subdirs)}] ✓ {dataset_dir.name} - {robot_type} - {total_episodes} episodes, {total_frames} frames, {total_tasks} tasks, {duration_seconds/60:.1f}分钟")
    
    return stats


def print_statistics(stats: Dict[str, Any], output_file: Path = None):
    """打印统计信息"""
    
    lines = []
    
    def add_line(line=""):
        lines.append(line)
        print(line)
    
    add_line("\n" + "=" * 100)
    add_line("LeRobot 数据集统计分析报告")
    add_line("=" * 100)
    
    # 1. 总体统计
    add_line("\n【总体统计】")
    add_line("-" * 100)
    add_line(f"  总数据集数量: {stats['total_datasets']}")
    add_line(f"  加载失败数量: {stats['failed_datasets']}")
    add_line(f"  总 Episodes 数: {stats['total_episodes_all']:,}")
    add_line(f"  总 Tasks 数: {stats['total_tasks_all']:,}")
    add_line(f"  总帧数: {stats['total_frames_all']:,}")
    
    # 计算总时长
    total_duration_hours = stats['total_duration_seconds_all'] / 3600
    total_duration_minutes = stats['total_duration_seconds_all'] / 60
    add_line(f"  总时长: {total_duration_hours:.2f} 小时 ({total_duration_minutes:.1f} 分钟, {stats['total_duration_seconds_all']:.1f} 秒)")
    
    add_line(f"  平均每个数据集的 Episodes: {stats['total_episodes_all'] / max(stats['total_datasets'], 1):.1f}")
    add_line(f"  平均每个 Episode 的帧数: {stats['total_frames_all'] / max(stats['total_episodes_all'], 1):.1f}")
    add_line(f"  平均每个 Episode 的时长: {stats['total_duration_seconds_all'] / max(stats['total_episodes_all'], 1):.1f} 秒")
    
    # 2. 数据格式版本
    add_line("\n【数据格式版本】")
    add_line("-" * 100)
    for version, count in sorted(stats['versions'].items()):
        percentage = (count / stats['total_datasets'] * 100) if stats['total_datasets'] > 0 else 0
        add_line(f"  {version}: {count} 个数据集 ({percentage:.1f}%)")
    
    # 3. 机器人类型统计
    add_line("\n【机器人类型统计】")
    add_line("-" * 100)
    for robot_type, count in sorted(stats['robot_types'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / stats['total_datasets'] * 100) if stats['total_datasets'] > 0 else 0
        detail = stats['robot_type_details'][robot_type]
        duration_hours = detail['total_duration_seconds'] / 3600
        add_line(f"  {robot_type}:")
        add_line(f"    - 数据集数量: {count} ({percentage:.1f}%)")
        add_line(f"    - 总 Episodes: {detail['total_episodes']:,}")
        add_line(f"    - 总 Tasks: {detail['total_tasks']:,}")
        add_line(f"    - 总帧数: {detail['total_frames']:,}")
        add_line(f"    - 总时长: {duration_hours:.2f} 小时 ({detail['total_duration_seconds'] / 60:.1f} 分钟)")
        add_line(f"    - 平均 Episodes/数据集: {detail['total_episodes'] / max(count, 1):.1f}")
        add_line(f"    - 平均帧数/Episode: {detail['total_frames'] / max(detail['total_episodes'], 1):.1f}")
        add_line(f"    - 平均时长/Episode: {detail['total_duration_seconds'] / max(detail['total_episodes'], 1):.1f} 秒")
    
    # 4. FPS 分布
    add_line("\n【FPS 分布】")
    add_line("-" * 100)
    for fps, count in sorted(stats['fps_distribution'].items()):
        percentage = (count / stats['total_datasets'] * 100) if stats['total_datasets'] > 0 else 0
        add_line(f"  {fps} FPS: {count} 个数据集 ({percentage:.1f}%)")
    
    # 5. 相机配置
    add_line("\n【相机配置】")
    add_line("-" * 100)
    for cameras, count in sorted(stats['cameras_distribution'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / stats['total_datasets'] * 100) if stats['total_datasets'] > 0 else 0
        add_line(f"  [{cameras}]: {count} 个数据集 ({percentage:.1f}%)")
    
    # 6. 图像分辨率
    add_line("\n【图像分辨率】")
    add_line("-" * 100)
    for resolution, count in sorted(stats['image_resolutions'].items(), key=lambda x: x[1], reverse=True):
        add_line(f"  {resolution}: {count} 个相机流")
    
    # 7. State/Action 维度
    add_line("\n【State/Action 维度】")
    add_line("-" * 100)
    for dim_str, count in sorted(stats['state_action_dims'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / stats['total_datasets'] * 100) if stats['total_datasets'] > 0 else 0
        add_line(f"  {dim_str}: {count} 个数据集 ({percentage:.1f}%)")
    
    # 8. 视频编码器
    add_line("\n【视频编码器】")
    add_line("-" * 100)
    for codec, count in sorted(stats['video_codecs'].items(), key=lambda x: x[1], reverse=True):
        add_line(f"  {codec}: {count} 个视频流")
    
    # 9. 各机器人类型的数据集列表（只显示前5个）
    add_line("\n【各机器人类型的数据集（每类最多显示5个）】")
    add_line("-" * 100)
    for robot_type in sorted(stats['robot_types'].keys()):
        detail = stats['robot_type_details'][robot_type]
        add_line(f"  {robot_type} ({len(detail['datasets'])} 个):")
        for dataset_name in detail['datasets'][:5]:
            add_line(f"    - {dataset_name}")
        if len(detail['datasets']) > 5:
            add_line(f"    ... 还有 {len(detail['datasets']) - 5} 个数据集")
    
    add_line("\n" + "=" * 100)
    
    # 保存到文件
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"\n✓ 统计报告已保存到: {output_file}")


def export_detailed_csv(stats: Dict[str, Any], output_file: Path):
    """导出详细的 CSV 文件"""
    import csv
    
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        
        # 写入表头
        writer.writerow([
            '任务ID', '数据格式版本', '机器人类型', 'Episodes数量', '任务数',
            '帧数', 'FPS', '时长(秒)', '时长(分钟)', '时长(小时)',
            '相机列表', 'State维度', 'Action维度'
        ])
        
        # 写入数据
        for info in stats['datasets_info']:
            writer.writerow([
                info['task_id'],
                info['version'],
                info['robot_type'],
                info['episodes'],
                info['tasks'],
                info['frames'],
                info['fps'],
                f"{info['duration_seconds']:.1f}",
                f"{info['duration_seconds'] / 60:.2f}",
                f"{info['duration_hours']:.4f}",
                ','.join(info['cameras']),
                info['state_dim'],
                info['action_dim'],
            ])
    
    print(f"✓ 详细数据已导出到: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="LeRobot v3.0 数据集统计分析工具"
    )
    
    parser.add_argument("data_dir", type=Path, nargs='?',
                        default=Path("/mnt/fastdisk/lerobotv3_shanghai"),
                        help="LeRobot 数据集根目录")
    parser.add_argument("--output", "-o", type=Path, default=None,
                        help="统计报告输出文件路径（txt格式）")
    parser.add_argument("--csv", type=Path, default=None,
                        help="详细数据导出文件路径（csv格式）")
    
    args = parser.parse_args()
    
    # 检查目录是否存在
    if not args.data_dir.exists():
        print(f"❌ 错误: 目录不存在 - {args.data_dir}")
        sys.exit(1)
    
    if not args.data_dir.is_dir():
        print(f"❌ 错误: 路径不是目录 - {args.data_dir}")
        sys.exit(1)
    
    # 分析数据集
    print("🔍 开始分析数据集...")
    print()
    
    stats = analyze_lerobot_datasets(args.data_dir)
    
    # 打印统计信息
    print("\n" + "=" * 100)
    print_statistics(stats, args.output)
    
    # 导出 CSV（如果指定）
    if args.csv:
        export_detailed_csv(stats, args.csv)
    
    print("\n✅ 分析完成！")


if __name__ == "__main__":
    main()
