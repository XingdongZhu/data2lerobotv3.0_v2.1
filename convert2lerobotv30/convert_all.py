# rclone copy huawei-cloud:openloong-apps-prod-private/data-collector-svc/align/7d4237d11d9f4d8494e2b361ed68c8e1 
# /mnt/sdc/align/7d4237d11d9f4d8494e2b361ed68c8e1 --transfers=16 -P

import os
import sys
import signal
import subprocess
import shlex
import argparse
import pandas as pd
import threading
import queue
from typing import Optional
from datetime import datetime
from pathlib import Path


# 全局标志，用于标记是否收到中断信号
interrupted = False


class TaskStatusLogger:
    """任务状态记录器"""
    
    def __init__(self, log_file_path, resume=False):
        self.log_file_path = Path(log_file_path)
        self.tasks_status = {}  # task_id -> status dict
        self.lock = threading.Lock()
        
        # Resume 模式：加载已有日志
        if resume and self.log_file_path.exists():
            print(f"📝 Resume模式：加载已有日志 {log_file_path}")
            self._load_from_file()
        else:
            # 初始化新日志文件
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                f.write("=" * 100 + "\n")
                f.write(f"任务处理状态日志\n")
                f.write(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 100 + "\n\n")
    
    def _load_from_file(self):
        """从日志文件加载已有状态"""
        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 解析日志文件
            task_blocks = content.split('-' * 100)
            for block in task_blocks:
                if '任务ID:' not in block:
                    continue
                
                # 提取任务信息
                lines = block.strip().split('\n')
                task_info = {}
                current_task_id = None
                
                for line in lines:
                    line = line.strip()
                    if line.startswith('任务编号:'):
                        task_number_info = line.split(':')[1].strip()
                        if '/' in task_number_info:
                            task_info['task_number'] = int(task_number_info.split('/')[0])
                            task_info['total_tasks'] = int(task_number_info.split('/')[1])
                    elif line.startswith('任务ID:'):
                        current_task_id = line.split(':')[1].strip()
                    elif line.startswith('来源表单:'):
                        task_info['sheet_name'] = line.split(':', 1)[1].strip()
                    elif line.startswith('Robot Type:'):
                        task_info['robot_type'] = line.split(':', 1)[1].strip()
                    elif line.startswith('任务名称:'):
                        task_info['task_name'] = line.split(':', 1)[1].strip()
                    elif line.startswith('处理后文本(英文):'):
                        task_info['processed_text_en'] = line.split(':', 1)[1].strip()
                    elif line.startswith('开始时间:'):
                        task_info['start_time'] = line.split(':', 1)[1].strip()
                    elif line.startswith('结束时间:'):
                        task_info['end_time'] = line.split(':', 1)[1].strip()
                    elif line.startswith('【下载状态】:'):
                        task_info['download_status'] = line.split(':')[1].strip()
                    elif line.startswith('下载错误:'):
                        task_info['download_error'] = line.split(':', 1)[1].strip()
                    elif line.startswith('【子文件夹数量】:'):
                        task_info['subfolder_count'] = int(line.split(':')[1].strip())
                    elif line.startswith('处理动作:'):
                        task_info['count_check_action'] = line.split(':', 1)[1].strip()
                    elif line.startswith('【转换状态】:'):
                        task_info['convert_status'] = line.split(':')[1].strip()
                    elif line.startswith('转换错误:'):
                        task_info['convert_error'] = line.split(':', 1)[1].strip()
                    elif line.startswith('【删除状态】:'):
                        task_info['delete_status'] = line.split(':')[1].strip()
                    elif line.startswith('删除错误:'):
                        task_info['delete_error'] = line.split(':', 1)[1].strip()
                
                if current_task_id and task_info:
                    # 确保所有必需字段存在
                    task_info.setdefault('download_status', 'pending')
                    task_info.setdefault('download_error', None)
                    task_info.setdefault('subfolder_count', None)
                    task_info.setdefault('count_check_action', None)
                    task_info.setdefault('convert_status', 'pending')
                    task_info.setdefault('convert_error', None)
                    task_info.setdefault('delete_status', 'pending')
                    task_info.setdefault('delete_error', None)
                    task_info.setdefault('end_time', None)
                    task_info.setdefault('sheet_name', '')
                    task_info.setdefault('robot_type', '')
                    
                    self.tasks_status[current_task_id] = task_info
            
            print(f"✓ 已加载 {len(self.tasks_status)} 个任务的历史状态")
            
            # 统计已完成的任务
            completed = sum(1 for s in self.tasks_status.values() 
                          if s.get('download_status') == 'success' 
                          and s.get('convert_status') == 'success'
                          and s.get('delete_status') == 'success')
            print(f"  - 已完成: {completed}")
            print(f"  - 待处理: {len(self.tasks_status) - completed}")
            
        except Exception as e:
            print(f"⚠ 加载日志文件失败: {e}")
            print("  将以新日志模式启动")
    
    def init_task(self, task_id, task_number, total_tasks, task_name, processed_text_en, sheet_name, robot_type):
        """初始化任务状态（如果任务已存在则不覆盖）"""
        with self.lock:
            # 如果任务已存在（resume模式），则不覆盖
            if task_id not in self.tasks_status:
                self.tasks_status[task_id] = {
                    'task_number': task_number,
                    'total_tasks': total_tasks,
                    'sheet_name': sheet_name,
                    'robot_type': robot_type,
                    'task_name': task_name,
                    'processed_text_en': processed_text_en,
                    'download_status': 'pending',
                    'download_error': None,
                    'subfolder_count': None,
                    'count_check_action': None,
                    'convert_status': 'pending',
                    'convert_error': None,
                    'delete_status': 'pending',
                    'delete_error': None,
                    'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': None,
                }
                self._write_to_file()
            else:
                self.tasks_status[task_id]['sheet_name'] = sheet_name
                self.tasks_status[task_id]['robot_type'] = robot_type
                self._write_to_file()
    
    def update_download(self, task_id, success, error_msg=None):
        """更新下载状态"""
        with self.lock:
            if task_id in self.tasks_status:
                self.tasks_status[task_id]['download_status'] = 'success' if success else 'failed'
                if error_msg:
                    self.tasks_status[task_id]['download_error'] = error_msg
                self._write_to_file()
    
    def update_convert(self, task_id, success, error_msg=None):
        """更新转换状态"""
        with self.lock:
            if task_id in self.tasks_status:
                self.tasks_status[task_id]['convert_status'] = 'success' if success else 'failed'
                if error_msg:
                    self.tasks_status[task_id]['convert_error'] = error_msg
                self._write_to_file()
    
    def update_check_count(self, task_id, subfolder_count, action_taken=None):
        """更新子文件夹数量检查状态"""
        with self.lock:
            if task_id in self.tasks_status:
                self.tasks_status[task_id]['subfolder_count'] = subfolder_count
                self.tasks_status[task_id]['count_check_action'] = action_taken or 'no_action'
                self._write_to_file()
    
    def update_delete(self, task_id, success, error_msg=None):
        """更新删除状态"""
        with self.lock:
            if task_id in self.tasks_status:
                self.tasks_status[task_id]['delete_status'] = 'success' if success else 'failed'
                if error_msg:
                    self.tasks_status[task_id]['delete_error'] = error_msg
                self.tasks_status[task_id]['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self._write_to_file()
    
    def skip_task(self, task_id, reason):
        """跳过任务"""
        with self.lock:
            if task_id in self.tasks_status:
                # 只跳过未执行的步骤，保留已完成步骤的状态
                if self.tasks_status[task_id]['download_status'] == 'pending':
                    self.tasks_status[task_id]['download_status'] = 'skipped'
                self.tasks_status[task_id]['convert_status'] = 'skipped'
                self.tasks_status[task_id]['delete_status'] = 'skipped'
                self.tasks_status[task_id]['convert_error'] = reason
                self.tasks_status[task_id]['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self._write_to_file()
    
    def should_download(self, task_id):
        """判断是否需要下载（只有 success 才代表数据确实已存在，skipped/failed/pending 都需要重新下载）"""
        if task_id not in self.tasks_status:
            return True
        status = self.tasks_status[task_id].get('download_status', 'pending')
        return status != 'success'
    
    def should_convert(self, task_id):
        """判断是否需要转换（只有 success 才跳过，skipped/failed/pending 都可重试）"""
        if task_id not in self.tasks_status:
            return False  # 没有初始化，不应该转换
        status = self.tasks_status[task_id].get('convert_status', 'pending')
        return status != 'success'
    
    def should_delete(self, task_id):
        """判断是否需要删除（只有 success 才跳过，skipped/failed/pending 都可重试）"""
        if task_id not in self.tasks_status:
            return False
        status = self.tasks_status[task_id].get('delete_status', 'pending')
        return status != 'success'
    
    def is_task_completed(self, task_id):
        """判断任务是否完全完成"""
        if task_id not in self.tasks_status:
            return False
        status = self.tasks_status[task_id]
        return (status.get('download_status') == 'success' and
                status.get('convert_status') == 'success' and
                status.get('delete_status') == 'success')
    
    def _write_to_file(self):
        """将所有状态写入文件"""
        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            f.write("=" * 100 + "\n")
            f.write(f"任务处理状态日志\n")
            f.write(f"更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 100 + "\n\n")
            
            for task_id, status in self.tasks_status.items():
                f.write("-" * 100 + "\n")
                f.write(f"任务编号: {status['task_number']}/{status['total_tasks']}\n")
                f.write(f"任务ID: {task_id}\n")
                f.write(f"来源表单: {status.get('sheet_name', '')}\n")
                f.write(f"Robot Type: {status.get('robot_type', '')}\n")
                f.write(f"任务名称: {status['task_name']}\n")
                f.write(f"处理后文本(英文): {status['processed_text_en']}\n")
                f.write(f"开始时间: {status['start_time']}\n")
                if status['end_time']:
                    f.write(f"结束时间: {status['end_time']}\n")
                f.write(f"\n")
                
                # 下载状态
                f.write(f"【下载状态】: {status['download_status']}\n")
                if status['download_error']:
                    f.write(f"  下载错误: {status['download_error']}\n")
                
                # 数量检查状态
                if status.get('subfolder_count') is not None:
                    f.write(f"【子文件夹数量】: {status['subfolder_count']}\n")
                    if status.get('count_check_action'):
                        f.write(f"  处理动作: {status['count_check_action']}\n")
                
                # 转换状态
                f.write(f"【转换状态】: {status['convert_status']}\n")
                if status['convert_error']:
                    f.write(f"  转换错误: {status['convert_error']}\n")
                
                # 删除状态
                f.write(f"【删除状态】: {status['delete_status']}\n")
                if status['delete_error']:
                    f.write(f"  删除错误: {status['delete_error']}\n")
                
                f.write("-" * 100 + "\n\n")
            
            # 统计信息
            total = len(self.tasks_status)
            download_success = sum(1 for s in self.tasks_status.values() if s['download_status'] == 'success')
            convert_success = sum(1 for s in self.tasks_status.values() if s['convert_status'] == 'success')
            delete_success = sum(1 for s in self.tasks_status.values() if s['delete_status'] == 'success')
            
            f.write("=" * 100 + "\n")
            f.write("统计信息:\n")
            f.write(f"  总任务数: {total}\n")
            f.write(f"  下载成功: {download_success}/{total}\n")
            f.write(f"  转换成功: {convert_success}/{total}\n")
            f.write(f"  删除成功: {delete_success}/{total}\n")
            f.write("=" * 100 + "\n")


def signal_handler(sig, frame):
    """处理 Ctrl+C 信号"""
    global interrupted
    interrupted = True
    print("\n\n" + "=" * 80)
    print("收到中断信号 (Ctrl+C)，正在终止程序...")
    print("=" * 80)
    sys.exit(0)


def run_command(cmd, description="执行命令"):
    """
    执行shell命令，支持 Ctrl+C 中断
    
    Args:
        cmd: 要执行的命令
        description: 命令描述
        
    Returns:
        int: 命令返回码，如果被中断则返回 -1
    """
    global interrupted
    
    if interrupted:
        print("程序已被中断，跳过后续命令")
        return -1
    
    print(f"\n{description}:")
    print(f"  {cmd}")
    
    try:
        # 使用 subprocess.run 替代 os.system，可以更好地处理信号
        process = subprocess.run(
            cmd,
            shell=True,
            check=False,  # 不自动抛出异常
        )
        
        if interrupted:
            return -1
            
        return process.returncode
        
    except KeyboardInterrupt:
        print("\n命令被用户中断")
        interrupted = True
        raise
    except Exception as e:
        print(f"执行命令时出错: {e}")
        return -1


def download_task_sync(
    task_id,
    task_number,
    total_tasks,
    obs_base_path,
    local_base_path,
    rclone_config,
    logger=None,
    max_count=300,
):
    """
    下载任务数据（同步执行）
    
    Args:
        task_id: 任务ID
        task_number: 任务编号
        total_tasks: 总任务数
        obs_base_path: OBS基础路径
        local_base_path: 本地基础路径
        rclone_config: rclone配置文件路径
        logger: 状态记录器
        
    Returns:
        tuple: (success: bool, error_msg: str or None)
    """
    global interrupted
    
    if interrupted:
        error_msg = "程序被中断"
        if logger:
            logger.update_download(task_id, False, error_msg)
        return False, error_msg
    
    try:
        # 仅下载前 max_count 个子文件夹（不足则全量下载）
        remote_task_path = f"{obs_base_path}/{task_id}"
        local_task_path = f"{local_base_path}/{task_id}"
        Path(local_task_path).mkdir(parents=True, exist_ok=True)

        list_cmd = [
            "rclone", "lsf",
            "--config", rclone_config,
            remote_task_path,
            "--dirs-only",
        ]
        list_proc = subprocess.run(list_cmd, capture_output=True, text=True)
        if list_proc.returncode != 0:
            error_msg = f"列出远端子文件夹失败: {list_proc.stderr.strip() or list_proc.returncode}"
            if logger:
                logger.update_download(task_id, False, error_msg)
            return False, error_msg

        subdirs = sorted([line.strip().rstrip("/") for line in list_proc.stdout.splitlines() if line.strip()])
        if len(subdirs) == 0:
            error_msg = "远端任务目录下未找到子文件夹"
            if logger:
                logger.update_download(task_id, False, error_msg)
            return False, error_msg

        selected_subdirs = subdirs[:max_count]
        print(
            f"  [下载策略] 远端子文件夹总数: {len(subdirs)}, "
            f"MAX_COUNT: {max_count}, 将下载: {len(selected_subdirs)}"
        )

        for idx, subdir in enumerate(selected_subdirs):
            if interrupted:
                error_msg = "程序被中断"
                if logger:
                    logger.update_download(task_id, False, error_msg)
                return False, error_msg

            cmd = (
                f'rclone copy --config {shlex.quote(rclone_config)} '
                f'{shlex.quote(remote_task_path + "/" + subdir)} '
                f'{shlex.quote(local_task_path + "/" + subdir)} '
                f'--transfers=16 -P'
            )
            description = (
                f"[下载] 任务 {task_number}/{total_tasks} (task_id: {task_id}) "
                f"子目录 {idx + 1}/{len(selected_subdirs)}: {subdir}"
            )
            ret = run_command(cmd, description)
            if ret != 0:
                error_msg = f"子目录下载失败({subdir})，返回码: {ret}"
                if logger:
                    logger.update_download(task_id, False, error_msg)
                return False, error_msg

        if logger:
            logger.update_download(task_id, True)
        return True, None
    except Exception as e:
        error_msg = f"下载异常: {str(e)}"
        if logger:
            logger.update_download(task_id, False, error_msg)
        return False, error_msg


class DownloadThread(threading.Thread):
    """带返回值的下载线程"""
    def __init__(
        self,
        task_id,
        task_number,
        total_tasks,
        obs_base_path,
        local_base_path,
        rclone_config,
        logger,
        max_count,
    ):
        super().__init__()
        self.task_id = task_id
        self.task_number = task_number
        self.total_tasks = total_tasks
        self.obs_base_path = obs_base_path
        self.local_base_path = local_base_path
        self.rclone_config = rclone_config
        self.logger = logger
        self.max_count = max_count
        self.success = False
        self.error_msg = None
    
    def run(self):
        self.success, self.error_msg = download_task_sync(
            self.task_id,
            self.task_number,
            self.total_tasks,
            self.obs_base_path,
            self.local_base_path,
            self.rclone_config,
            self.logger,
            self.max_count,
        )


def check_and_limit_subfolder_count(task_path, max_count=300, logger=None, task_id=None):
    """
    检查指定路径下的子文件夹数量，并限制在最大数量以内
    
    Args:
        task_path: 任务数据路径 (如 /mnt/fastdisk/align/task_id)
        max_count: 最大允许的子文件夹数量 (默认300)
        logger: 状态记录器
        task_id: 任务ID
    
    Returns:
        tuple: (success: bool, subfolder_count: int, action_taken: str)
            - success: 是否成功执行
            - subfolder_count: 子文件夹数量
            - action_taken: 执行的动作描述
    """
    task_path = Path(task_path)
    
    # 检查路径是否存在
    if not task_path.exists():
        error_msg = f"路径不存在: {task_path}"
        print(f"  ⚠ {error_msg}")
        return False, 0, error_msg
    
    if not task_path.is_dir():
        error_msg = f"路径不是目录: {task_path}"
        print(f"  ⚠ {error_msg}")
        return False, 0, error_msg
    
    try:
        # 获取所有子文件夹和文件
        subfolders = sorted([item for item in task_path.iterdir() if item.is_dir()])
        files = sorted([item for item in task_path.iterdir() if item.is_file()])
        
        total_count = len(subfolders) + len(files)
        subfolder_count = len(subfolders)
        file_count = len(files)
        
        print(f"\n  [检查数量] 路径: {task_path}")
        print(f"    子文件夹数量: {subfolder_count}")
        print(f"    文件数量: {file_count}")
        print(f"    总计: {total_count}")
        
        # 如果总数量低于最大值，不做处理
        if total_count <= max_count:
            action_taken = f"数量合规(共{total_count}个)，无需处理"
            print(f"    ✓ {action_taken}")
            if logger and task_id:
                logger.update_check_count(task_id, total_count, action_taken)
            return True, total_count, action_taken
        
        # 数量超过最大值，需要删除多余的
        excess_count = total_count - max_count
        print(f"    ⚠ 数量超出限制 (超出{excess_count}个)，开始删除多余项...")
        
        # 合并所有项目并按名称排序
        all_items = subfolders + files
        all_items_sorted = sorted(all_items, key=lambda x: x.name)
        
        # 保留前 max_count 个，删除其余的
        items_to_keep = all_items_sorted[:max_count]
        items_to_delete = all_items_sorted[max_count:]
        
        deleted_count = 0
        failed_deletions = []
        
        for item in items_to_delete:
            try:
                if item.is_dir():
                    import shutil
                    shutil.rmtree(item)
                    print(f"      删除文件夹: {item.name}")
                else:
                    item.unlink()
                    print(f"      删除文件: {item.name}")
                deleted_count += 1
            except Exception as e:
                failed_deletions.append(f"{item.name}: {e}")
                print(f"      ✗ 删除失败: {item.name} - {e}")
        
        remaining_count = total_count - deleted_count
        
        if failed_deletions:
            action_taken = f"删除了{deleted_count}/{excess_count}个多余项，{len(failed_deletions)}个失败，剩余{remaining_count}个"
            print(f"    ⚠ {action_taken}")
        else:
            action_taken = f"删除了{deleted_count}个多余项，剩余{remaining_count}个"
            print(f"    ✓ {action_taken}")
        
        if logger and task_id:
            logger.update_check_count(task_id, remaining_count, action_taken)
        
        return True, remaining_count, action_taken
        
    except Exception as e:
        error_msg = f"检查和限制数量时出错: {str(e)}"
        print(f"  ✗ {error_msg}")
        if logger and task_id:
            logger.update_check_count(task_id, 0, error_msg)
        return False, 0, error_msg


def get_data(excel_path, sheet_name):
    """
    读取excel表格的指定表单，解析数据信息
    
    表格列结构:
        任务ID    任务名称    设备类型    设备序列号   处理后文本(中文)   处理后文本(英文)   总时长(小时)
    
    Args:
        excel_path: Excel文件路径
        sheet_name: 表单名称（如"上海"）
    
    Returns:
        list[dict]: 每行数据的字典列表，键名为表头
    """
    try:
        # 读取Excel文件指定的sheet
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        # 显示表格基本信息
        print(f"\n读取表单: {sheet_name}")
        print(f"总行数: {len(df)}")
        print(f"列名: {list(df.columns)}")
        
        # 去除完全空的行
        df = df.dropna(how='all')
        print(f"去除空行后行数: {len(df)}\n")
        
        # 转换为字典列表，每行是一个字典
        data_list = []
        for idx, row in df.iterrows():
            record = {
                '任务ID': row.get('任务ID', ''),
                '任务名称': row.get('任务名称', ''),
                '设备类型': row.get('设备名称', ''),
                '处理后文本(中文)': row.get('步骤(处理后)', ''),
                '处理后文本(英文)': row.get('步骤(英文)', ''),
            }
            data_list.append(record)
        
        return data_list
        
    except Exception as e:
        print(f"读取Excel文件出错: {e}")
        raise


if __name__ == "__main__":
    # 注册信号处理器，捕获 Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(
        description="数据转换批处理脚本（Dwheel 等，见脚本内 Excel/路径配置）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
任务范围（序号相对「筛选后的任务清单」，从 1 开始）:
  可与 --resume 同时使用。仅处理 [task-from, task-to] 闭区间内的任务；
  日志中「任务编号: X/Y」的 Y 为清单总任务数，X 为全局序号（与 Excel 筛选后顺序一致）。

示例:
  python convert_all.py --task-from 10 --task-to 60
  python convert_all.py -r --task-from 10 --task-to 60

注意: 若使用子区间且状态日志文件已存在，非 resume 运行会报错；请先 --resume，或删除/移走日志后再跑。
        """.strip(),
    )
    parser.add_argument(
        "--resume",
        "-r",
        action="store_true",
        help="Resume：从状态日志恢复，跳过已完成的步骤",
    )
    parser.add_argument(
        "--task-from",
        type=int,
        default=1,
        metavar="N",
        help="本批起始任务序号（含），默认 1",
    )
    parser.add_argument(
        "--task-to",
        type=int,
        default=None,
        metavar="N",
        help="本批结束任务序号（含）；默认处理到最后一个任务",
    )
    args = parser.parse_args()
    resume_mode = args.resume
    task_from = args.task_from
    task_to_arg = args.task_to

    # 根据region 获取 obs_base_path 和 rclone_config
    OBS_RCLONE_CONFIG = {
        "郑州": {
            "obs_base_path": 'huawei-cloud:openloong-zhengzhou-apps-private/data-collector-svc/align',
            "rclone_config": '/root/.config/rclone/rclone_zhengzhou.conf'
        },
        "上海": {
            "obs_base_path": 'huawei-cloud:openloong-apps-prod-private/data-collector-svc/align',
            "rclone_config": '/root/.config/rclone/rclone_shanghai.conf'
        },
    }

    excel_path = "/mnt/fastdisk/convert2lerobotv30/数据转换第三批次20260513测试版本.xlsx"
    # sheet name 获取 robot_types 和 region
    # 需要转换的sheet name，和 sheet name下需要转换的机型
    EXCEL_CONFIG = {
        "郑州平台": {
            "robot_types": [
                "智元G1",
                "青龙ROS2",
                "智元A2",
                "乐聚KUAVO",
                "傅利叶GR2",
                "Dwheel",
            ],
            "region": "郑州",
        },
        "重点研发-上海": {
            "robot_types": ["智元G1"],
            "region": "上海",
        },
        "模型内部需求-上海": {
            "robot_types": [    
                "智元A2",
                "方舟无限arx-acone",
                "傅利叶GR2",
                "智元G1",
                "星尘智能S1",
                "UR5e",
                "天机",
                "Dwheel",
                "松灵Aloha",
                "乐聚KUAVO",
                "星海图R1",
                "Franka FR3",
            ],
            "region": "上海",
        },
    }

    local_base_path = '/mnt/fastdisk/align'
    output_base_path = '/mnt/fastdisk/lerobotv30'
    output_base_path_map = {}
    # 根据表格的sheet name新建输出目录
    for key in EXCEL_CONFIG.keys():
        path = os.path.join(output_base_path, key)
        os.makedirs(path, exist_ok=True)
        output_base_path_map[key] = path

    # 转换脚本路径
    scripts_base_path = "/mnt/fastdisk/convert2lerobotv30"
    # 默认转换脚本，11个机型
    # !!!青龙机型需要根据机器编号来选择转换脚本，当作2个机型!!!
    DEFAULT_CONVERT_SCRIPTS = {
        "方舟无限arx-acone": scripts_base_path + "/arx_loong_align2lerobotv30.py",
        "星尘智能S1": scripts_base_path + "/astribot_s1_align2lerobot_v30_no_norm.py",
        "松灵Aloha": scripts_base_path + "/aloha_align2lerobot_v30_no_norm.py",
        "UR5e": scripts_base_path + "/ur5e_align2lerobot_v30_no_norm.py",
        "Dwheel": scripts_base_path + "/Dwheel_align2lerobotv30.py",
        "Franka FR3": scripts_base_path + "/fr3_align2lerobotv30.py",
        "傅利叶GR2": scripts_base_path + "/gr2_align2lerobot_v30_no_norm.py",
        "傅利叶GR-2": scripts_base_path + "/gr2_align2lerobot_v30_no_norm.py",
        "乐聚KUAVO": scripts_base_path + "/leju_align2lerobot_v30_no_norm.py",
        "天机": scripts_base_path + "/TIANJI_align2lerobotv30.py",
        "星海图R1": scripts_base_path + "/R1_align2lerobot_v30_no_norm.py",
        "智元A2": scripts_base_path + "/ZhiYuanA2_align2lerobotv30.py",
        "青龙ROS1": scripts_base_path + "/qinglongros1_align2lerobot_v30_no_norm.py",
        "青龙ROS2": scripts_base_path + "/qinglongros2_align2lerobot_v30_no_norm.py",
        "灵龙-h": scripts_base_path + "/linglong_h_align2lerobotv30.py",
    }

    # 智元G1的转换脚本，2个地区，当作2个机型
    G1_SCRIPTS = {
        "郑州": scripts_base_path + "/zhengzhou_zhiyuan_G1_align2lerobotv30.py",
        "上海": scripts_base_path + "/Genie1_align2lerobotv30.py",
    }

    MAX_COUNT = 2 # 根据taskID 读取桶上数量，只下载 MAX_COUNT 条eposide
    log_file_path = '/mnt/fastdisk/convert2lerobotv30/convert_all_status.txt'

    def get_convert_script(robot_type, region):
        if robot_type == "智元G1":
            return G1_SCRIPTS[region]
        return DEFAULT_CONVERT_SCRIPTS[robot_type]

    try:
        # 按 EXCEL_CONFIG 中的 sheet 配置读取所有表单，并筛选出待处理任务。
        all_target_tasks = []
        for sheet_name, sheet_config in EXCEL_CONFIG.items():
            robot_types = sheet_config["robot_types"]
            region = sheet_config["region"]
            obs_config = OBS_RCLONE_CONFIG[region]
            sheet_output_base_path = output_base_path_map[sheet_name]
            robot_type_label = "_".join(robot_types)

            print("\n" + "=" * 80)
            print(f"读取配置表单: {sheet_name}")
            print(f"  机型: {robot_type_label}")
            print(f"  region: {region}")
            print(f"  输出目录: {sheet_output_base_path}")
            print("=" * 80)

            data = get_data(excel_path, sheet_name)
            sheet_task_count = 0
            for record in data:
                robot_type = record['设备类型']
                if robot_type not in robot_types: #and record['组帧失败次数'] == 0 and record['平台'] == '郑州'
                    continue

                task_record = dict(record)
                task_record["_sheet_name"] = sheet_name
                task_record["_region"] = region
                task_record["_obs_base_path"] = obs_config["obs_base_path"]
                task_record["_rclone_config"] = obs_config["rclone_config"]
                task_record["_output_base_path"] = sheet_output_base_path
                task_record["_convert_script"] = get_convert_script(robot_type, region)
                all_target_tasks.append(task_record)
                sheet_task_count += 1

            print(f"表单 {sheet_name} 筛选出 {sheet_task_count} 个待处理任务")

        global_total_tasks = len(all_target_tasks)
        task_to = task_to_arg if task_to_arg is not None else global_total_tasks

        if global_total_tasks == 0:
            print("没有需要处理的任务")
            sys.exit(0)
        if task_from < 1 or task_from > global_total_tasks:
            print(f"错误: --task-from 必须在 1～{global_total_tasks} 之间，当前为 {task_from}")
            sys.exit(1)
        if task_to < task_from or task_to > global_total_tasks:
            print(
                f"错误: --task-to 必须满足 {task_from} ≤ --task-to ≤ {global_total_tasks}，当前为 {task_to}"
            )
            sys.exit(1)

        target_tasks = all_target_tasks[task_from - 1 : task_to]
        batch_total = len(target_tasks)

        print(f"\n全部表单筛选后清单共 {global_total_tasks} 个任务")
        print(f"本批处理范围: 第 {task_from} 个 → 第 {task_to} 个（共 {batch_total} 个）")
        print("=" * 80)
        print("提示: 按 Ctrl+C 可以随时终止程序")
        print("提示: 使用流水线并行模式 - 下载和转换并行执行")
        print("=" * 80)

        if batch_total == 0:
            print("本批没有需要处理的任务")
            sys.exit(0)

        # 部分区间 + 非 resume + 已有日志：避免先清空日志再校验区间；须显式 --resume 或先删除/移走日志
        partial_range = task_from != 1 or task_to != global_total_tasks
        if partial_range and not resume_mode and Path(log_file_path).is_file():
            print(
                "错误: 已指定任务子区间 (--task-from/--task-to)，但未使用 --resume，且状态日志文件已存在。"
            )
            print(
                "  请使用 --resume 在原有日志上继续跑该区间，或先备份/删除日志文件后再非 resume 运行。"
            )
            print(f"  日志: {log_file_path}")
            sys.exit(1)

        logger = TaskStatusLogger(log_file_path, resume=resume_mode)
        print(f"状态日志文件: {log_file_path}")
        if resume_mode:
            print("🔄 Resume 模式已启动，将跳过已完成的任务步骤")
        
        pipeline_buffer_size = 3  # 流水线缓冲区大小
        
        # 初始化本批任务状态（任务编号为清单中的全局序号，分母为清单总任务数）
        for i, record in enumerate(target_tasks):
            logger.init_task(
                task_id=record['任务ID'],
                task_number=task_from + i,
                total_tasks=global_total_tasks,
                task_name=record['任务名称'],
                processed_text_en=record['处理后文本(英文)'],
                sheet_name=record['_sheet_name'],
                robot_type=record['设备类型'],
            )
        
        # 阶段1: 预下载待处理任务（跳过已完成的，找到前 pipeline_buffer_size 个需要处理的任务并下载）
        print("\n" + "=" * 80)
        print(f"阶段1: 预下载待处理任务 (缓冲大小: {pipeline_buffer_size})")
        print("=" * 80)
        
        pre_download_count = 0
        next_download_idx = 0
        
        for i in range(batch_total):
            if interrupted:
                break
            if pre_download_count >= pipeline_buffer_size:
                break
            
            task_id = target_tasks[i]['任务ID']
            next_download_idx = i + 1
            
            # 已完全完成的任务跳过，不占缓冲名额
            if logger.is_task_completed(task_id):
                continue
            
            # 已下载成功但还没转换，占一个缓冲名额（无需重新下载）
            if not logger.should_download(task_id):
                print(f"⏭ 任务 {task_from + i}/{global_total_tasks} (ID: {task_id}) 已下载，跳过下载")
                pre_download_count += 1
                continue
            
            # 需要下载（pending 或 failed）
            success, error_msg = download_task_sync(
                task_id, task_from + i, global_total_tasks,
                target_tasks[i]["_obs_base_path"], local_base_path, target_tasks[i]["_rclone_config"], logger, MAX_COUNT
            )
            if not success or interrupted:
                if error_msg:
                    print(f"下载失败: {error_msg}")
                if interrupted:
                    break
            else:
                task_path = Path(local_base_path) / task_id
                check_and_limit_subfolder_count(task_path, max_count=MAX_COUNT, logger=logger, task_id=task_id)
                # 下载成功后，重置之前被 skip 的转换和删除状态
                with logger.lock:
                    st = logger.tasks_status.get(task_id, {})
                    if st.get('convert_status') == 'skipped':
                        st['convert_status'] = 'pending'
                    if st.get('delete_status') == 'skipped':
                        st['delete_status'] = 'pending'
                logger._write_to_file()
                pre_download_count += 1
        
        print(f"\n预下载完成: {pre_download_count} 个任务已就绪, next_download_idx={next_download_idx}")
        
        if interrupted:
            print("\n程序已被中断，退出")
            sys.exit(0)
        
        # 阶段2: 流水线处理
        print("\n" + "=" * 80)
        print("阶段2: 流水线处理模式 (转换当前任务 + 下载后续任务)")
        print("=" * 80)
        
        download_thread: Optional[DownloadThread] = None
        # next_download_idx 已在阶段1中设置
        
        for i in range(batch_total):
            if interrupted:
                print("\n程序已被中断，退出循环")
                break
            
            global_task_num = task_from + i
            record = target_tasks[i]
            task_id = record['任务ID']
            processed_text_en = record['处理后文本(英文)']
            
            print("\n" + "=" * 80)
            print(f"处理进度: {global_task_num}/{global_total_tasks}（本批第 {i + 1}/{batch_total} 个）")
            print(f"  任务ID: {task_id}")
            print(f"  表单: {record['_sheet_name']}")
            print(f"  region: {record['_region']}")
            print(f"  设备类型: {record['设备类型']}")
            print(f"  处理后文本(英文): {processed_text_en}")
            
            # Resume模式：检查任务是否已完全完成
            if logger.is_task_completed(task_id):
                print("  ✅ 任务已完成（下载、转换、删除），跳过")
                print("=" * 80)
                continue
            
            print("=" * 80)
            
            # 如果有下载线程在运行，等待它完成
            if download_thread is not None and download_thread.is_alive():
                print(f"\n[等待] 等待后续任务下载完成...")
                download_thread.join()
                download_thread = None
            
            # Resume模式：如果当前任务需要（重新）下载，先同步下载
            if logger.should_download(task_id):
                print(f"\n[重新下载] 任务 {global_task_num}/{global_total_tasks} (ID: {task_id})")
                dl_success, dl_error = download_task_sync(
                    task_id, global_task_num, global_total_tasks,
                    record["_obs_base_path"], local_base_path, record["_rclone_config"], logger, MAX_COUNT
                )
                if dl_success:
                    task_path_dl = Path(local_base_path) / task_id
                    check_and_limit_subfolder_count(task_path_dl, max_count=MAX_COUNT, logger=logger, task_id=task_id)
                    # 下载成功后，重置之前被 skip 的转换和删除状态
                    with logger.lock:
                        st = logger.tasks_status.get(task_id, {})
                        if st.get('convert_status') == 'skipped':
                            st['convert_status'] = 'pending'
                        if st.get('delete_status') == 'skipped':
                            st['delete_status'] = 'pending'
                    logger._write_to_file()
                if interrupted:
                    break
            
            # 检查当前任务的下载状态
            download_success = logger.tasks_status[task_id]['download_status'] == 'success'
            if not download_success:
                print(f"⚠ 任务 {task_id} 下载失败或未完成，跳过转换和删除")
                logger.skip_task(task_id, "下载未成功，跳过后续步骤")
                continue
            
            # 下载成功后，检查并限制子文件夹数量
            task_path = Path(local_base_path) / task_id
            check_success, subfolder_count, action_taken = check_and_limit_subfolder_count(
                task_path, max_count=MAX_COUNT, logger=logger, task_id=task_id
            )
            
            # 无论数量多少，都继续进行转换和删除
            # 开始转换当前任务，同时在后台下载下一个待处理任务
            # 跳过已完成的任务，找到真正需要下载的
            while next_download_idx < batch_total and logger.is_task_completed(target_tasks[next_download_idx]['任务ID']):
                next_download_idx += 1
            
            if next_download_idx < batch_total:
                next_task_id = target_tasks[next_download_idx]['任务ID']
                if logger.should_download(next_task_id):
                    next_record = target_tasks[next_download_idx]
                    download_thread = DownloadThread(
                        task_id=next_task_id,
                        task_number=task_from + next_download_idx,
                        total_tasks=global_total_tasks,
                        obs_base_path=next_record["_obs_base_path"],
                        local_base_path=local_base_path,
                        rclone_config=next_record["_rclone_config"],
                        logger=logger,
                        max_count=MAX_COUNT,
                    )
                    download_thread.start()
                next_download_idx += 1
            
            # 执行转换命令（在主线程）
            convert_success = False
            convert_error = None
            
            # Resume模式：检查是否需要转换
            if not logger.should_convert(task_id):
                print(f"⏭ 任务 {task_id} 已转换，跳过转换步骤")
                convert_success = True  # 标记为成功以便继续后续步骤
            else:
                try:
                    convert_script = record["_convert_script"]
                    task_output_base_path = record["_output_base_path"]
                    cmd = f'SVT_LOG=1 python3 {convert_script} --input {local_base_path}/{task_id} --output {task_output_base_path}/{task_id} --task {shlex.quote(processed_text_en)}'
                    ret = run_command(cmd, f"[转换] 任务 {global_task_num}/{global_total_tasks}")
                    
                    if ret == 0:
                        convert_success = True
                        logger.update_convert(task_id, True)
                        print(f"✓ 转换成功")
                    elif ret == -1:
                        convert_error = "程序被中断"
                        logger.update_convert(task_id, False, convert_error)
                        if interrupted:
                            break
                    else:
                        convert_error = f"转换命令返回码: {ret}"
                        logger.update_convert(task_id, False, convert_error)
                        print(f"✗ 转换失败: {convert_error}")
                except Exception as e:
                    convert_error = f"转换异常: {str(e)}"
                    logger.update_convert(task_id, False, convert_error)
                    print(f"✗ 转换异常: {e}")

            # 执行上传命令
            # TODO: 添加上传命令

            # 只有转换成功才执行删除命令
            if convert_success:
                # Resume模式：检查是否需要删除
                if not logger.should_delete(task_id):
                    print(f"⏭ 任务 {task_id} 已删除，跳过删除步骤")
                else:
                    try:
                        cmd = f'rm -rf {local_base_path}/{task_id}'
                        ret = run_command(cmd, f"[清理] 任务 {global_task_num}/{global_total_tasks}")
                        
                        if ret == 0:
                            logger.update_delete(task_id, True)
                            print(f"✓ 删除成功")
                        elif ret == -1:
                            delete_error = "程序被中断"
                            logger.update_delete(task_id, False, delete_error)
                            if interrupted:
                                break
                        else:
                            delete_error = f"删除命令返回码: {ret}"
                            logger.update_delete(task_id, False, delete_error)
                            print(f"✗ 删除失败: {delete_error}")
                    except Exception as e:
                        delete_error = f"删除异常: {str(e)}"
                        logger.update_delete(task_id, False, delete_error)
                        print(f"✗ 删除异常: {e}")
            else:
                # 转换失败，不删除原数据
                logger.update_delete(task_id, False, "转换未成功，保留原数据")
                print(f"⚠ 转换失败，保留原数据: {local_base_path}/{task_id}")
            
            print(f"\n{'✓' if convert_success else '✗'} 任务 {global_task_num}/{global_total_tasks} {'完成' if convert_success else '失败'}")
        
        # 等待最后一个下载线程完成
        if download_thread is not None and download_thread.is_alive():
            print(f"\n[等待] 等待最后的下载任务完成...")
            download_thread.join()
        
        # 生成最终统计信息
        download_success_count = sum(1 for s in logger.tasks_status.values() if s['download_status'] == 'success')
        convert_success_count = sum(1 for s in logger.tasks_status.values() if s['convert_status'] == 'success')
        delete_success_count = sum(1 for s in logger.tasks_status.values() if s['delete_status'] == 'success')
        
        if not interrupted:
            print("\n" + "=" * 80)
            print(f"所有任务处理完成！")
            print("=" * 80)
        else:
            print("\n" + "=" * 80)
            print(f"程序被中断")
            print("=" * 80)
        
        print(f"\n最终统计:")
        print(f"  清单总任务数: {global_total_tasks}")
        print(f"  本批任务范围: 第 {task_from} - {task_to} 个（本批 {batch_total} 个）")
        print(f"  日志中任务记录数: {len(logger.tasks_status)}")
        print(f"  下载成功(日志内): {download_success_count}")
        print(f"  转换成功(日志内): {convert_success_count}")
        print(f"  删除成功(日志内): {delete_success_count}")
        print(f"\n详细状态已保存到: {log_file_path}")
        print("=" * 80)
            
    except KeyboardInterrupt:
        print("\n\n" + "=" * 80)
        print("程序被用户中断 (Ctrl+C)")
        print("=" * 80)
        print(f"\n详细状态已保存到: {log_file_path}")
        sys.exit(0)
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        if 'log_file_path' in locals():
            print(f"\n详细状态已保存到: {log_file_path}")
        sys.exit(1)
    finally:
        # 确保日志文件最终状态已写入
        if 'logger' in locals():
            print(f"\n最终日志已保存: {log_file_path}")
