# convert_all.py - 机器人数据批量转换工具

一个用于批量下载、转换和管理机器人数据集的流水线处理工具。支持从云端下载数据、转换为 LeRobot v3.0 格式，并具有 Resume 功能。

## 功能特性

### 🚀 核心功能
- **批量数据处理**：从 Excel 表格读取任务列表，批量处理多个数据集
- **云端下载**：使用 rclone 从云存储（华为云 OBS）下载数据
- **数据转换**：将原始机器人数据转换为 LeRobot v3.0 标准格式
- **自动清理**：转换成功后自动删除原始数据，节省磁盘空间

### ⚡ 性能优化
- **流水线并行**：下载和转换同时进行，充分利用 I/O 和 CPU 资源
- **智能缓冲**：预下载 3 个任务，确保转换时数据已就绪
- **子文件夹限制**：自动检查并限制每个任务的数据量（最多 300 个子文件夹）

### 🔄 Resume 功能
- **断点续传**：程序中断后可从上次位置继续执行
- **智能跳过**：自动跳过已完成的下载、转换、删除步骤
- **状态日志**：详细记录每个任务的处理状态

### 📊 状态监控
- **实时日志**：生成详细的状态日志文件，记录每个任务的执行情况
- **统计信息**：显示下载、转换、删除的成功率
- **错误追踪**：记录详细的错误信息，便于问题排查

## 系统要求

### 依赖项
```bash
pip install pandas openpyxl
```

### 外部工具
- **rclone**：用于云端数据下载
- **转换脚本**：针对不同机器人类型的转换脚本
  - `ginie1_align2lerobot_v30.py` - 智元 G1
  - `R1_align2lerobot_v30.py` - 星海图 R1
  - `leju_align2lerobot_v30.py` - 乐聚夸父
  - `qinglongros1_align2lerobot_v30.py` - 青龙

### 配置文件
- rclone 配置文件（默认：`/root/.config/rclone/rclone.conf`）
- Excel 数据清单（包含任务ID、设备类型、任务描述等信息）

## 使用方法

### 基本用法

```bash
# 从头开始执行
python convert_all.py

# Resume 模式（中断后继续）
python convert_all.py --resume
python convert_all.py -r

# 显示帮助信息
python convert_all.py --help
```

### 配置说明

在脚本中修改以下配置：

```python
# 云端配置
obs_base_path = 'huawei-cloud:openloong-apps-private/data-collector-svc/align'
rclone_config = '/root/.config/rclone/rclone.conf'

# 本地路径
local_base_path = '/mnt/fastdisk/align'        # 下载临时目录
output_base_path = '/mnt/fastdisk/lerobotv3_zhengzhou'  # 输出目录

# Excel 配置
excel_path = "/mnt/fastdisk/定制化&存量数据交付记录_extracted_data.xlsx"
sheet_name = "郑州"                             # Excel 表单名称
robot_type = "智元G1"                           # 要处理的机器人类型

# 日志文件
log_file_path = '/mnt/fastdisk/convert_all_G1_status.txt'
```

### Excel 表格格式

脚本需要一个 Excel 文件，包含以下列：

| 列名 | 说明 | 示例 |
|------|------|------|
| 任务ID | 云端数据的唯一标识 | `7d4237d11d9f4d8494e2b361ed68c8e1` |
| 任务名称 | 任务的中文名称 | 拿可乐 |
| 设备类型 | 机器人类型 | 智元G1 / 星海图R1 / 乐聚KUAVO |
| 设备序列号 | 机器人序列号 | - |
| 处理后文本(中文) | 任务描述（中文） | 拿可乐 |
| 处理后文本(英文) | 任务描述（英文） | Pick up the cola |
| 总时长(小时) | 录制时长 | 2.5 |

## 工作流程

### 流水线处理模式

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  下载任务1   │ --> │  转换任务1   │ --> │  删除任务1   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     │             │
       │                                                      │             │
       v                                                      │   并行执行   │
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     │             │
│  下载任务2   │ --> │  转换任务2   │ --> │  删除任务2   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     │             │
       │                                                      │             │
       v                                                      │             │
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     │             │
│  下载任务3   │ --> │  转换任务3   │ --> │  删除任务3   │     └─────────────┘
└─────────────┘     └─────────────┘     └─────────────┘
```

### 处理步骤

1. **阶段1：预下载**
   - 预先下载前 3 个任务
   - 为后续流水线处理做准备

2. **阶段2：流水线处理**
   - **下载**：从云端下载任务数据（后台线程）
   - **检查数量**：检查子文件夹数量，超过 300 个则删除多余的
   - **转换**：调用转换脚本，转换为 LeRobot v3.0 格式（主线程）
   - **删除**：转换成功后删除原始数据

3. **同时执行**
   - 在转换任务 N 的同时，后台下载任务 N+1
   - 充分利用网络和 CPU 资源

### 数据量限制

每个任务的子文件夹数量会被限制在 **最多 300 个**：
- 数量 ≤ 300：不做处理，继续转换
- 数量 > 300：按名称排序，保留前 300 个，删除多余的

## Resume 功能详解

### 工作原理

Resume 模式会从日志文件中读取历史状态，智能跳过已完成的步骤：

```
任务状态：【下载状态】【转换状态】【删除状态】

场景1：全部完成
  download: success  →  跳过整个任务
  convert:  success
  delete:   success

场景2：下载完成，转换未完成
  download: success  →  跳过下载
  convert:  pending  →  执行转换
  delete:   pending  →  执行删除

场景3：转换完成，删除未完成
  download: success  →  跳过下载
  convert:  success  →  跳过转换
  delete:   pending  →  执行删除

场景4：下载失败
  download: failed   →  跳过整个任务（保护数据完整性）
  convert:  pending
  delete:   pending
```

### 使用场景

1. **程序意外中断**（Ctrl+C、网络断开、系统重启等）
```bash
python convert_all.py --resume
```

2. **只想重新转换失败的任务**
```bash
# 手动编辑日志文件，将失败任务的状态改为 pending
python convert_all.py --resume
```

## 日志文件格式

日志文件示例（`convert_all_status.txt`）：

```
====================================================================================================
任务处理状态日志
更新时间: 2026-03-10 14:30:15
====================================================================================================

----------------------------------------------------------------------------------------------------
任务编号: 1/100
任务ID: 7d4237d11d9f4d8494e2b361ed68c8e1
任务名称: 拿可乐
处理后文本(英文): Pick up the cola
开始时间: 2026-03-10 14:00:00
结束时间: 2026-03-10 14:15:30

【下载状态】: success
【子文件夹数量】: 287
  处理动作: 数量合规(共287个)，无需处理
【转换状态】: success
【删除状态】: success
----------------------------------------------------------------------------------------------------

统计信息:
  总任务数: 100
  下载成功: 95/100
  转换成功: 92/100
  删除成功: 90/100
====================================================================================================
```

## 中断处理

程序支持优雅的中断处理（Ctrl+C）：

1. **捕获信号**：程序会捕获 Ctrl+C 信号
2. **保存状态**：当前任务的状态会立即保存到日志
3. **安全退出**：完成当前操作后退出，不会留下损坏的数据
4. **继续执行**：使用 `--resume` 参数可从中断处继续

```bash
# 运行时按 Ctrl+C
^C
收到中断信号 (Ctrl+C)，正在终止程序...
程序被中断，退出

详细状态已保存到: /mnt/fastdisk/convert_all_status.txt

# 继续执行
python convert_all.py --resume
```

## 转换脚本要求

每个机器人类型需要对应的转换脚本，脚本必须支持以下命令行参数：

```bash
python <robot>_align2lerobot_v30.py \
    --input <输入目录> \
    --output <输出目录> \
    --task <任务描述>
```

## 故障排查

### 问题1：下载失败
```bash
# 检查 rclone 配置
rclone config

# 测试连接
rclone ls huawei-cloud:openloong-apps-private/ --max-depth 1
```

### 问题2：转换失败
```bash
# 查看详细日志
cat convert_all_status.txt

# 单独运行转换脚本测试
python ginie1_align2lerobot_v30.py --input /path/to/data --output /path/to/output --task "test task"
```

### 问题3：磁盘空间不足
- 原始数据通常很大（每个任务可能几十GB）
- 确保临时目录（`local_base_path`）有足够空间
- 转换成功后会自动删除原始数据

## 性能优化建议

1. **调整流水线缓冲区大小**
   ```python
   pipeline_buffer_size = 3  # 可根据网速和磁盘调整
   ```

2. **调整 rclone 传输线程**
   ```python
   --transfers=16  # 可调整并发传输数量
   ```

3. **选择合适的临时目录**
   - 使用 SSD 作为临时目录可显著提升性能
   - 确保有足够的磁盘空间

## 支持的机器人类型

| 机器人类型 | 转换脚本 | State/Action 维度 | 相机 |
|-----------|---------|------------------|------|
| 智元G1 | `ginie1_align2lerobot_v30.py` | 16 (arm:14 + gripper:2) | head, hand_left, hand_right |
| 星海图R1 | `R1_align2lerobot_v30.py` | 14 (arm:12 + gripper:2) | head, hand_left, hand_right |
| 乐聚夸父 | `leju_align2lerobot_v30.py` | 30 (arm:14 + gripper:2 + head:2 + leg:12) | head, hand_left, hand_right |
| 青龙 | `qinglongros1_align2lerobot_v30.py` | - | - |

## 输出结构

```
output_base_path/
├── <task_id_1>/
│   ├── meta/
│   │   ├── info.json          # 数据集元信息
│   │   ├── stats.json         # 统计信息
│   │   ├── tasks.parquet      # 任务列表
│   │   └── episodes/          # Episode 元数据
│   ├── data/                  # Parquet 数据文件
│   │   └── chunk-000/
│   │       └── file-000.parquet
│   └── videos/                # 视频文件
│       └── observation.images.head/
│           └── chunk-000/
│               └── file-000.mp4
├── <task_id_2>/
└── ...
```

## 注意事项

1. **磁盘空间**：确保有足够的磁盘空间存储临时数据和输出数据
2. **网络连接**：需要稳定的网络连接访问云存储
3. **rclone 配置**：确保 rclone 已正确配置云存储访问权限
4. **转换脚本路径**：确保转换脚本的路径正确
5. **Excel 文件格式**：确保 Excel 文件包含所有必需的列

## 错误处理

程序会记录每个任务的详细错误信息：

```
【下载状态】: failed
  下载错误: 下载命令返回码: 1

【转换状态】: failed
  转换错误: 转换命令返回码: 1

【删除状态】: skipped
  删除错误: 转换未成功，保留原数据
```

失败的任务会保留原始数据，不会自动删除，便于手动排查和恢复。

## 示例

### 示例1：处理智元 G1 数据

```python
obs_base_path = 'huawei-cloud:openloong-zhengzhou-apps-private/data-collector-svc/align'
rclone_config = '/root/.config/rclone/rclone.conf'
local_base_path = '/mnt/fastdisk/align'
excel_path = "/mnt/fastdisk/数据交付记录.xlsx"
sheet_name = "郑州"
robot_type = "智元G1"
output_base_path = '/mnt/fastdisk/lerobotv3_zhengzhou'
log_file_path = '/mnt/fastdisk/convert_all_G1_status.txt'
```

```bash
python convert_all.py
```

### 示例2：Resume 继续执行

```bash
# 程序中断（Ctrl+C）
^C
收到中断信号 (Ctrl+C)，正在终止程序...
详细状态已保存到: /mnt/fastdisk/convert_all_G1_status.txt

# 查看已完成的任务
cat convert_all_G1_status.txt

# 继续执行
python convert_all.py --resume

📝 Resume模式：加载已有日志 /mnt/fastdisk/convert_all_G1_status.txt
✓ 已加载 100 个任务的历史状态
  - 已完成: 45
  - 待处理: 55

总共需要处理 100 个智元G1任务
...
```

## 性能指标

基于实际测试的性能数据：

- **下载速度**：取决于网络带宽（rclone --transfers=16）
- **转换速度**：约 50-100 episodes/分钟（取决于数据复杂度）
- **流水线效率**：比串行处理节省约 30-40% 的时间
- **磁盘占用**：临时目录峰值约为 3 个任务的数据量

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 相关链接

- [LeRobot GitHub](https://github.com/huggingface/lerobot)
- [LeRobot 文档](https://github.com/huggingface/lerobot/tree/main/lerobot)
