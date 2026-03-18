# analyze_lerobot_data.py - LeRobot v3.0 数据集统计分析工具

一个用于分析 LeRobot v3.0 格式数据集的统计工具，可以快速生成详细的数据分析报告和 CSV 导出。

## 功能特性

### 📊 统计指标

**总体统计**
- 数据集总数量
- Episode 总数
- Task 总数
- 总帧数
- 总时长（小时/分钟/秒）
- 平均 Episode 时长和帧数

**机器人类型分析**
- 每种机器人类型的数据集数量
- 每种机器人的总 Episodes、Frames、Tasks、时长
- 平均 Episode 数/数据集
- 平均帧数/Episode
- 平均时长/Episode

**技术规格统计**
- 数据格式版本分布（v2.1 / v3.0）
- FPS 分布（帧率统计）
- 相机配置分布（如：head, hand_left, hand_right）
- State/Action 维度统计
- 图像分辨率分布（如：480x640）
- 视频编码格式分布（如：h264, h265）

### 📁 输出格式

- **终端输出**：实时显示分析进度和统计结果
- **文本报告**：保存完整的统计报告到文本文件
- **CSV 导出**：导出详细的数据集信息到 CSV 文件，便于 Excel 分析

## 系统要求

### 依赖项
```bash
# 标准库即可，无需额外安装
python 3.8+
```

### 输入要求
- LeRobot v3.0 格式的数据集目录
- 每个数据集包含 `meta/info.json` 文件

## 使用方法

### 基本用法

```bash
# 基本分析（仅终端输出）
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai

# 保存报告到文本文件
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai --output report.txt

# 导出 CSV 文件
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai --csv datasets.csv

# 同时生成文本报告和 CSV
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai --output report.txt --csv datasets.csv

# 显示帮助
python analyze_lerobot_data.py --help
```

### 命令行参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `data_dir` | 数据集根目录（必需） | `/mnt/fastdisk/lerobotv3_shanghai` |
| `--output`, `-o` | 输出报告的文本文件路径（可选） | `report.txt` |
| `--csv`, `-c` | 导出 CSV 文件路径（可选） | `datasets.csv` |

## 数据集目录结构

脚本要求数据集具有以下结构：

```
data_dir/
├── <task_id_1>/
│   ├── meta/
│   │   └── info.json          # ← 必需：包含数据集元信息
│   ├── data/
│   └── videos/
├── <task_id_2>/
│   ├── meta/
│   │   └── info.json          # ← 必需
│   ├── data/
│   └── videos/
└── ...
```

### info.json 格式要求

脚本会从 `info.json` 中读取以下字段：

```json
{
  "codebase_version": "v3.0",
  "robot_type": "智元G1",
  "total_episodes": 287,
  "total_frames": 72450,
  "total_tasks": 1,
  "fps": 50,
  "features": {
    "observation.state": {
      "shape": [16],
      "names": ["arm_joint_1", "arm_joint_2", ...]
    },
    "action": {
      "shape": [16],
      "names": ["arm_joint_1", "arm_joint_2", ...]
    },
    "observation.images.head": {
      "shape": [480, 640, 3],
      "info": {
        "video.height": 480,
        "video.width": 640,
        "video.codec": "h264",
        "video.fps": 50
      }
    }
  }
}
```

## 输出示例

### 1. 终端输出

```
📂 扫描目录: /mnt/fastdisk/lerobotv3_shanghai
找到 95 个子目录

====================================================================================================
[1/95] ✓ 7d4237d11d9f4d8494e2b361ed68c8e1 - 智元G1 - 287 episodes, 72450 frames, 1 tasks, 24.2分钟
[2/95] ✓ 8e5348e22ea0523ab5f86498df01e88f - 智元G1 - 310 episodes, 78120 frames, 1 tasks, 26.0分钟
[3/95] ✓ 9f6459f33fb1634bc6g97509eg12f99g - 星海图R1 - 256 episodes, 64000 frames, 1 tasks, 21.3分钟
...

====================================================================================================
LeRobot 数据集统计分析报告
====================================================================================================

【总体统计】
----------------------------------------------------------------------------------------------------
  总数据集数量: 95
  加载失败数量: 0
  总 Episodes 数: 28,450
  总 Tasks 数: 95
  总帧数: 7,156,000
  总时长: 39.75 小时 (2,385.3 分钟, 143,120.0 秒)
  平均每个数据集的 Episodes: 299.5
  平均每个 Episode 的帧数: 251.6
  平均每个 Episode 的时长: 5.0 秒

【数据格式版本】
----------------------------------------------------------------------------------------------------
  v3.0: 95 个数据集 (100.0%)

【机器人类型统计】
----------------------------------------------------------------------------------------------------
  智元G1:
    - 数据集数量: 60 (63.2%)
    - 总 Episodes: 18,000
    - 总 Tasks: 60
    - 总帧数: 4,536,000
    - 总时长: 25.20 小时 (1,512.0 分钟)
    - 平均 Episodes/数据集: 300.0
    - 平均帧数/Episode: 252.0
    - 平均时长/Episode: 5.0 秒
  星海图R1:
    - 数据集数量: 25 (26.3%)
    - 总 Episodes: 7,500
    - 总 Tasks: 25
    - 总帧数: 1,890,000
    - 总时长: 10.50 小时 (630.0 分钟)
    - 平均 Episodes/数据集: 300.0
    - 平均帧数/Episode: 252.0
    - 平均时长/Episode: 5.0 秒
  乐聚KUAVO:
    - 数据集数量: 10 (10.5%)
    - 总 Episodes: 2,950
    - 总 Tasks: 10
    - 总帧数: 730,000
    - 总时长: 4.05 小时 (243.3 分钟)
    - 平均 Episodes/数据集: 295.0
    - 平均帧数/Episode: 247.5
    - 平均时长/Episode: 4.9 秒

【FPS 分布】
----------------------------------------------------------------------------------------------------
  50 FPS: 95 个数据集 (100.0%)

【相机配置分布】
----------------------------------------------------------------------------------------------------
  hand_left,hand_right,head: 95 个数据集 (100.0%)

【State/Action 维度分布】
----------------------------------------------------------------------------------------------------
  state:16,action:16: 60 个数据集 (63.2%)  # 智元G1
  state:14,action:14: 25 个数据集 (26.3%)  # 星海图R1
  state:30,action:30: 10 个数据集 (10.5%)  # 乐聚KUAVO

【图像分辨率分布】
----------------------------------------------------------------------------------------------------
  480x640: 285 次出现 (100.0%)  # 3 个相机 × 95 个数据集

【视频编码格式分布】
----------------------------------------------------------------------------------------------------
  h264: 285 次出现 (100.0%)

====================================================================================================
分析完成！

报告已保存到: report.txt
详细数据已导出到 CSV: datasets.csv
====================================================================================================
```

### 2. CSV 导出格式

`datasets.csv` 包含每个数据集的详细信息：

| 任务ID | 数据版本 | 机器人类型 | Episodes数 | 帧数 | 任务数 | 时长(秒) | 时长(分钟) | 时长(小时) | FPS | 相机列表 | State维度 | Action维度 |
|--------|---------|----------|-----------|------|--------|---------|-----------|-----------|-----|---------|-----------|-----------|
| 7d4237d1... | v3.0 | 智元G1 | 287 | 72450 | 1 | 1449.00 | 24.15 | 0.40 | 50 | hand_left, hand_right, head | 16 | 16 |
| 8e5348e2... | v3.0 | 智元G1 | 310 | 78120 | 1 | 1562.40 | 26.04 | 0.43 | 50 | hand_left, hand_right, head | 16 | 16 |
| 9f6459f3... | v3.0 | 星海图R1 | 256 | 64000 | 1 | 1280.00 | 21.33 | 0.36 | 50 | hand_left, hand_right, head | 14 | 14 |

可以直接用 Excel/LibreOffice 打开进行进一步分析和可视化。

## 应用场景

### 1. 数据质量检查

```bash
# 检查所有数据集是否转换成功
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai

# 查看失败的数据集
# 输出中会显示 "加载失败数量: N"
```

### 2. 数据集概览

```bash
# 快速了解数据集的整体情况
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai

# 输出会显示：
# - 有多少个机器人类型
# - 每种机器人有多少数据
# - 数据的平均质量（帧数、时长等）
```

### 3. 生成报告给团队

```bash
# 生成完整的文本报告
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai \
    --output reports/shanghai_data_report_2026-03-10.txt

# 分享报告文件给团队成员
```

### 4. 数据分析和可视化

```bash
# 导出 CSV 用于数据分析
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai \
    --csv datasets.csv

# 然后可以使用 Excel、Pandas、Tableau 等工具进行进一步分析：
# - 绘制 Episode 时长分布图
# - 比较不同机器人类型的数据量
# - 分析数据收集的时间趋势
```

### 5. 多数据源对比

```bash
# 分析上海数据
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai \
    --output reports/shanghai.txt --csv reports/shanghai.csv

# 分析郑州数据
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_zhengzhou \
    --output reports/zhengzhou.txt --csv reports/zhengzhou.csv

# 对比两个地区的数据差异
```

## 统计指标说明

### Episodes vs Tasks vs Frames

- **Task（任务）**：一个数据集通常对应一个任务描述（如"拿可乐"）
- **Episode（片段）**：一个任务包含多个 Episode（录制的不同尝试）
- **Frame（帧）**：每个 Episode 包含多帧数据（状态、动作、图像）

关系：`1 Task → N Episodes → M Frames`

### 时长计算

```
时长(秒) = 总帧数 / FPS

示例：
  总帧数: 72,450
  FPS: 50
  时长 = 72,450 / 50 = 1,449 秒 = 24.15 分钟 = 0.40 小时
```

### State/Action 维度

不同机器人的关节数量不同：

| 机器人类型 | State/Action 维度 | 说明 |
|-----------|------------------|------|
| 智元G1 | 16 | arm(14) + gripper(2) |
| 星海图R1 | 14 | arm(12) + gripper(2) |
| 乐聚KUAVO | 30 | arm(14) + gripper(2) + head(2) + leg(12) |

### 相机配置

常见的相机组合：
- `hand_left, hand_right, head`：三目相机（最常见）
- `hand_left, hand_right`：双手相机
- `head`：单头部相机

## 性能

- **扫描速度**：约 100-200 个数据集/秒（取决于磁盘性能）
- **内存占用**：约 50-100 MB（仅读取 `info.json`，不加载实际数据）
- **大规模数据集**：可处理 1000+ 个数据集

## 故障排查

### 问题1：找不到 info.json

```
[1/95] ✗ task_id_xxx - 无法加载 info.json
```

**原因**：数据集未转换完成或转换失败

**解决方法**：
```bash
# 检查数据集目录结构
ls -la /mnt/fastdisk/lerobotv3_shanghai/task_id_xxx/meta/

# 重新转换该数据集
python ginie1_align2lerobot_v30.py --input ... --output ...
```

### 问题2：加载失败数量较多

```
加载失败数量: 15
```

**原因**：可能有多个数据集转换失败

**解决方法**：
```bash
# 查看详细的日志输出，找到失败的数据集
python analyze_lerobot_data.py /path/to/data 2>&1 | grep "✗"

# 检查 convert_all.py 的日志文件
cat /mnt/fastdisk/convert_all_status.txt | grep "转换状态: failed"
```

### 问题3：统计数据异常

```
平均每个 Episode 的帧数: 0.0
```

**原因**：`info.json` 中的数据不完整

**解决方法**：
```bash
# 检查 info.json 的内容
cat /path/to/dataset/meta/info.json

# 确保包含 total_frames、total_episodes、fps 等字段
```

## 与 convert_all.py 的配合使用

`analyze_lerobot_data.py` 通常在 `convert_all.py` 完成数据转换后使用：

```bash
# 步骤1：批量转换数据
python convert_all.py

# 步骤2：分析转换后的数据
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai \
    --output reports/final_report.txt \
    --csv reports/datasets.csv

# 步骤3：检查是否有失败的转换
# 查看 "加载失败数量" 指标
```

## 输出解读示例

### 场景：数据集质量检查

```
总数据集数量: 100
加载失败数量: 5         ← ⚠️ 有 5 个数据集转换失败
总 Episodes 数: 28,500
平均每个 Episode 的帧数: 250.5   ← ✓ 正常范围（200-300）
平均每个 Episode 的时长: 5.0 秒  ← ✓ 正常范围（3-10秒）
```

**分析**：
- 95% 的数据集转换成功
- Episode 质量良好（帧数和时长在合理范围）
- 需要检查 5 个失败的数据集

### 场景：不同机器人数据对比

```
智元G1:
  - 数据集数量: 60 (60%)
  - 总时长: 25.20 小时
  - 平均时长/Episode: 5.0 秒

星海图R1:
  - 数据集数量: 25 (25%)
  - 总时长: 10.50 小时
  - 平均时长/Episode: 5.0 秒
```

**分析**：
- 智元G1 的数据量是 R1 的 2.4 倍
- 两种机器人的 Episode 质量一致（平均时长相同）
- 数据收集较为均衡

## 扩展建议

### 自定义分析

可以修改脚本添加更多分析维度：

```python
# 添加任务类型统计
task_types = defaultdict(int)
for dataset_dir in subdirs:
    info = load_dataset_info(dataset_dir)
    task_name = dataset_dir.name  # 或从 info 中提取
    task_types[task_name] += 1

# 输出任务类型分布
print("\n【任务类型分布】")
for task_name, count in sorted(task_types.items()):
    print(f"  {task_name}: {count}")
```

### 数据可视化

使用 Python 绘图库可视化统计结果：

```python
import matplotlib.pyplot as plt
import pandas as pd

# 读取 CSV
df = pd.read_csv('datasets.csv')

# 绘制 Episode 时长分布
plt.hist(df['时长(分钟)'], bins=20)
plt.xlabel('时长 (分钟)')
plt.ylabel('数据集数量')
plt.title('Episode 时长分布')
plt.show()

# 绘制机器人类型分布
robot_counts = df['机器人类型'].value_counts()
plt.pie(robot_counts, labels=robot_counts.index, autopct='%1.1f%%')
plt.title('机器人类型分布')
plt.show()
```

## 技术细节

### info.json 解析

脚本使用标准的 JSON 解析，支持 LeRobot v2.1 和 v3.0 格式：

```python
info = json.load(open('meta/info.json'))

# v3.0 格式
version = info['codebase_version']          # "v3.0"
robot_type = info['robot_type']             # "智元G1"
total_episodes = info['total_episodes']     # 287
total_frames = info['total_frames']         # 72450
fps = info['fps']                           # 50

# features 解析
features = info['features']
cameras = [key.replace('observation.images.', '') 
           for key in features if key.startswith('observation.images.')]
```

### 相机信息提取

从 `features` 中提取相机配置：

```python
# observation.images.head → head
# observation.images.hand_left → hand_left
# observation.images.hand_right → hand_right

cameras = extract_cameras_from_features(features)
# 返回: ['hand_left', 'hand_right', 'head']
```

### CSV 导出格式

CSV 使用 UTF-8 编码，包含 BOM 头（Excel 兼容）：

```python
with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['任务ID', '数据版本', '机器人类型', ...])
    # ...
```

## 注意事项

1. **只读操作**：脚本只读取 `meta/info.json`，不会修改任何数据
2. **轻量级**：不加载实际的 Parquet 或视频数据，运行速度快
3. **容错性**：即使某些数据集损坏，也会继续处理其他数据集
4. **编码兼容**：CSV 导出使用 UTF-8-sig，Excel 可直接打开

## 许可证

MIT License

## 相关文档

- [convert_all.py README](README_convert_all.md) - 数据转换工具
- [LeRobot GitHub](https://github.com/huggingface/lerobot)
- [LeRobot v3.0 格式说明](https://github.com/huggingface/lerobot/tree/main/lerobot)

## 贡献

欢迎提交 Issue 和 Pull Request！

### 功能建议

- [ ] 支持自定义筛选条件（如只统计特定机器人类型）
- [ ] 添加数据可视化功能（直接生成图表）
- [ ] 支持增量分析（只分析新增的数据集）
- [ ] 支持导出 JSON 格式
- [ ] 添加数据健康检查（检测异常的 Episode）

## 更新日志

### v1.1.0 (2026-03-10)
- 新增：总 Tasks 数统计
- 新增：总时长统计（小时/分钟/秒）
- 新增：CSV 导出时长字段
- 改进：统计报告格式优化

### v1.0.0 (2026-03-09)
- 初始版本
- 支持基本统计分析
- 支持文本报告和 CSV 导出
