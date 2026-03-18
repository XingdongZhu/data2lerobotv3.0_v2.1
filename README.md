# LeRobot 数据处理工具集

这是一套完整的机器人数据处理工具集，涵盖从原始数据下载、格式转换到数据分析的全流程。

## 📚 文档导航

根据您的需求，选择相应的工具和文档：

### 🔄 数据转换工具

#### 1. 原始数据 → LeRobot v3.0 格式

**工具位置：** `convert2lerobotv30/`

**功能：** 从云端批量下载原始机器人数据，并转换为 LeRobot v3.0 标准格式

**适用场景：**
- 需要从云存储（华为云 OBS）下载原始数据
- 需要将原始 H5 格式数据转换为 LeRobot v3.0 格式
- 需要批量处理多个任务的数据
- 需要流水线并行处理（下载和转换同时进行）
- 需要 Resume 功能（中断后继续）

**支持的机器人类型：**
- 智元 G1 (`ginie1_align2lerobot_v30.py`)
- 智元 A2 (`a2_align2_lerobot_v30.py`)
- 星海图 R1 (`R1_align2lerobot_v30.py`)
- 乐聚夸父 (`leju_align2lerobot_v30.py`)
- 青龙 (`qinglongros1_align2lerobot_v30.py`)
- 傅利叶 GR2 (`gr2_align2lerobot_v30.py`)

**📖 详细文档：** [convert2lerobotv30/README_convert_all.md](convert2lerobotv30/README_convert_all.md)

---

#### 2. LeRobot v3.0 → v2.1 格式

**工具位置：** `lerobot_v30_to_v21/`

**功能：** 将 LeRobot v3.0 格式数据集转换为 v2.1 格式

**适用场景：**
- 需要使用旧版本的 LeRobot 工具链
- 需要按 Episode 独立存储数据和视频
- 需要 JSONL 格式的元数据（而不是 Parquet）
- 需要批量并行转换大量数据集
- 需要 Resume 功能（自动跳过已转换的数据集）

**主要特性：**
- 单个或批量转换
- 多进程并行转换（可配置 workers 数量）
- 自动按机器人类型分组
- Resume 支持（断点续传）
- 视频切分使用 stream copy（无质量损失）

**📖 详细文档：** [lerobot_v30_to_v21/README.md](lerobot_v30_to_v21/README.md)

---

### 📊 数据分析工具

#### 3. LeRobot v3.0 数据集统计分析

**工具位置：** `convert2lerobotv30/analyze_lerobot_data.py`

**功能：** 对 LeRobot v3.0 格式数据集进行统计分析和报告生成

**适用场景：**
- 需要了解数据集的整体情况（数量、时长、分布等）
- 需要检查数据转换是否成功
- 需要生成数据统计报告（文本或 CSV）
- 需要按机器人类型统计数据
- 需要分析不同数据集的质量指标

**统计指标：**
- 总体统计（数据集数量、Episodes、Tasks、帧数、时长）
- 机器人类型分析（每种机器人的数据量、平均质量）
- 技术规格（FPS、相机配置、State/Action 维度、视频编码）
- 详细的 CSV 导出（便于 Excel 分析和可视化）

**📖 详细文档：** [convert2lerobotv30/README_analyze_lerobot_data.md](convert2lerobotv30/README_analyze_lerobot_data.md)

---

## 🚀 快速开始

### 典型工作流程

**场景1：从零开始处理原始数据**

```bash
# 步骤1：批量下载并转换为 v3.0 格式
cd convert2lerobotv30
python convert_all.py --batch

# 步骤2：分析转换后的数据
python analyze_lerobot_data.py /mnt/fastdisk/lerobotv3_shanghai \
    --output report.txt --csv datasets.csv
```

**场景2：转换 v3.0 数据为 v2.1 格式**

```bash
# 批量并行转换（使用 8 个进程）
cd lerobot_v30_to_v21
python convert.py \
    --input /mnt/fastdisk/lerobotv3_shanghai \
    --output-dir /mnt/fastdisk/lerobotv21 \
    --batch --workers 8
```

**场景3：Resume 模式继续处理**

```bash
# convert_all.py 中断后继续
cd convert2lerobotv30
python convert_all.py --resume

# v3.0 to v2.1 转换中断后继续（自动检测已完成的）
cd lerobot_v30_to_v21
python convert.py \
    --input /mnt/fastdisk/lerobotv3_shanghai \
    --output-dir /mnt/fastdisk/lerobotv21 \
    --batch --workers 8
```

---

## 📋 功能对比表

| 功能 | convert_all.py | lerobot_v30_to_v21 | analyze_lerobot_data.py |
|------|---------------|-------------------|------------------------|
| **主要功能** | 下载原始数据并转换为 v3.0 | v3.0 → v2.1 格式转换 | 数据统计分析 |
| **输入格式** | 云端原始数据（H5） | LeRobot v3.0 | LeRobot v3.0 |
| **输出格式** | LeRobot v3.0 | LeRobot v2.1 | 统计报告 + CSV |
| **并行处理** | ✓ 流水线（下载+转换） | ✓ 多进程并行 | - |
| **Resume 支持** | ✓ 基于日志 | ✓ 自动检测 | - |
| **批量模式** | ✓ | ✓ | ✓ |
| **Progress 显示** | ✓ 详细状态 | ✓ 简洁进度 | ✓ 实时扫描 |
| **配置复杂度** | 高（需配置 rclone） | 低 | 低 |

---

## 🛠️ 环境准备

### 通用依赖

```bash
# Python 3.8+
python --version

# 安装 LeRobot
git clone https://github.com/huggingface/lerobot.git
cd lerobot
pip install -e .
```

### 工具特定依赖

**convert_all.py:**
```bash
pip install pandas openpyxl h5py opencv-python numpy
# 需要配置 rclone（用于云端下载）
```

**lerobot_v30_to_v21:**
```bash
pip install "datasets<4.0.0"
# 需要安装 ffmpeg（用于视频切分）
sudo apt-get install -y ffmpeg
```

**analyze_lerobot_data.py:**
```bash
# 仅使用标准库，无需额外依赖
```

---

## 📁 目录结构

```
/mnt/fastdisk/
├── README.md                                    # 📄 本文档（总索引）
│
├── convert2lerobotv30/                          # 🔄 v3.0 转换工具
│   ├── README_convert_all.md                   # 📖 批量转换文档
│   ├── README_analyze_lerobot_data.md          # 📖 数据分析文档
│   ├── convert_all.py                          # 批量下载和转换脚本
│   ├── analyze_lerobot_data.py                 # 数据统计分析脚本
│   ├── ginie1_align2lerobot_v30.py            # 智元 G1 转换脚本
│   ├── R1_align2lerobot_v30.py                # 星海图 R1 转换脚本
│   ├── leju_align2lerobot_v30.py              # 乐聚夸父转换脚本
│   ├── qinglongros1_align2lerobot_v30.py      # 青龙转换脚本
│   └── gr2_align2lerobot_v30.py               # 傅利叶 GR2 转换脚本
│
├── lerobot_v30_to_v21/                          # 🔄 v2.1 转换工具
│   ├── README.md                               # 📖 v3.0→v2.1 转换文档
│   ├── convert.py                              # 转换入口脚本
│   ├── convert_dataset_v30_to_v21.py           # 核心转换逻辑
│   └── requirements.txt                        # Python 依赖
│
├── align/                                       # 💾 原始数据下载目录（临时）
├── lerobotv3_shanghai/                         # 💾 v3.0 格式数据（上海）
├── lerobotv3_zhengzhou/                        # 💾 v3.0 格式数据（郑州）
└── lerobotv21/                                 # 💾 v2.1 格式数据
```

---

## 🔍 常见问题

### Q1: 我应该使用哪个工具？

**如果您有原始数据（H5 格式）：**
→ 使用 `convert_all.py` 转换为 v3.0 格式
→ 参考：[README_convert_all.md](convert2lerobotv30/README_convert_all.md)

**如果您需要 v2.1 格式：**
→ 使用 `lerobot_v30_to_v21/convert.py` 转换
→ 参考：[lerobot_v30_to_v21/README.md](lerobot_v30_to_v21/README.md)

**如果您需要统计数据集信息：**
→ 使用 `analyze_lerobot_data.py` 分析
→ 参考：[README_analyze_lerobot_data.md](convert2lerobotv30/README_analyze_lerobot_data.md)

### Q2: 为什么有 v3.0 和 v2.1 两个版本？

- **v3.0**：最新版本，使用合并的 Parquet 和视频文件，更高效，适合训练
- **v2.1**：旧版本，每个 Episode 独立文件，兼容旧工具链

### Q3: 数据处理的完整流程是什么？

```
原始数据（云端 H5）
    ↓ convert_all.py
LeRobot v3.0 格式
    ↓ analyze_lerobot_data.py（可选：数据检查）
    ↓ lerobot_v30_to_v21/convert.py（可选：如需 v2.1）
LeRobot v2.1 格式
```

### Q4: 如何中断后继续处理？

**convert_all.py:**
```bash
python convert_all.py --resume
```

**lerobot_v30_to_v21/convert.py:**
```bash
# 自动检测已完成的数据集，无需特殊参数
python convert.py --input ... --output-dir ... --batch
```

### Q5: 如何提升批量转换速度？

**convert_all.py:**
- 调整流水线缓冲区大小（默认 3）
- 增加 rclone 传输线程（`--transfers=16`）
- 使用 SSD 作为临时目录

**lerobot_v30_to_v21/convert.py:**
- 增加 workers 数量：`--workers 8`（建议为 CPU 核心数的 50-100%）
- 使用 SSD 作为输出目录
- 确保磁盘 I/O 不是瓶颈

---

## 📊 性能参考

基于实际测试的性能数据（Intel Xeon Gold 6248R, 64GB RAM, SSD）：

| 工具 | 处理速度 | 100 个数据集耗时 |
|------|---------|----------------|
| convert_all.py | 流水线模式 | ~3-5 小时（取决于网速） |
| lerobot_v30_to_v21 (4 workers) | ~40 秒/数据集 | ~2 小时 |
| lerobot_v30_to_v21 (8 workers) | ~20 秒/数据集 | ~1.1 小时 |
| analyze_lerobot_data.py | ~200 数据集/秒 | ~30 秒 |

---

## 💡 使用建议

1. **首次使用**：先处理少量数据（3-5 个任务）测试流程
2. **磁盘规划**：确保有足够空间
   - 原始数据：~20-30 GB/任务
   - v3.0 格式：~15-25 GB/任务
   - v2.1 格式：~15-25 GB/任务
3. **并行设置**：根据硬件配置调整
   - CPU 密集：增加 workers
   - 磁盘瓶颈：减少 workers
4. **数据校验**：转换后使用 `analyze_lerobot_data.py` 检查数据完整性
5. **备份策略**：重要数据及时备份，转换成功后再删除原始数据

---

## 🐛 故障排查

### 通用问题

**磁盘空间不足：**
```bash
df -h  # 检查磁盘空间
du -sh /mnt/fastdisk/*  # 查看各目录占用
```

**Python 依赖问题：**
```bash
pip list | grep -E "lerobot|datasets|h5py|opencv"
pip install --upgrade <package>
```

### 工具特定问题

各工具的详细故障排查请参考相应的 README 文档：
- [convert_all.py 故障排查](convert2lerobotv30/README_convert_all.md#故障排查)
- [lerobot_v30_to_v21 故障排查](lerobot_v30_to_v21/README.md#故障排查)
- [analyze_lerobot_data.py 故障排查](convert2lerobotv30/README_analyze_lerobot_data.md#故障排查)

---

## 📞 获取帮助

1. 查看相应工具的详细 README 文档
2. 使用 `--help` 参数查看命令行帮助
   ```bash
   python convert_all.py --help
   python convert.py --help
   python analyze_lerobot_data.py --help
   ```
3. 检查日志文件（`convert_all_status.txt` 等）

---

## 📝 更新日志

### 2026-03-18
- 创建总索引文档
- 整合三个工具的文档链接

### 2026-03-16
- 完成 lerobot_v30_to_v21 并行转换功能
- 添加 Resume 支持

### 2026-03-10
- 完成 convert_all.py Resume 功能
- 完成 analyze_lerobot_data.py 时长统计

---

## 📄 许可证

MIT License

## 🔗 相关链接

- [LeRobot GitHub](https://github.com/huggingface/lerobot)
- [LeRobot 文档](https://github.com/huggingface/lerobot/tree/main/lerobot)
- [Hugging Face Datasets](https://huggingface.co/docs/datasets)

---

## 🎯 快速跳转

- **批量下载和转换原始数据** → [README_convert_all.md](convert2lerobotv30/README_convert_all.md)
- **v3.0 转 v2.1 格式** → [lerobot_v30_to_v21/README.md](lerobot_v30_to_v21/README.md)
- **数据统计分析** → [README_analyze_lerobot_data.md](convert2lerobotv30/README_analyze_lerobot_data.md)
