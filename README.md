# 数据处理工具说明

本目录包含机器人数据从原始 align 数据到 LeRobot 格式、再到统计分析与版本转换的一整套脚本。下文按使用顺序简要介绍各工具；详细参数与配置请查看对应文档。

---

## 1. 批量转换：convert_all.py

**路径：** `convert2lerobotv30/convert_all.py`

**功能：** 从 Excel 任务清单读取任务，按机型和 region 自动选择转换脚本，批量完成「云端下载 → LeRobot v3.0 转换 → 本地原始数据清理」。支持多 sheet、多机型混合处理，以及 Resume 断点续跑。

**Quick Start：**

```bash
cd convert2lerobotv30

# 处理全部任务
python convert_all.py

# 断点续跑
python convert_all.py --resume

# 只处理清单中第 10～60 个任务
python convert_all.py -r --task-from 10 --task-to 60
```

**详细文档：** [convert2lerobotv30/README.md](convert2lerobotv30/README.md)

---

## 2. 数据统计与报告合并

### analyze_lerobot_data.py

**路径：** `convert2lerobotv30/analyze_lerobot_data.py`

**功能：** 扫描 LeRobot v3.0 数据集目录，统计机型、episode 数、帧数、时长、相机配置等信息，并生成文本报告或 CSV 导出。

**Quick Start：**

```bash
cd convert2lerobotv30

# 分析指定目录（终端输出）
python analyze_lerobot_data.py ../lerobotv30

# 保存报告到文件
python analyze_lerobot_data.py ../lerobotv30 --output report.txt
```

### merge_lerobot_reports.py

**路径：** `convert2lerobotv30/merge_lerobot_reports.py`

**功能：** 将多份 `analyze_lerobot_data.py` 生成的 txt 报告按机器人类型合并汇总，便于跨地区 / 跨批次统计。

**Quick Start：**

```bash
cd convert2lerobotv30

# 使用默认输入报告，输出到 report_merged.txt
python merge_lerobot_reports.py

# 指定输入和输出
python merge_lerobot_reports.py report_shanghai.txt report_zhengzhou.txt -o report_merged.txt
```

**详细文档：** [convert2lerobotv30/README_analyze_lerobot_data.md](convert2lerobotv30/README_analyze_lerobot_data.md)

---

## 3. 版本转换：lerobot_v30_to_v21/convert.py

**路径：** `lerobot_v30_to_v21/convert.py`

**功能：** 将 LeRobot v3.0 格式数据集转换为 v2.1 格式。支持单个数据集转换、批量并行转换，以及 Resume（自动跳过已转换的数据集）。输出默认按 `robot_type/dataset_id` 组织目录。

**Quick Start：**

```bash
cd lerobot_v30_to_v21

# 转换单个数据集
python convert.py \
    --input ../lerobotv30/<dataset_id> \
    --output-dir ../lerobotv21

# 批量并行转换
python convert.py \
    --input ../lerobotv30 \
    --output-dir ../lerobotv21 \
    --batch --workers 8
```

**详细文档：** [lerobot_v30_to_v21/README.md](lerobot_v30_to_v21/README.md)

---

## 典型工作流

```
Excel 任务清单
    ↓  convert_all.py
LeRobot v3.0 数据集（lerobotv30/...）
    ↓  analyze_lerobot_data.py / merge_lerobot_reports.py
统计报告
    ↓  lerobot_v30_to_v21/convert.py（可选）
LeRobot v2.1 数据集（lerobotv21/...）
```
