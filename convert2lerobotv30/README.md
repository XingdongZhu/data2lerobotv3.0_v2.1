# convert2lerobotv30 使用说明

本目录包含机器人 align 数据到 LeRobot v3.0 的一整套工具：

- **`convert_all.py`**：从 Excel 任务清单批量完成「云端下载 → LeRobot v3.0 转换 → 本地原始数据清理」
- **各机型 `*_align2lerobot*.py`**：单任务 H5 → LeRobot v3.0 转换脚本；核心八机型使用 `*_no_norm` 版本（保留原始物理量）

输入为已对齐的 h5 文件（`*_align.h5`，metadata ver 2.1.0），输出为 LeRobot v3.0 数据集。本目录是项目整体工作流的第一步，详见根目录 [README.md](../README.md)。

---

## 1. 批量转换：convert_all.py

`convert_all.py` 从 Excel 任务清单读取任务，按机型和 region 自动选择转换脚本，支持多 sheet、多 region、多机型混合批处理，以及 Resume 断点续跑。

### 功能概览

| 能力 | 说明 |
|------|------|
| 多 sheet 批处理 | 通过 `EXCEL_CONFIG` 同时配置多个 Excel sheet，每个 sheet 可指定不同机型和 region |
| 按机型选脚本 | 根据 Excel 中的「设备名称」自动匹配对应 `*_align2lerobot*.py` |
| 按 region 选 OBS | 郑州 / 上海使用不同的 OBS 桶和 rclone 配置 |
| 流水线并行 | 转换当前任务的同时，后台线程下载下一个任务 |
| Resume | 从 `convert_all_status.txt` 恢复进度，跳过已完成步骤 |
| 任务区间 | 支持 `--task-from` / `--task-to` 只跑清单中的部分任务 |
| 状态日志 | 统一记录每个任务的下载 / 转换 / 删除状态及错误信息 |

### 依赖与环境

#### Python 依赖

```bash
pip install pandas openpyxl
```

#### 外部工具

- **rclone**：从华为云 OBS 下载 align 数据
- **各机型转换脚本**：位于 `convert2lerobotv30/`（部署环境默认 `/mnt/fastdisk/convert2lerobotv30/`）
- **lerobot 环境**：转换脚本通常需要在已安装 `lerobot`、`h5py` 等依赖的环境中运行

#### rclone 配置

脚本内 `OBS_RCLONE_CONFIG` 定义了两个 region：

| region | OBS 路径 | rclone 配置文件 |
|--------|----------|-----------------|
| 郑州 | `huawei-cloud:openloong-zhengzhou-apps-private/data-collector-svc/align` | `/root/.config/rclone/rclone_zhengzhou.conf` |
| 上海 | `huawei-cloud:openloong-apps-prod-private/data-collector-svc/align` | `/root/.config/rclone/rclone_shanghai.conf` |

### 快速开始

```bash
cd convert2lerobotv30

# 处理全部任务
python convert_all.py

# 断点续跑
python convert_all.py --resume
python convert_all.py -r

# 只处理清单中第 10～60 个任务
python convert_all.py --task-from 10 --task-to 60

# 子区间 + Resume（推荐）
python convert_all.py -r --task-from 10 --task-to 60

# 查看帮助
python convert_all.py --help
```

### 命令行参数

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--resume` | `-r` | 关闭 | 从状态日志恢复，跳过已成功完成的步骤 |
| `--task-from` | — | `1` | 本批起始任务序号（含），相对「筛选后的全局清单」 |
| `--task-to` | — | 最后一个 | 本批结束任务序号（含） |

#### 任务序号说明

- 所有 Excel sheet 按 `EXCEL_CONFIG` 顺序读取、按机型筛选后，合并成**一份全局任务清单**
- `--task-from` / `--task-to` 的序号是这份清单中的位置，从 1 开始
- 日志里「任务编号: X/Y」中，`Y` 是清单总任务数，`X` 是全局序号

#### 子区间 + 日志注意事项

若指定了子区间（不是从 1 到最后一个），且**未使用 `--resume`**，而状态日志文件已存在，脚本会直接报错退出。

处理方式：

1. 使用 `--resume` 在原日志上继续跑该区间，或
2. 先备份 / 删除 `convert_all_status.txt` 后再非 resume 运行

### 脚本内主要配置

以下配置均在 `convert_all.py` 的 `if __name__ == "__main__":` 块中，运行前按需修改。

#### Excel 文件

```python
excel_path = "/mnt/fastdisk/convert2lerobotv30/数据转换第三批次20260513测试版本.xlsx"
```

#### 表单与机型配置 `EXCEL_CONFIG`

每个 sheet 配置：

- `robot_types`：该 sheet 中需要处理的「设备名称」列表
- `region`：`"郑州"` 或 `"上海"`，决定 OBS 桶和 rclone 配置

当前默认配置：

| sheet 名称 | region | 机型 |
|------------|--------|------|
| 郑州平台 | 郑州 | 智元G1、青龙ROS2、智元A2、乐聚KUAVO、傅利叶GR2、Dwheel |
| 重点研发-上海 | 上海 | 智元G1 |
| 模型内部需求-上海 | 上海 | 智元A2、方舟无限arx-acone、傅利叶GR2、智元G1、星尘智能S1、UR5e、天机、Dwheel、松灵Aloha、乐聚KUAVO、星海图R1、Franka FR3 |

Excel 中「设备名称」不在 `robot_types` 列表内的行会被跳过。

#### 本地路径

```python
local_base_path = '/mnt/fastdisk/align'       # 下载临时目录
output_base_path = '/mnt/fastdisk/lerobotv30' # 输出根目录
```

输出目录按 sheet 名自动创建：

```
/mnt/fastdisk/lerobotv30/
├── 郑州平台/
│   └── <task_id>/
├── 重点研发-上海/
│   └── <task_id>/
└── 模型内部需求-上海/
    └── <task_id>/
```

#### 数据量限制 `MAX_COUNT`

```python
MAX_COUNT = 2  # 每个 task_id 最多下载 / 保留的子目录（episode）数量
```

- 下载时：只从 OBS 拉取按名称排序后的前 `MAX_COUNT` 个子文件夹
- 本地检查时：若子目录 + 文件总数超过 `MAX_COUNT`，按名称排序保留前 `MAX_COUNT` 个，删除其余

> 生产环境通常将 `MAX_COUNT` 设为 `300`；当前脚本中默认值为 `2`，适合测试。

#### 状态日志

```python
log_file_path = '/mnt/fastdisk/convert2lerobotv30/convert_all_status.txt'
```

#### 流水线缓冲

```python
pipeline_buffer_size = 3  # 预下载缓冲任务数
```

### Excel 表格格式

脚本通过 `get_data()` 读取 Excel，使用的列名如下：

| Excel 列名 | 脚本内部字段 | 说明 |
|------------|--------------|------|
| 任务ID | 任务ID | 云端 / 本地目录名 |
| 任务名称 | 任务名称 | 中文任务名 |
| 设备名称 | 设备类型 | 用于匹配机型和转换脚本 |
| 步骤(处理后) | 处理后文本(中文) | 中文步骤描述 |
| 步骤(英文) | 处理后文本(英文) | 传给转换脚本 `--task` 参数 |

空行会自动过滤。

### 工作流程

#### 总体流程

```
读取 EXCEL_CONFIG 中所有 sheet
    ↓
按 robot_types 筛选任务，合并为全局清单
    ↓
按 --task-from / --task-to 截取本批任务
    ↓
阶段1：预下载（最多 pipeline_buffer_size 个任务）
    ↓
阶段2：流水线处理（对每个任务）
    ├─ 下载（如需）
    ├─ 检查 / 限制子目录数量
    ├─ 转换（主线程）
    ├─ 删除本地 align 数据（转换成功后）
    └─ 同时后台下载下一个待处理任务
    ↓
LeRobot v3.0 数据集（lerobotv30/<sheet_name>/<task_id>/）
    ↓  analyze_lerobot_data.py / merge_lerobot_reports.py（可选）
统计报告
    ↓  lerobot_v30_to_v21/convert.py（可选）
LeRobot v2.1 数据集（lerobotv21/...）
```

#### 单个任务的处理步骤

1. **下载**  
   使用 rclone 从 `{obs_base_path}/{task_id}/{subdir}` 复制到 `{local_base_path}/{task_id}/{subdir}`，最多 `MAX_COUNT` 个子目录。

2. **数量检查**  
   调用 `check_and_limit_subfolder_count()`，确保本地 episode 数量不超过 `MAX_COUNT`。

3. **转换**  
   执行：

   ```bash
   SVT_LOG=1 python3 <convert_script> \
       --input /mnt/fastdisk/align/<task_id> \
       --output /mnt/fastdisk/lerobotv30/<sheet_name>/<task_id> \
       --task "<处理后文本(英文)>"
   ```

4. **清理**  
   转换成功后执行 `rm -rf /mnt/fastdisk/align/<task_id>`。  
   转换失败则**保留**原始数据，便于排查。

5. **上传**  
   代码中预留了上传步骤（`# TODO: 添加上传命令`），当前未实现。

#### 流水线示意

```
时间 →
任务1: [下载] → [转换] → [删除]
任务2:          [下载] → [转换] → [删除]
任务3:                   [下载] → [转换] → [删除]
```

### Resume 机制

状态由 `TaskStatusLogger` 写入 `convert_all_status.txt`，每个任务记录：

- 来源表单（`sheet_name`）
- Robot Type（`robot_type`）
- 下载 / 转换 / 删除状态（`success` / `failed` / `pending` / `skipped`）
- 各阶段错误信息

#### 跳过规则

| 条件 | 行为 |
|------|------|
| 下载、转换、删除均为 `success` | 跳过整个任务 |
| 下载已为 `success` | 跳过下载，继续转换 / 删除 |
| 转换已为 `success` | 跳过转换 |
| 删除已为 `success` | 跳过删除 |
| 下载失败 | 跳过后续转换和删除，保留 `failed` 状态 |

只有状态为 `success` 的步骤才会被跳过；`skipped` / `failed` / `pending` 均可重试。

重新下载成功后，若之前转换 / 删除被标记为 `skipped`，会自动重置为 `pending` 并重试。

### 状态日志示例

```
====================================================================================================
任务处理状态日志
更新时间: 2026-05-26 14:30:15
====================================================================================================

----------------------------------------------------------------------------------------------------
任务编号: 3/120
任务ID: b6104a919ef14249966311b1ff8cd5a5
来源表单: 模型内部需求-上海
Robot Type: Franka FR3
任务名称: xxx
处理后文本(英文): pick up the object
开始时间: 2026-05-26 14:00:00
结束时间: 2026-05-26 14:15:30

【下载状态】: success
【子文件夹数量】: 2
  处理动作: 数量合规(共2个)，无需处理
【转换状态】: success
【删除状态】: success
----------------------------------------------------------------------------------------------------

统计信息:
  总任务数: 120
  下载成功: 118/120
  转换成功: 115/120
  删除成功: 115/120
====================================================================================================
```

### 输出目录结构

每个任务转换完成后，输出为 LeRobot v3.0 数据集：

```
/mnt/fastdisk/lerobotv30/<sheet_name>/<task_id>/
├── meta/
│   ├── info.json
│   ├── stats.json
│   ├── tasks.parquet
│   └── episodes/
├── data/
│   └── chunk-000/
│       └── file-000.parquet
└── videos/
    └── observation.images.<camera>/
        └── chunk-000/
            └── file-000.mp4
```

### 中断与恢复

程序注册了 `SIGINT` 处理器，支持 `Ctrl+C` 优雅退出：

```bash
# 运行中按 Ctrl+C
^C
收到中断信号 (Ctrl+C)，正在终止程序...

# 查看日志
cat convert_all_status.txt

# 继续执行
python convert_all.py --resume
```

### 故障排查

#### 下载失败

```bash
# 检查 rclone 配置
rclone --config /root/.config/rclone/rclone_shanghai.conf lsf \
  huawei-cloud:openloong-apps-prod-private/data-collector-svc/align --dirs-only | head
```

常见原因：rclone 配置错误、OBS 路径不存在、网络问题、远端无子目录。

#### 转换失败

```bash
# 查看日志中的 convert_error
grep -A2 "转换错误" convert_all_status.txt

# 单独测试某个机型（以 Franka FR3 为例）
SVT_LOG=1 python3 fr3_align2lerobotv30.py \
  --input /mnt/fastdisk/align/<task_id> \
  --output /mnt/fastdisk/lerobotv30/test/<task_id> \
  --task "test task"

# 单独测试 no_norm 机型（以傅利叶 GR2 为例）
SVT_LOG=1 python3 gr2_align2lerobot_v30_no_norm.py \
  --input /mnt/fastdisk/align/<task_id> \
  --output /mnt/fastdisk/lerobotv30/test/<task_id> \
  --task "test task"
```

常见原因：转换脚本路径错误、H5 字段缺失、Python 依赖未安装。

#### 子区间运行报错

```
错误: 已指定任务子区间 (--task-from/--task-to)，但未使用 --resume，且状态日志文件已存在。
```

解决：加 `-r`，或移走已有日志文件。

#### 磁盘空间

- 下载目录 `/mnt/fastdisk/align` 在转换完成前会暂存原始数据
- 流水线模式下，峰值约为多个任务的数据量叠加
- 转换成功后原始 align 数据会被删除

### 修改配置时的检查清单

- [ ] `excel_path` 指向正确的 Excel 文件
- [ ] `EXCEL_CONFIG` 中 sheet 名与 Excel 实际 sheet 名一致
- [ ] `robot_types` 与 Excel「设备名称」列的值完全匹配
- [ ] `MAX_COUNT` 是否满足生产 / 测试需求
- [ ] 各机型转换脚本文件存在于 `scripts_base_path`
- [ ] rclone 配置文件可正常访问对应 OBS 桶
- [ ] 输出目录 `/mnt/fastdisk/lerobotv30` 有足够磁盘空间

---

## 2. 各机型转换脚本（H5 → LeRobot v3.0）

`convert_all.py` 根据 Excel「设备名称」和 region 自动选择脚本；也可单独运行各机型脚本进行调试或手动转换。

### 机型与脚本映射

#### 默认机型脚本 `DEFAULT_CONVERT_SCRIPTS`

| 设备名称（Excel） | 转换脚本 | 备注 |
|-------------------|----------|------|
| 方舟无限arx-acone | `arx_loong_align2lerobotv30.py` | |
| 星尘智能S1 | `astribot_s1_align2lerobot_v30_no_norm.py` | 支持 `--cameras` |
| 松灵Aloha | `aloha_align2lerobot_v30_no_norm.py` | |
| UR5e | `ur5e_align2lerobot_v30_no_norm.py` | |
| Dwheel | `Dwheel_align2lerobotv30.py` | |
| Franka FR3 | `fr3_align2lerobotv30.py` | |
| 傅利叶GR2 / 傅利叶GR-2 | `gr2_align2lerobot_v30_no_norm.py` | |
| 乐聚KUAVO | `leju_align2lerobot_v30_no_norm.py` | |
| 天机 | `TIANJI_align2lerobotv30.py` | |
| 星海图R1 | `R1_align2lerobot_v30_no_norm.py` | |
| 智元A2 | `ZhiYuanA2_align2lerobotv30.py` | |
| 青龙ROS1 | `qinglongros1_align2lerobot_v30_no_norm.py` | |
| 青龙ROS2 | `qinglongros2_align2lerobot_v30_no_norm.py` | |
| 灵龙-h | `linglong_h_align2lerobotv30.py` | |

#### 智元 G1 按 region 选脚本 `G1_SCRIPTS`

| region | 转换脚本 |
|--------|----------|
| 郑州 | `zhengzhou_zhiyuan_G1_align2lerobotv30.py` |
| 上海 | `Genie1_align2lerobotv30.py` |

选择逻辑：

```python
def get_convert_script(robot_type, region):
    if robot_type == "智元G1":
        return G1_SCRIPTS[region]
    return DEFAULT_CONVERT_SCRIPTS[robot_type]
```

### 全部机型转换命令

`convert_all.py` 调用时固定传入 `--input`、`--output`、`--task`；单独调试时可参考下表（与 `DEFAULT_CONVERT_SCRIPTS` / `G1_SCRIPTS` 一致）：

| Excel 设备名称 | 脚本名称 | 转换命令 | 备注 |
|---|---|---|---|
| 方舟无限arx-acone | `arx_loong_align2lerobotv30.py` | `python arx_loong_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | H5 无 action，取下一帧 state；额外输出 velocity/effort/end |
| 星尘智能S1 | `astribot_s1_align2lerobot_v30_no_norm.py` | `python astribot_s1_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述" [--cameras head hand_left hand_right torso]` | 支持 `--cameras` |
| 松灵Aloha | `aloha_align2lerobot_v30_no_norm.py` | `python aloha_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` | no_norm |
| UR5e | `ur5e_align2lerobot_v30_no_norm.py` | `python ur5e_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` | no_norm |
| Dwheel | `Dwheel_align2lerobotv30.py` | `python Dwheel_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | |
| Franka FR3 | `fr3_align2lerobotv30.py` | `python fr3_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | 含末端位姿 |
| 傅利叶GR2 / 傅利叶GR-2 | `gr2_align2lerobot_v30_no_norm.py` | `python gr2_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` | no_norm |
| 乐聚KUAVO | `leju_align2lerobot_v30_no_norm.py` | `python leju_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` | no_norm；腿缺失补零 |
| 天机 | `TIANJI_align2lerobotv30.py` | `python TIANJI_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | |
| 星海图R1 | `R1_align2lerobot_v30_no_norm.py` | `python R1_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` | no_norm；task 优先从文件名提取 |
| 智元A2 | `ZhiYuanA2_align2lerobotv30.py` | `python ZhiYuanA2_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | 灵巧手 12 维 |
| 青龙ROS1 | `qinglongros1_align2lerobot_v30_no_norm.py` | `python qinglongros1_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` | no_norm |
| 青龙ROS2 | `qinglongros2_align2lerobot_v30_no_norm.py` | `python qinglongros2_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` | no_norm；子组缺失补零 |
| 灵龙-h | `linglong_h_align2lerobotv30.py` | `python linglong_h_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述" [--cameras head hand_left hand_right]` | no_norm；支持 `--cameras` |
| 智元G1（郑州） | `zhengzhou_zhiyuan_G1_align2lerobotv30.py` | `python zhengzhou_zhiyuan_G1_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | 由 region 自动选择 |
| 智元G1（上海） | `Genie1_align2lerobotv30.py` | `python Genie1_align2lerobotv30.py --input <H5目录> --output <输出目录> --task "任务描述"` | 由 region 自动选择 |

### 通用可选参数

各转换脚本均支持以下参数（`convert_all.py` 未显式传入时使用默认值）：

- `--input`：H5 数据目录，部分脚本也支持单个 `.h5` 文件
- `--output`：LeRobot v3.0 输出目录
- `--repo_id`：HuggingFace 仓库 ID，默认取输出目录名
- `--task`：任务描述，支持不加引号的多个词，自动拼接
- `--fps`：默认 30
- `--workers`：并行进程数，默认 8
- `--vcodec`：视频编码器，默认 `libsvtav1`（AV1）
- `--crf`：视频质量，默认 30

额外参数：

- **星尘智能 S1**：`--cameras`（可选 `head` / `hand_left` / `hand_right` / `torso` / `stereo`，默认前四个）
- **灵龙-h**：`--cameras`（可选 `head` / `hand_left` / `hand_right`，默认三个）

### 各机型 State/Action 规格

| Excel 设备名称 | robot_type | 维度 | 构成 | effector 处理 | 相机 |
|---|---|---|---|---|---|
| 方舟无限arx-acone | `arx_loong` | 14 | left_arm6+left_grip1+right_arm6+right_grip1 | action 末 2 维 > -0.5 置 0 | hand_left, hand_right, head |
| 星尘智能S1 | `AstribotS1` | 25 | arm14+爪2+头2+躯干4+底盘3 | clip [0,100] mm | 默认含 torso 共 4 个 |
| 松灵Aloha | `cobotmagic` | 20 | arm12+爪2+底盘角速度3+线速度3 | clip [0,0.08] m | head, hand_left, hand_right |
| UR5e | `DualUR5e` | 14 | arm12+爪2 | clip [0,100] | head, hand_left, hand_right |
| Dwheel | `Dwheel` | 20 | arm14+爪2+腰2+底盘2 | clip [0,0.09] | head, hand_left, hand_right |
| Franka FR3 | `fr3` | 30 | arm14+爪2+末端位置6+四元数8 | clip [0,1] | head, hand_left, hand_right |
| 傅利叶GR2 | `GR2` | 41 | arm14+灵巧手12+头2+腿12+腰1 | 按通道 clip（多数 [-1.3,0]，thumb_pitch [0,1]） | head_left, head_right |
| 乐聚KUAVO | `lejukuafu` | 30 | arm14+爪2+头2+腿12 | clip [0,100]；腿缺失补零 | head, hand_left, hand_right |
| 天机 | `TIANJI` | 16 | arm14+爪2 | clip [0,100] | head, hand_left, hand_right |
| 星海图R1 | `xinghaitu_r1` | 14 | arm12+爪2 | clip [0,100] | head, hand_left, hand_right |
| 智元A2 | `ZhiYuanA2` | 28 | arm14+灵巧手12+头2 | clip [0,2000] | head_front, chest_left, chest_right |
| 青龙ROS1 | `QinLongROS1` | 16 | arm14+爪2 | clip [0,90] | head, hand_left, hand_right |
| 青龙ROS2 | `QinLongROS2` | 33 | arm14+爪2+头2+腰3+腿12 | clip [0,90]；子组缺失补零 | head, hand_left, hand_right |
| 灵龙-h | `linglong_h` | 24 | arm14+爪2+头2+腰4+底盘2 | clip [0,1] | head, hand_left, hand_right |
| 智元G1（郑州） | `zhengzhou_zhiyuan_G1` | 16 | arm14+爪2 | state clip [0,120]；action clip [0,1] | head, hand_left, hand_right |
| 智元G1（上海） | `Genie1` | 16 | arm14+爪2 | state clip [35,120]；action clip [0,1] | head, hand_left, hand_right |

> **方舟无限 arx-acone** 除主 state/action（14 维 position）外，还输出 `observation.velocity`、`observation.effort`、`observation.end` 及对应 action 流，各 14 维。

### 转换脚本注意事项

- 运行时会**删除已存在的输出目录**（R1 在合并前删除），注意备份。
- 图像统一 resize 到 640×480，AV1（yuv420p, g=2）编码；无 PyAV 时降级为 FFmpeg 子进程。
- 转换流程：逐 episode 并行转换到 `<输出目录名>_separate_episodes/`，全部成功后 `merge_datasets` 合并为最终数据集并清理临时目录。
- **R1**：task 优先从文件名提取（`任务名_序列号_align.h5` 取下划线前段），无法提取时使用 `--task`。
- **方舟无限 arx-acone**：H5 无 `joints/action`，action 取下一帧 state，末帧丢弃。
- **智元 G1**：郑州与上海使用不同脚本，effector clip 范围也不同，由 `region` 自动区分。
- 带 `no_norm` 后缀的脚本保留原始物理量，不做额外归一化。

---

## 相关文件

| 文件 | 说明 |
|------|------|
| `convert_all.py` | 主批处理脚本 |
| `convert_all_status.txt` | 运行状态日志（Resume 依赖） |
| `数据转换第三批次20260513测试版本.xlsx` | 当前任务清单 |
| `*_align2lerobot*.py` | 各机型 LeRobot v3.0 转换脚本 |
| `analyze_lerobot_data.py` | 数据集统计分析（见 [README_analyze_lerobot_data.md](README_analyze_lerobot_data.md)） |
| `download_task.sh` | 单任务下载辅助脚本（非 convert_all 必需） |

---

## 注意事项

1. Excel 中「设备名称」必须与 `EXCEL_CONFIG["robot_types"]` 和 `DEFAULT_CONVERT_SCRIPTS` 的键名一致。
2. 「傅利叶GR2」/「傅利叶GR-2」对应脚本 `gr2_align2lerobot_v30_no_norm.py`（no_norm 版本，保留原始物理量）。
3. 智元 G1 在上海和郑州使用不同转换脚本，由 `region` 自动区分。
4. 当前 `MAX_COUNT = 2` 会限制每个任务只处理 2 条 episode，正式批跑前请改大。
5. 转换失败的任务不会删除本地 align 数据，可手动修复后配合 `--resume` 重试。
6. 带 `*_no_norm.py` 后缀的脚本（乐聚、傅利叶 GR2、青龙 ROS1/ROS2、松灵 Aloha、星尘 S1、星海图 R1、UR5e、灵龙-h）保留原始物理量；其余机型见上文「全部机型转换命令」与 State/Action 规格表。
