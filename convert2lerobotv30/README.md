# 七机型数据转换脚本（对齐 H5 → LeRobot v3.0）

本目录收录以下机型的转换脚本（均为 no_norm 版本，保留原始物理量）。
输入为已对齐的 h5 文件（`*_align.h5`，metadata ver 2.1.0），输出为 LeRobot v3.0 数据集。

## 机型 / 脚本 / 转换命令

| 机型 | 脚本名称 | 转换命令 |
|---|---|---|
| 乐聚（夸父） | `leju_align2lerobot_v30_no_norm.py` | `python leju_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` |
| 傅利叶 GR2 | `gr2_align2lerobot_v30_no_norm.py` | `python gr2_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` |
| 青龙 ROS1 | `qinglongros1_align2lerobot_v30_no_norm.py` | `python qinglongros1_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` |
| 青龙 ROS2 | `qinglongros2_align2lerobot_v30_no_norm.py` | `python qinglongros2_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` |
| 松灵 aloha（COBOTMAGIC V2.0） | `aloha_align2lerobot_v30_no_norm.py` | `python aloha_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` |
| 星尘 Astribot S1 | `astribot_s1_align2lerobot_v30_no_norm.py` | `python astribot_s1_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述" [--cameras head hand_left hand_right torso]` |
| 星海图 R1 | `R1_align2lerobot_v30_no_norm.py` | `python R1_align2lerobot_v30_no_norm.py --input <H5目录> --output <输出目录> --task "任务描述"` |
| UR5e（双臂） | `ur5e_align2lerobot_v30_no_norm.py` | `python ur5e_align2lerobot_v30_no_norm.py --input <H5目录或单个.h5> --output <输出目录> --task "任务描述"` |

## 通用可选参数

所有脚本共用：

- `--repo_id`：HuggingFace 仓库 ID，默认取输出目录名
- `--fps`：默认 30
- `--workers`：并行进程数，默认 8
- `--vcodec`：视频编码器，默认 `libsvtav1`（AV1）
- `--crf`：视频质量，默认 30
- `--task`：支持不加引号的多个词，自动拼接

仅 Astribot S1 额外支持 `--cameras`（可选 `head` / `hand_left` / `hand_right` / `torso` / `stereo`，默认前四个）。

## 各机型 State/Action 规格

| 机型 | robot_type | 维度 | 构成 | effector 处理 | 相机 |
|---|---|---|---|---|---|
| 乐聚 | `lejukuafu` | 30 | arm14+爪2+头2+腿12 | clip [0,100]；腿缺失补零 | head, hand_left, hand_right |
| 傅利叶 GR2 | `GR2` | 41 | arm14+灵巧手12+头2+腿12+腰1 | 按通道 clip（多数 [-1.3,0]，thumb_pitch [0,1]） | head_left, head_right |
| 青龙 ROS1 | `QinLongROS1` | 16 | arm14+爪2 | clip [0,90] | head, hand_left, hand_right |
| 青龙 ROS2 | `QinLongROS2` | 33 | arm14+爪2+头2+腰3+腿12 | clip [0,90]；子组缺失补零 | head, hand_left, hand_right |
| 松灵 aloha | `cobotmagic` | 20 | arm12+爪2+底盘角速度3+线速度3 | clip [0,0.08] m | head, hand_left, hand_right |
| 星尘 S1 | `AstribotS1` | 25 | arm14+爪2+头2+躯干4+底盘3 | clip [0,100] mm | 默认含 torso 共 4 个 |
| 星海图 R1 | `xinghaitu_r1` | 14 | arm12+爪2 | clip [0,100] | head, hand_left, hand_right |
| UR5e | `DualUR5e` | 14 | arm12+爪2 | clip [0,100] | head, hand_left, hand_right |

## 注意事项

- 所有脚本运行时会**删除已存在的输出目录**（R1 在合并前删除），注意备份。
- 图像统一 resize 到 640×480，AV1（yuv420p, g=2）编码；无 PyAV 时降级为 FFmpeg 子进程。
- 转换流程：逐 episode 并行转换到 `<输出目录名>_separate_episodes/`，全部成功后 `merge_datasets` 合并为最终数据集并清理临时目录。
- R1 脚本的 task 优先从文件名提取（`任务名_序列号_align.h5` 取下划线前段），无法提取时使用 `--task`。
