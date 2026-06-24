#!/usr/bin/env bash
set -euo pipefail

# Convert RoboTwin HDF5 data to LeRobot format.
# Usage:
#   bash hdf52lerobot_robotwin.sh <hdf5_dir> <repo_id> [output_root] [robot_type]
#
# Examples:
#   bash hdf52lerobot_robotwin.sh processed_data/sim_aloha/task-50 task_repo
#   bash hdf52lerobot_robotwin.sh processed_data/sim_arx_x5/task-50 task_repo /path/to/output arx_x5
#   IMAGE_WRITER_PROCESSES=20 IMAGE_WRITER_THREADS=4 bash hdf52lerobot_robotwin.sh ...
#
# Environment:
#   LEROBOT_OUTPUT_ROOT      Default output root when output_root is omitted
#   ROBOTWIN_ROBOT_TYPE      Default robot type when robot_type is omitted (default: aloha)
#   IMAGE_WRITER_PROCESSES   Parallel image/video writer processes (default: 10)
#   IMAGE_WRITER_THREADS     Threads per writer process (default: 5)

data_dir=${1:?Usage: hdf52lerobot_robotwin.sh <hdf5_dir> <repo_id> [output_root] [robot_type]}
repo_id=${2:?Usage: hdf52lerobot_robotwin.sh <hdf5_dir> <repo_id> [output_root] [robot_type]}
output_root=${3:-${LEROBOT_OUTPUT_ROOT:-${HF_LEROBOT_HOME:-$HOME/.cache/huggingface/lerobot}}}
robot_type=${4:-${ROBOTWIN_ROBOT_TYPE:-aloha}}
image_writer_processes=${IMAGE_WRITER_PROCESSES:-10}
image_writer_threads=${IMAGE_WRITER_THREADS:-5}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SVT_LOG=1 python3 "${SCRIPT_DIR}/scripts/convert_robotwin_data_to_lerobot.py" \
  --raw_dir "$data_dir" \
  --repo_id "$repo_id" \
  --output-root "$output_root" \
  --robot-type "$robot_type" \
  --dataset-config.image-writer-processes "$image_writer_processes" \
  --dataset-config.image-writer-threads "$image_writer_threads"
