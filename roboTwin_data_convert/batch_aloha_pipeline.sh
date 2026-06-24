#!/usr/bin/env bash
set -euo pipefail

# Pipeline: raw RoboTwin aloha data -> HDF5 -> LeRobot (per job).
# Usage:
#   ./batch_aloha_pipeline.sh              # run all 100 jobs
#   ./batch_aloha_pipeline.sh --dry-run    # print commands only
#   ./batch_aloha_pipeline.sh --task adjust_bottle

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="${ROBOTWIN_DATA_ROOT:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/dataset_unzip}"
HDF5_ROOT="${ROBOTWIN_HDF5_ROOT:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/processed_data/sim_aloha}"
LEROBOT_ROOT="${LEROBOT_OUTPUT_ROOT:-/qinglong_datasets/qinglong/lerobotv21/BAIHU_v3.1-p2_sim_robotwin2/sim_aloha}"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/batch_aloha_pipeline_${TIMESTAMP}.log"

DRY_RUN=false
SINGLE_TASK=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --task)
      SINGLE_TASK="${2:-}"
      if [[ -z "$SINGLE_TASK" ]]; then
        echo "Error: --task requires a task name" >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      cat <<EOF
Usage: $0 [--dry-run] [--task TASK_NAME]

Pipeline per job:
  1. robotwin2hdf5.sh  (raw -> HDF5 under ${HDF5_ROOT})
  2. hdf52lerobot.sh   (HDF5 -> LeRobot under ${LEROBOT_ROOT})

Options:
  --dry-run         Print jobs without running conversion
  --task TASK_NAME  Process only one task (e.g. adjust_bottle)

Environment:
  ROBOTWIN_DATA_ROOT     Raw data root (default: ${DATA_ROOT})
  ROBOTWIN_HDF5_ROOT     HDF5 output/input root (default: ${HDF5_ROOT})
  LEROBOT_OUTPUT_ROOT    LeRobot output root (default: ${LEROBOT_ROOT})
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$LOG_DIR"

extract_expert_data_num() {
  local setting="$1"
  local num="${setting##*_}"
  if [[ ! "$num" =~ ^[0-9]+$ ]]; then
    echo "Error: cannot parse expert_data_num from setting '$setting'" >&2
    return 1
  fi
  echo "$num"
}

run_pipeline() {
  local task_name="$1"
  local setting="$2"
  local expert_data_num="$3"
  local repo_id="${task_name}-${setting}-${expert_data_num}"
  local hdf5_dir="${HDF5_ROOT}/${repo_id}"

  echo "[$(date '+%F %T')] PIPELINE START task=${task_name} setting=${setting}" | tee -a "$LOG_FILE"

  if $DRY_RUN; then
    echo "  [1/2] bash ${SCRIPT_DIR}/robotwin2hdf5.sh ${task_name} ${setting} ${expert_data_num} ${DATA_ROOT} ${HDF5_ROOT}"
    echo "  [2/2] bash ${SCRIPT_DIR}/hdf52lerobot.sh ${hdf5_dir} ${repo_id} ${LEROBOT_ROOT}"
    return 0
  fi

  echo "  [1/2] raw -> HDF5: ${repo_id}" | tee -a "$LOG_FILE"
  if ! bash "${SCRIPT_DIR}/robotwin2hdf5.sh" \
      "$task_name" \
      "$setting" \
      "$expert_data_num" \
      "$DATA_ROOT" \
      "$HDF5_ROOT" >>"$LOG_FILE" 2>&1; then
    echo "[$(date '+%F %T')] FAIL step1 (HDF5) task=${task_name} setting=${setting} (see ${LOG_FILE})" | tee -a "$LOG_FILE" >&2
    return 1
  fi

  if [[ ! -d "$hdf5_dir" ]]; then
    echo "[$(date '+%F %T')] FAIL step1 output missing: ${hdf5_dir}" | tee -a "$LOG_FILE" >&2
    return 1
  fi

  echo "  [2/2] HDF5 -> LeRobot: ${repo_id}" | tee -a "$LOG_FILE"
  if ! bash "${SCRIPT_DIR}/hdf52lerobot.sh" \
      "$hdf5_dir" \
      "$repo_id" \
      "$LEROBOT_ROOT" >>"$LOG_FILE" 2>&1; then
    echo "[$(date '+%F %T')] FAIL step2 (LeRobot) repo_id=${repo_id} (see ${LOG_FILE})" | tee -a "$LOG_FILE" >&2
    return 1
  fi

  echo "[$(date '+%F %T')] OK   repo_id=${repo_id}" | tee -a "$LOG_FILE"
  return 0
}

total=0
ok=0
fail=0

echo "Raw data root:  ${DATA_ROOT}"
echo "HDF5 root:      ${HDF5_ROOT}"
echo "LeRobot root:   ${LEROBOT_ROOT}"
echo "Log file:       ${LOG_FILE}"
echo

if [[ -n "$SINGLE_TASK" ]]; then
  task_dirs=("${DATA_ROOT}/${SINGLE_TASK}")
  if [[ ! -d "${task_dirs[0]}" ]]; then
    echo "Error: task directory not found: ${task_dirs[0]}" >&2
    exit 1
  fi
else
  mapfile -t task_dirs < <(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
fi

for task_dir in "${task_dirs[@]}"; do
  task_name="$(basename "$task_dir")"

  mapfile -t settings < <(find "$task_dir" -mindepth 1 -maxdepth 1 -type d -name 'aloha*' | sort)
  if [[ ${#settings[@]} -eq 0 ]]; then
    echo "Skip ${task_name}: no aloha* settings found"
    continue
  fi

  for setting_dir in "${settings[@]}"; do
    setting="$(basename "$setting_dir")"
    expert_data_num="$(extract_expert_data_num "$setting")"
    total=$((total + 1))

    if run_pipeline "$task_name" "$setting" "$expert_data_num"; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
  done
done

echo
echo "Done. total=${total} ok=${ok} fail=${fail}"
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
