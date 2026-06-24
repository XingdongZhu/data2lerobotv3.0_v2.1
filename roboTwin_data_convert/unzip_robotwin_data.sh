#!/usr/bin/env bash
set -euo pipefail

DATASET_ROOT="${1:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/dataset}"
OUTPUT_ROOT="${2:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/dataset_unzip}"

if [[ ! -d "$DATASET_ROOT" ]]; then
  echo "Error: dataset directory not found: $DATASET_ROOT" >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"

extracted=0
skipped=0
failed=0
no_zip_tasks=0

shopt -s nullglob
task_dirs=("$DATASET_ROOT"/*/)
shopt -u nullglob
total_tasks=${#task_dirs[@]}

task_idx=0
for task_dir in "${task_dirs[@]}"; do
  [[ -d "$task_dir" ]] || continue
  task_idx=$((task_idx + 1))
  task_name="$(basename "$task_dir")"
  output_task_dir="$OUTPUT_ROOT/$task_name"

  shopt -s nullglob
  zip_files=("$task_dir"*.zip)
  shopt -u nullglob
  total_zips=${#zip_files[@]}

  if [[ "$total_zips" -eq 0 ]]; then
    echo "[${task_idx}/${total_tasks}] [NO ZIP] $task_name"
    no_zip_tasks=$((no_zip_tasks + 1))
    continue
  fi

  mkdir -p "$output_task_dir"

  zip_idx=0
  for zip_file in "${zip_files[@]}"; do
    zip_idx=$((zip_idx + 1))
    zip_name="$(basename "$zip_file")"
    target_dir="$output_task_dir/${zip_name%.zip}"
    progress="[文件夹 ${task_idx}/${total_tasks}] [zip ${zip_idx}/${total_zips}]"

    if [[ -d "$target_dir" ]]; then
      echo "${progress} [SKIP] $task_name/$zip_name -> dataset_unzip/$task_name/${zip_name%.zip}/ already exists"
      skipped=$((skipped + 1))
      continue
    fi

    echo "${progress} 正在解压 $task_name/$zip_name -> dataset_unzip/$task_name/"
    if unzip -o -q "$zip_file" -d "$output_task_dir"; then
      extracted=$((extracted + 1))
    else
      echo "${progress} [FAIL] $task_name/$zip_name" >&2
      failed=$((failed + 1))
    fi
  done
done

echo
echo "Done."
echo "  source:    $DATASET_ROOT"
echo "  output:    $OUTPUT_ROOT"
echo "  extracted: $extracted"
echo "  skipped:   $skipped"
echo "  failed:    $failed"
echo "  no zip:    $no_zip_tasks tasks"

if [[ "$failed" -gt 0 ]]; then
  exit 1
fi
