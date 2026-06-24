task_name=${1}
setting=${2}
expert_data_num=${3}
data_root=${4:-${ROBOTWIN_DATA_ROOT:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/dataset_unzip}}
output_root=${5:-${ROBOTWIN_OUTPUT_ROOT:-/qinglong_datasets/qinglong/HuggingFace/RoboTwin2.0/processed_data/sim_arx_x5}}

python3 scripts/process_data.py "$task_name" "$setting" "$expert_data_num" \
  --data-root "$data_root" \
  --output-root "$output_root"