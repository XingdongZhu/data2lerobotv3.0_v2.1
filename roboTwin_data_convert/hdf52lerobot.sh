data_dir=${1}
repo_id=${2}
output_root=${3:-${LEROBOT_OUTPUT_ROOT:-${HF_LEROBOT_HOME:-$HOME/.cache/huggingface/lerobot}}}

SVT_LOG=1 python3 scripts/convert_aloha_data_to_lerobot_robotwin.py \
  --raw_dir "$data_dir" \
  --repo_id "$repo_id" \
  --output-root "$output_root"
