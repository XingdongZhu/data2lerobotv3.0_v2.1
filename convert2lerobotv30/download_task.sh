task_ids=(
            "b6104a919ef14249966311b1ff8cd5a5"
        )
# 每个 task 最多下载多少个子文件夹
max_subfolders=2

# 上海的obs地址
obs_base="huawei-cloud:openloong-apps-prod-private/data-collector-svc/align"
conf="/root/.config/rclone/rclone_shanghai.conf"
# 郑州的obs地址
# obs_base='huawei-cloud:openloong-zhengzhou-apps-private/data-collector-svc/align'
# conf="/root/.config/rclone/rclone_zhengzhou.conf"
local="/mnt/fastdisk/align"

for i in "${!task_ids[@]}"; do
  tid="${task_ids[$i]}"
  echo "========================================"
  echo "[$(($i+1))/${#task_ids[@]}] Downloading: $tid"
  echo "========================================"
  mkdir -p "$local/$tid"

  # 列出远端 task 目录下所有子文件夹（目录会以 / 结尾）
  mapfile -t remote_dirs < <(
    rclone lsf --config "$conf" "$obs_base/$tid" --dirs-only | sed 's:/$::' | sort
  )

  total_dirs="${#remote_dirs[@]}"
  if [ "$total_dirs" -eq 0 ]; then
    echo "No subfolders found under $obs_base/$tid, skip."
    echo ""
    continue
  fi

  if [ "$total_dirs" -lt "$max_subfolders" ]; then
    download_count="$total_dirs"
  else
    download_count="$max_subfolders"
  fi

  echo "Found $total_dirs subfolders, will download first $download_count"

  for ((j=0; j<download_count; j++)); do
    subdir="${remote_dirs[$j]}"
    echo "  - [$((j+1))/$download_count] $subdir"
    rclone copy --config "$conf" "$obs_base/$tid/$subdir" "$local/$tid/$subdir" --transfers=16 -P
  done

  echo ""
done
echo "All downloads complete!"