import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from huggingface_hub import list_repo_files, hf_hub_download

REPO_ID = "TianxingChen/RoboTwin2.0"
LOCAL_DIR = "/home/zxd/Downloads/roboTwin2.0"
MAX_WORKERS = 6

_print_lock = Lock()


def is_target(file_path):
    return (
        file_path.startswith("dataset/")
        and ("/aloha" in file_path or "/arx" in file_path)
        and file_path.endswith(".zip")
    )


def _log(msg):
    with _print_lock:
        print(msg)


def _task_zip_label(file_path):
    """dataset/<task>/xxx.zip -> <task>/xxx.zip"""
    rel = file_path.removeprefix("dataset/")
    task, zip_name = rel.split("/", 1)
    return f"{task}/{zip_name}"


def download_one(file_path):
    relative_path = file_path.replace("dataset/", "")
    local_path = os.path.join(LOCAL_DIR, relative_path)
    label = _task_zip_label(file_path)

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        _log(f"[SKIP] {label}")
        return "skip"

    _log(f"[DOWN] {label}")

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    downloaded_path = hf_hub_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        filename=file_path,
        local_dir=LOCAL_DIR,
        force_download=False,
    )

    if not os.path.exists(downloaded_path):
        raise FileNotFoundError(downloaded_path)

    if os.path.abspath(downloaded_path) != os.path.abspath(local_path):
        shutil.move(downloaded_path, local_path)

    return "ok"


def main():
    files = list_repo_files(repo_id=REPO_ID, repo_type="dataset")
    target_files = [f for f in files if is_target(f)]

    print(f"Total target files: {len(target_files)}")
    print(f"Parallel workers: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, fp): fp for fp in target_files}
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                future.result()
            except Exception as e:
                _log(f"[FAIL] {_task_zip_label(file_path)}: {e}")

    print("Done!")


if __name__ == "__main__":
    main()
