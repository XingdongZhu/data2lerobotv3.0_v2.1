import os
import h5py
import numpy as np
import cv2
import argparse
import yaml
import json


def load_hdf5(dataset_path):
    if not os.path.isfile(dataset_path):
        print(f"Dataset does not exist at \n{dataset_path}\n")
        exit()

    with h5py.File(dataset_path, "r") as root:
        left_gripper, left_arm = (
            root["/joint_action/left_gripper"][()],
            root["/joint_action/left_arm"][()],
        )
        right_gripper, right_arm = (
            root["/joint_action/right_gripper"][()],
            root["/joint_action/right_arm"][()],
        )
        left_endpose = root["/endpose/left_endpose"][()]
        right_endpose = root["/endpose/right_endpose"][()]
        image_dict = dict()
        cam_params = dict()
        for cam_name in root["/observation/"].keys():
            rgb_path = f"/observation/{cam_name}/rgb"
            if rgb_path not in root:
                continue
            image_dict[cam_name] = root[rgb_path][()]
            cam_params[cam_name] = {
                "cam2world_gl": root[f"/observation/{cam_name}/cam2world_gl"][()],
                "extrinsic_cv": root[f"/observation/{cam_name}/extrinsic_cv"][()],
                "intrinsic_cv": root[f"/observation/{cam_name}/intrinsic_cv"][()],
            }

    return (
        left_gripper,
        left_arm,
        right_gripper,
        right_arm,
        left_endpose,
        right_endpose,
        image_dict,
        cam_params,
    )


def save_camera_params(h5_file, cam_params, camera_names, obs_slice):
    """Store all camera params in observations/cameras/, aligned with qpos (N, ...)."""
    obs_cams = h5_file["observations"].create_group("cameras")

    for cam_name in camera_names:
        params = cam_params[cam_name]
        obs_cam = obs_cams.create_group(cam_name)
        obs_cam.create_dataset(
            "cam2world_gl", data=params["cam2world_gl"][obs_slice].astype(np.float32)
        )
        obs_cam.create_dataset(
            "extrinsic_cv", data=params["extrinsic_cv"][obs_slice].astype(np.float32)
        )
        obs_cam.create_dataset(
            "intrinsic_cv", data=params["intrinsic_cv"][obs_slice].astype(np.float32)
        )


def build_state_vector(state, left_endpose, right_endpose):
    """28-dim vector: [joint(14), left_endpose(7), right_endpose(7)]."""
    return np.concatenate(
        [
            state.astype(np.float32),
            left_endpose.astype(np.float32),
            right_endpose.astype(np.float32),
        ]
    )


def decode_resize_image(image_bits):
    image = cv2.imdecode(np.frombuffer(image_bits, np.uint8), cv2.IMREAD_COLOR)
    return cv2.resize(image, (640, 480))


def images_encoding(imgs):
    encode_data = []
    padded_data = []
    max_len = 0
    for i in range(len(imgs)):
        success, encoded_image = cv2.imencode(".jpg", imgs[i])
        jpeg_data = encoded_image.tobytes()
        encode_data.append(jpeg_data)
        max_len = max(max_len, len(jpeg_data))
    # padding
    for i in range(len(imgs)):
        padded_data.append(encode_data[i].ljust(max_len, b"\0"))
    return encode_data, max_len


def get_task_config(task_name):
    with open(f"./task_config/{task_name}.yml", "r", encoding="utf-8") as f:
        args = yaml.load(f.read(), Loader=yaml.FullLoader)
    return args


def data_transform(path, episode_num, save_path):
    begin = 0
    floders = os.listdir(path)
    # assert episode_num <= len(floders), "data num not enough"

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for i in range(episode_num):

        desc_type = "seen"
        instruction_data_path = os.path.join(path, "instructions", f"episode{i}.json")
        with open(instruction_data_path, "r") as f_instr:
            instruction_dict = json.load(f_instr)
        instructions = instruction_dict[desc_type]
        save_instructions_json = {"instructions": instructions}

        os.makedirs(os.path.join(save_path, f"episode_{i}"), exist_ok=True)

        with open(
            os.path.join(os.path.join(save_path, f"episode_{i}"), "instructions.json"),
            "w",
        ) as f:
            json.dump(save_instructions_json, f, indent=2)

        (
            left_gripper_all,
            left_arm_all,
            right_gripper_all,
            right_arm_all,
            left_endpose_all,
            right_endpose_all,
            image_dict,
            cam_params,
        ) = load_hdf5(os.path.join(path, "data", f"episode{i}.hdf5"))
        camera_names = sorted(image_dict.keys())
        num_frames = left_gripper_all.shape[0]
        obs_slice = slice(0, num_frames - 1)
        qpos = []
        actions = []
        cam_images = {cam_name: [] for cam_name in camera_names}
        left_arm_dim = []
        right_arm_dim = []

        for j in range(0, left_gripper_all.shape[0]):

            left_gripper, left_arm, right_gripper, right_arm = (
                left_gripper_all[j],
                left_arm_all[j],
                right_gripper_all[j],
                right_arm_all[j],
            )

            state = np.array(left_arm.tolist() + [left_gripper] + right_arm.tolist() + [right_gripper])  # joints angle

            state = state.astype(np.float32)

            if j != left_gripper_all.shape[0] - 1:
                qpos.append(
                    build_state_vector(state, left_endpose_all[j], right_endpose_all[j])
                )

                for cam_name in camera_names:
                    cam_images[cam_name].append(
                        decode_resize_image(image_dict[cam_name][j])
                    )

            if j != 0:
                actions.append(
                    build_state_vector(state, left_endpose_all[j], right_endpose_all[j])
                )
                left_arm_dim.append(left_arm.shape[0])
                right_arm_dim.append(right_arm.shape[0])

        hdf5path = os.path.join(save_path, f"episode_{i}/episode_{i}.hdf5")

        with h5py.File(hdf5path, "w") as f:
            f.create_dataset("action", data=np.array(actions))
            obs = f.create_group("observations")
            obs.create_dataset("qpos", data=np.array(qpos))
            obs.create_dataset("left_arm_dim", data=np.array(left_arm_dim))
            obs.create_dataset("right_arm_dim", data=np.array(right_arm_dim))
            image = obs.create_group("images")
            for cam_name in camera_names:
                cam_enc, enc_len = images_encoding(cam_images[cam_name])
                image.create_dataset(cam_name, data=cam_enc, dtype=f"S{enc_len}")

            save_camera_params(f, cam_params, camera_names, obs_slice)

        begin += 1
        print(f"proccess {i} success!")

    return begin


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some episodes.")
    parser.add_argument(
        "task_name",
        type=str,
        default="beat_block_hammer",
        help="The name of the task (e.g., beat_block_hammer)",
    )
    parser.add_argument("setting", type=str)
    parser.add_argument(
        "expert_data_num",
        type=int,
        default=50,
        help="Number of episodes to process (e.g., 50)",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default=os.environ.get("ROBOTWIN_DATA_ROOT", "/home/zxd/Downloads/roboTwin2.0"),
        help="Root directory containing RoboTwin task folders",
    )
    parser.add_argument(
        "--output-root",
        type=str,
        default=os.environ.get("ROBOTWIN_OUTPUT_ROOT", "processed_data"),
        help="Root directory for processed HDF5 output",
    )
    args = parser.parse_args()

    task_name = args.task_name
    setting = args.setting
    expert_data_num = args.expert_data_num

    load_dir = os.path.join(args.data_root, str(task_name), str(setting))

    begin = 0
    print(f"read data from path:{load_dir}")

    target_dir = os.path.join(
        args.output_root, f"{task_name}-{setting}-{expert_data_num}"
    )
    print(f"save data to path:{target_dir}")
    begin = data_transform(
        load_dir,
        expert_data_num,
        target_dir,
    )
