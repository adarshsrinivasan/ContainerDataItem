import logging
import os
import random
import string
import base64

import numpy as np


def getenv_with_default(env_name, default):
    value = os.getenv(env_name)
    if value is None or len(value) == 0:
        value = default
    return value


def generate_data_of_size_kb(date_size_kb: int) -> str:
    size_bytes = date_size_kb * 1024

    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))

    return random_string


def encode_payload(decoded_payload):
    # return base64.b64encode(decoded_payload.encode("ascii")).decode("ascii")
    return decoded_payload


def decode_payload(encoded_payload):
    # return base64.b64decode(encoded_payload).decode("ascii")
    return encoded_payload


def get_kube_dns_url(node_ip="", pod_ip="", pod_namespace="", deploy_platform="kubernetes"):
    if deploy_platform == "kubernetes":
        # return f"{pod_ip}.{pod_namespace}.pod.cluster.local"
        return pod_ip
    return node_ip


def proto_pack_data(process_id, process_name, app_id, app_name, cdi_id, cdi_key, cdi_size_bytes, cdi_access_mode, uid,
                    gid, payload):
    sz = len(payload)
    info_str = f"{process_id}:{process_name}:{app_id}:{app_name}:{cdi_id}:{cdi_key}:{cdi_size_bytes}:{cdi_access_mode}:{uid}:{gid}:{sz}"
    packed_data = f"{info_str}\n{payload}"
    return packed_data


def proto_unpack_data(packed_data):
    data_split = packed_data.split("\n")
    info_split = data_split[0].split(":")

    process_id = info_split[0].strip()
    process_name = info_split[1].strip()
    app_id = info_split[2].strip()
    app_name = info_split[3].strip()
    cdi_id = info_split[4].strip()
    cdi_key = int(info_split[5])
    cdi_size_bytes = int(info_split[6])
    cdi_access_mode = int(info_split[7])
    uid = int(info_split[8])
    gid = int(info_split[9])
    sz = int(info_split[10])

    payload = f"{data_split[1]}\n{data_split[2]}"

    # cleanup extra characters
    payload = payload[:sz]
    # payload = payload.strip()
    # find_substr = "DSW'"
    # pos = payload.find(find_substr)
    # if pos == -1:
    #     find_substr = "DRX'"
    #     pos = payload.find(find_substr)
    # if pos != -1:
    #     payload = payload[:pos + len(find_substr)]

    return (process_id, process_name, app_id, app_name, cdi_id, cdi_key, cdi_size_bytes, cdi_access_mode, uid,
            gid, payload)


def pack_data(stream_id, frame_count, frame_order, x_shape, y_shape, done, frame, remote_video_save_dir_path,
              sftp_host, sftp_port, sftp_user, sftp_pwd):
    frame_shape = frame.shape
    frame_data_type = frame.dtype.name
    info_str = f"{stream_id}:{frame_count}:{frame_order}:{x_shape}:{y_shape}:{done}:{frame_data_type}:{frame_shape[0]}:{frame_shape[1]}:{frame_shape[2]}:{remote_video_save_dir_path}:{sftp_host}:{sftp_port}:{sftp_user}:{sftp_pwd} "
    frame_data_str = frame.flatten().tostring()

    packed_data = f"{info_str}\n{frame_data_str}"
    return packed_data


def unpack_data(packed_data):
    data_split = packed_data.split("\n")
    info_split = data_split[0].split(":")

    stream_id = info_split[0].strip()
    frame_count = int(info_split[1])
    frame_order = int(info_split[2])
    x_shape = int(info_split[3])
    y_shape = int(info_split[4])
    done = info_split[5] == 'True'
    frame_data_type = info_split[6].strip()
    frame_shape_x = int(info_split[7])
    frame_shape_y = int(info_split[8])
    frame_shape_z = int(info_split[9])
    remote_video_save_dir_path = info_split[10].strip()
    sftp_host = info_split[11].strip()
    sftp_port = int(info_split[12])
    sftp_user = info_split[13].strip()
    sftp_pwd = info_split[14].strip()

    frame = np.fromstring(eval(data_split[1]), dtype=frame_data_type).reshape(frame_shape_x, frame_shape_y,
                                                                              frame_shape_z)

    return (stream_id, frame_count, frame_order, x_shape, y_shape, done, frame, remote_video_save_dir_path, sftp_host,
            sftp_port, sftp_user, sftp_pwd)


def write_string_to_file(string, file_name):
    try:
        with open(file_name, 'w') as file:
            file.write(string)
        logging.info(f"String written to {file_name} successfully.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")
