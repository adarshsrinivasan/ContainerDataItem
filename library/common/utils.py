import os
import random
import string
import base64


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
    return base64.b64encode(decoded_payload.encode("ascii")).decode("ascii")


def decode_payload(encoded_payload):
    return base64.b64decode(encoded_payload).decode("ascii")


def get_kube_dns_url(node_ip, pod_ip, pod_namespace):
    # return f"{pod_ip}.{pod_namespace}..pod.cluster.local"
    return node_ip


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
    done = (info_split[5] == 'True')
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
