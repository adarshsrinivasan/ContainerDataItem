import logging
import threading

import cv2
import numpy as np
import paramiko

from library.common.utils import unpack_data
from srvs.combiner.db.cache_ops import add_frame_obj_to_cache, delete_frame_obj_from_cache, get_frame_obj_from_cache


def upload_file(stream_id, local_out_file_path, remote_video_save_path, sftp_host, sftp_port, sftp_user, sftp_pwd):
    logging.info(f"Uploading Video for: {stream_id}")
    logging.info(
        f"local_out_file_path: '{local_out_file_path}'\nremote_video_save_dir_path: '{remote_video_save_path}'\nsftp_host: '{sftp_host}'\nsftp_port: '{sftp_port}'\nsftp_user: '{sftp_user}'\nsftp_pwd: '{sftp_pwd}'")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=sftp_host, port=sftp_port, username=sftp_user,
                       password=sftp_pwd, look_for_keys=False)
    sftp_client = ssh_client.open_sftp()

    try:
        sftp_client.put(local_out_file_path, remote_video_save_path)
    except FileNotFoundError as err:
        print(
            f"File: {remote_video_save_path} was not found on the source server {sftp_host}:{sftp_port}: {err}")
    sftp_client.close()
    ssh_client.close()
    logging.info(f"Upload Video completed for: {stream_id}")


class Combiner(threading.Thread):
    def __init__(self, local_buffer_dir="/tmp", packed_data=""):
        threading.Thread.__init__(self)

        self.local_buffer_dir = local_buffer_dir
        self.packed_data = packed_data

        self.stream_id = ""
        self.local_out_file_path = ""
        self.remote_video_save_path = ""
        self.sftp_host = ""
        self.sftp_port = ""
        self.sftp_user = ""
        self.sftp_pwd = ""

        self.out = None
        self.frame_buffer = None
        self.done = False

    def run(self):
        self.combiner()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def pack_cache_data(self, frame):
        frame_shape = frame.shape
        frame_data_type = frame.dtype.name
        info_str = f"{frame_data_type}:{frame_shape[0]}:{frame_shape[1]}:{frame_shape[2]}"
        frame_data_str = frame.flatten().tostring()

        return f"{info_str}\n{frame_data_str}"

    def unpack_cache_data(self, packed_cache_data):
        data_split = packed_cache_data.split("\n")
        info_split = data_split[0].split(":")

        frame_data_type = info_split[0].strip()
        frame_shape_x = int(info_split[1])
        frame_shape_y = int(info_split[2])
        frame_shape_z = int(info_split[3])

        frame = np.fromstring(eval(data_split[1]), dtype=frame_data_type).reshape(frame_shape_x, frame_shape_y,
                                                                                  frame_shape_z)
        return frame

    def combiner(self):
        stream_id, frame_count, frame_order, x_shape, y_shape, done, frame, remote_video_save_dir_path, sftp_host, sftp_port, sftp_user, sftp_pwd = unpack_data(
            packed_data=self.packed_data)

        self.stream_id = stream_id
        self.local_out_file_path = f"{self.local_buffer_dir}/processed_{stream_id}.avi"
        self.remote_video_save_path = f"{remote_video_save_dir_path}/processed_{stream_id}.avi"
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_user = sftp_user
        self.sftp_pwd = sftp_pwd

        logging.info(f"combiner: Started Processing Payload: {stream_id}. Done Status: {done}")
        if not self.done:
            key = f"{stream_id}_{frame_order}"
            add_frame_obj_to_cache(key=key, frame_obj=self.pack_cache_data(frame=frame))
            logging.info(f"combiner: Added frame {frame_order + 1} out of {frame_count} with key {key} to cache")
            return

        if self.out is None:
            four_cc = cv2.VideoWriter_fourcc(*"MJPG")
            self.out = cv2.VideoWriter(self.local_out_file_path, four_cc, 20, (x_shape, y_shape))

        logging.info(f"combiner: Received all the frames for the stream {stream_id}. Attempting to compile all the frames.")
        for i in range(0, frame_order):
            key = f"{stream_id}_{i}"
            frame = self.unpack_cache_data(packed_cache_data=get_frame_obj_from_cache(key=key))
            self.out.write(frame)
            cv2.waitKey(1)
            delete_frame_obj_from_cache(key=key)
        self.out.release()
        logging.info(f"combiner: Compiled all the frames for {stream_id}")
        return self.done
