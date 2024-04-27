import logging
import threading

import cv2
import numpy as np
import paramiko
from library.db.evaluation_db import update_finish_time


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
    def __init__(self, local_buffer_dir="/tmp", packed_data="", cache_client=None):
        threading.Thread.__init__(self)

        self.local_buffer_dir = local_buffer_dir
        self.packed_data = packed_data
        self.cache_client = cache_client

        self.stream_id = ""
        self.remote_video_save_dir_path = ""
        self.remote_video_save_path = ""
        self.sftp_host = ""
        self.sftp_port = 22
        self.sftp_user = ""
        self.sftp_pwd = ""
        self.local_out_file_path = ""

        self.out = None
        self.frame_buffer = None
        self.done = False

    def run(self):
        self.combiner()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def upload_file(self):
        logging.info(f"Uploading Video for: {self.stream_id}")
        logging.info(
            f"local_out_file_path: '{self.local_out_file_path}'\nremote_video_save_dir_path: '{self.remote_video_save_path}'\nsftp_host: '{self.sftp_host}'\nsftp_port: '{self.sftp_port}'\nsftp_user: '{self.sftp_user}'\nsftp_pwd: '{self.sftp_pwd}'")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.sftp_host, port=self.sftp_port, username=self.sftp_user,
                           password=self.sftp_pwd, look_for_keys=False)
        sftp_client = ssh_client.open_sftp()

        try:
            sftp_client.put(self.local_out_file_path, self.remote_video_save_path)
        except FileNotFoundError as err:
            print(
                f"File: {self.remote_video_save_dir_path} was not found on the source server {self.sftp_host}:{self.sftp_port}: {err}")
        sftp_client.close()
        ssh_client.close()
        logging.info(f"Upload Video completed for: {self.stream_id}")

    def unpack_data(self):
        # packed_data = zlib.decompress(packed_data.encode('latin-1')).decode('latin-1')
        data_split = self.packed_data.split("\n")
        info_split = data_split[0].split(":")

        self.stream_id = info_split[0].strip()
        frame_count = int(info_split[1])
        frame_order = int(info_split[2])
        x_shape = int(info_split[3])
        y_shape = int(info_split[4])
        self.done = (info_split[5] == 'True')
        frame_data_type = info_split[6].strip()
        frame_shape_x = int(info_split[7])
        frame_shape_y = int(info_split[8])
        frame_shape_z = int(info_split[9])
        self.remote_video_save_dir_path = info_split[10].strip()
        self.sftp_host = info_split[11].strip()
        self.sftp_port = int(info_split[12])
        self.sftp_user = info_split[13].strip()
        self.sftp_pwd = info_split[14].strip()
        self.local_out_file_path = f"{self.local_buffer_dir}/processed_{self.stream_id}.avi"
        self.remote_video_save_path = f"{self.remote_video_save_dir_path}/processed_{self.stream_id}.avi"

        frame = np.fromstring(eval(data_split[1]), dtype=frame_data_type).reshape(frame_shape_x, frame_shape_y,
                                                                                  frame_shape_z)

        logging.info(f"Unpacked Payload: {self.stream_id}")

        return frame_count, x_shape, y_shape, frame_order, frame

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
        frame_count, x_shape, y_shape, frame_order, frame = self.unpack_data()
        logging.info(f"Started Processing Payload: {self.stream_id}. Done Status: {self.done}")
        if self.out is None:
            four_cc = cv2.VideoWriter_fourcc(*"MJPG")
            self.out = cv2.VideoWriter(self.local_out_file_path, four_cc, 20, (x_shape, y_shape))

        if not self.done:
            # try:
            #     cv2.imshow("Img", frame)
            # except Exception:
            #     pass
            # cv2.waitKey(1)
            key = f"{self.stream_id}_{frame_order}"
            self.cache_client.insert_data(key=key, value=self.pack_cache_data(frame=frame))
            logging.info(f"Added frame {frame_order + 1} out of {frame_count} with key {key} to cache")
            return
        logging.info(f"Received all the frames for the stream {self.stream_id}. Attempting to compile all the frames.")
        for i in range(0, frame_order):
            key = f"{self.stream_id}_{i}"
            frame = self.unpack_cache_data(packed_cache_data=self.cache_client.read_data(key=key))
            self.out.write(frame)
            cv2.waitKey(1)
            self.cache_client.delete_data(key=key)
        self.out.release()
        logging.info(f"Compiled all the frames for {self.stream_id}")
        return self.done
