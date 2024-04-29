from datetime import datetime
import logging
import threading
import time

import cv2
import numpy as np
import paramiko

from library.common.constants import NEXT_RPC_HOST_ENV, NEXT_RPC_PORT_ENV, SERVER_TYPE_ENV
from library.common.utils import getenv_with_default
from library.db.evaluation_db import add_start_time
from srvs.extractor.rpc_api.process_client_api_handlers import RPCProcessClient
from srvs.extractor.rest_api.process_client_api_handlers import RESTProcessClient
from srvs.extractor.tcp_ip_api.process_client_api_handlers import TCPProcessClient

next_rpc_host = getenv_with_default(NEXT_RPC_HOST_ENV, "0.0.0.0")
next_rpc_port = getenv_with_default(NEXT_RPC_PORT_ENV, "50002")


class Extractor(threading.Thread):
    def __init__(self, stream_id="", local_fetch_file_path="/tmp/out.avi", remote_video_save_dir_path="",
                 remote_video_fetch_path="", sftp_host="0.0.0.0", sftp_port=22, sftp_user="sftpuser",
                 sftp_pwd="sftpuser", packed_data=""):

        threading.Thread.__init__(self)
        self.stream_id = stream_id
        self.local_fetch_file_path = local_fetch_file_path
        self.remote_video_save_dir_path = remote_video_save_dir_path
        self.remote_video_fetch_path = remote_video_fetch_path
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_user = sftp_user
        self.sftp_pwd = sftp_pwd
        self.packed_data = packed_data

    def run(self):
        self.download_file()
        logging.info("Adding start time to the DB")
        add_start_time(stream_id=self.stream_id, start_time=datetime.now().time())
        self.extractor()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def get_video_from_path(self):
        return cv2.VideoCapture(self.local_fetch_file_path)

    def pack_data(self, frame_count, x_shape, y_shape, frame_order, frame, done):
        frame_shape = frame.shape
        frame_data_type = frame.dtype.name
        info_str = f"{self.stream_id}:{frame_count}:{frame_order}:{x_shape}:{y_shape}:{done}:{frame_data_type}:{frame_shape[0]}:{frame_shape[1]}:{frame_shape[2]}:{self.remote_video_save_dir_path}:{self.sftp_host}:{self.sftp_port}:{self.sftp_user}:{self.sftp_pwd} "
        frame_data_str = frame.flatten().tostring()

        self.packed_data = f"{info_str}\n{frame_data_str}"
        # self.packed_data = zlib.compress(packed_data.encode('latin-1')).decode('latin-1')

    def extractor(self):
        logging.info(f"Started extracting frames for {self.stream_id}")
        player = self.get_video_from_path()
        assert player.isOpened()
        frame_count = int(player.get(cv2.CAP_PROP_FRAME_COUNT))
        x_shape = int(player.get(cv2.CAP_PROP_FRAME_WIDTH))
        y_shape = int(player.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_order = 0
        while True:
            ret, frame = player.read()
            logging.info(f"Extracted frame {frame_order + 1} out of {frame_count} for {self.stream_id}")
            if not ret:
                frame = np.zeros(shape=(1, 1, 1), dtype='uint8')
                self.pack_data(frame_count=frame_count, x_shape=x_shape, y_shape=y_shape, frame_order=frame_order,
                               frame=frame, done=True)
                logging.info(f"Notifying Done for {self.stream_id} to Object Detector.")
                self.obj_detector()
                break
            logging.info(f"Passing frame {frame_order + 1} out of {frame_count} for {self.stream_id} to Object Detector.")
            self.pack_data(frame_count=frame_count, x_shape=x_shape, y_shape=y_shape, frame_order=frame_order,
                           frame=frame, done=False)
            self.obj_detector()
            frame_order += 1

        player.release()
        cv2.destroyAllWindows()
        logging.info(f"Done extracting frames for {self.stream_id}")

    def download_file(self):
        logging.info(f"Downloading video from {self.remote_video_fetch_path} with sftp for {self.stream_id}.")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.sftp_host, port=self.sftp_port, username=self.sftp_user,
                           password=self.sftp_pwd)
        sftp_client = ssh_client.open_sftp()

        try:
            sftp_client.get(self.remote_video_fetch_path, self.local_fetch_file_path)
        except FileNotFoundError as err:
            print(
                f"File: {self.remote_video_fetch_path} was not found on the source server {self.sftp_host}:{self.sftp_port}: {err}")
        sftp_client.close()
        logging.info(f"Download video completed for {self.stream_id}.")

    def obj_detector(self):
        global next_rpc_host, next_rpc_port
        
        server_type = getenv_with_default(SERVER_TYPE_ENV, "rest")

        process_clients = {
            "rest": lambda: RESTProcessClient(host=next_rpc_host, port=next_rpc_port),
            "tcp": lambda: TCPProcessClient(host=next_rpc_host, port=next_rpc_port),
            "grpc": lambda: RPCProcessClient(host=next_rpc_host, port=next_rpc_port)
        }

        combiner_client = process_clients.get(server_type)()
        if combiner_client:
            combiner_client.TransferPayload(payload=self.packed_data)
        else:
            logging.error(f"Invalid server type: {server_type}")
