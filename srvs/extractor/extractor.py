import logging
import threading

import cv2
import paramiko

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, PROCESS_ID_ENV
from library.common.utils import getenv_with_default
from library.common.cdi_config_model import Config
from srvs.extractor.cdi_handlers import populate_and_transfer_cdis

from srvs.extractor.db.cache_ops import add_obj_to_cache, Extractor_cache_data, front_obj_of_cache_queue
from srvs.extractor.rpc_api.controller_client_api_handlers import ControllerClient

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
process_id = getenv_with_default(PROCESS_ID_ENV, "extractor")


class Extractor(threading.Thread):
    def __init__(self, stream_id="", local_fetch_file_path="/tmp/out.avi", remote_video_save_dir_path="",
                 remote_video_fetch_path="", sftp_host="0.0.0.0", sftp_port=22, sftp_user="sftpuser",
                 sftp_pwd="sftpuser"):

        threading.Thread.__init__(self)
        self.stream_id = stream_id
        self.local_fetch_file_path = local_fetch_file_path
        self.remote_video_save_dir_path = remote_video_save_dir_path
        self.remote_video_fetch_path = remote_video_fetch_path
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_user = sftp_user
        self.sftp_pwd = sftp_pwd

    def run(self):
        self.download_file()
        self.extractor()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def get_video_from_path(self):
        return cv2.VideoCapture(self.local_fetch_file_path)

    def extractor(self):
        logging.info(f"Started extracting frames for {self.stream_id}")
        player = self.get_video_from_path()
        assert player.isOpened()
        frame_count = int(player.get(cv2.CAP_PROP_FRAME_COUNT))
        x_shape = int(player.get(cv2.CAP_PROP_FRAME_WIDTH))
        y_shape = int(player.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_order = 0
        extractor_cache_obj = Extractor_cache_data(player=player, frame_count=frame_count,
                                                   x_shape=x_shape, y_shape=y_shape, frame_order=frame_order)
        add_obj_to_cache(stream_id=self.stream_id, obj=extractor_cache_obj)

        submit_task_model = front_obj_of_cache_queue()
        if submit_task_model is None:
            raise Exception("extractor: No task found in queue")
        # if current process is at the top, then start the call function.
        if submit_task_model.stream_id == self.stream_id:
            controller_client = ControllerClient(host=controller_host, port=controller_port)
            response = controller_client.GetCDIsByProcessID(process_id=process_id)
            if response.err != "":
                raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
            cdi_config = Config()
            cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
            populate_and_transfer_cdis(config=cdi_config)
            logging.info(f"extractor: Done transferring frames for {self.stream_id}")
            return
        logging.info(f"extractor: Added frames obj to queue for stream_id {self.stream_id}")

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
