import logging
import threading

import cv2
import paramiko
import time

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, PROCESS_ID_ENV
from library.common.utils import getenv_with_default
from library.common.cdi_config_model import Config
from srvs.client.cdi_handlers import populate_and_transfer_cdis

from srvs.client.rpc_api.controller_client_api_handlers import ControllerClient

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
process_id = getenv_with_default(PROCESS_ID_ENV, "extractor")


class Extractor(threading.Thread):
    def __init__(self, data = "", size = 0):

        threading.Thread.__init__(self)
        self.data = data
        self.size = size

    def run(self):
        self.extractor()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def get_video_from_path(self):
        return cv2.VideoCapture(self.local_fetch_file_path)

    def extractor(self):
        logging.info(f"Started Client data")
        # player = self.get_video_from_path()
        # assert player.isOpened()
        # frame_count = int(player.get(cv2.CAP_PROP_FRAME_COUNT))
        # x_shape = int(player.get(cv2.CAP_PROP_FRAME_WIDTH))
        # y_shape = int(player.get(cv2.CAP_PROP_FRAME_HEIGHT))
        # extractor_cache_obj = Extractor_cache_data(frame_count=frame_count, x_shape=x_shape, y_shape=y_shape,
        #                                            frame_order=0)
        # add_obj_to_cache(stream_id=self.stream_id, obj=extractor_cache_obj)

        # frame_order = 0
        # while True:
        #     ret, frame = player.read()
        #     if not ret:
        #         break
        #     frame_order += 1
        #     logging.info(f"Extracted frame {frame_order} out of {frame_count} for {self.stream_id}")
        #     add_frame_to_cache(stream_id=self.stream_id, frame_order=frame_order, frame=frame)
        # player.release()
        # cv2.destroyAllWindows()

        # submit_task_model = front_obj_of_cache_queue()
        # if submit_task_model is None:
        #     raise Exception("extractor: No task found in queue")
        # if current process is at the top, then start the call function.
        # if submit_task_model.stream_id == self.stream_id:
        controller_client = ControllerClient(host=controller_host, port=controller_port)
        response = controller_client.GetCDIsByProcessID(process_id=process_id)
        if response.err != "":
            raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
        cdi_config = Config()
        self.data = "x" * self.size
        logging.info(f"Size of Data: {len(self.data)}")
        to_send_cdis = {}
        logging.info(f"!!!!!!!!Create Start Time: {time.time()}")
        cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
        logging.info(f"!!!!!!!!!Create Finish Time: {time.time()}")
        
        for cdi_key, cdi in cdi_config.cdis.items():
            logging.info(f"data {cdi}")
            cdi.clear_data()

            packed_data = f"{self.data}"
            logging.info(f"!!!!!!!!Write Start Time: {time.time()}")
            cdi.write_data(data=packed_data)
            logging.info(f"!!!!!!!!!Write Finish Time: {time.time()}")
            to_send_cdis[0] = cdi
            break

        logging.info(f"perform_cdi_ops: Transferring CDI")
        cdi_config.cdis = to_send_cdis
        populate_and_transfer_cdis(config=cdi_config, data = self.data)
        logging.info(f"extractor: Done transferring frames ")

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
