import logging
import os
import threading
import cv2
import numpy as np
import torch

from ultralytics import YOLO

from library.common.constants import NEXT_RPC_HOST_ENV, NEXT_RPC_PORT_ENV, OBJ_DET_MODEL_DIR_PATH_ENV
from library.common.utils import getenv_with_default
from srvs.detector.rpc_api.process_client_api_handlers import ProcessClient

next_rpc_host = getenv_with_default(NEXT_RPC_HOST_ENV, "0.0.0.0")
next_rpc_port = getenv_with_default(NEXT_RPC_PORT_ENV, "50002")

model_dir_path = getenv_with_default(OBJ_DET_MODEL_DIR_PATH_ENV, "")
if model_dir_path != "":
    model_dir_path = os.path.join(model_dir_path, '')

class Object_Detector(threading.Thread):
    def __init__(self, packed_data=""):
        global model
        threading.Thread.__init__(self)

        self.packed_data = packed_data

        self.stream_id = ""
        self.remote_video_save_dir_path = ""
        self.sftp_host = ""
        self.sftp_port = 22
        self.sftp_user = ""
        self.sftp_pwd = ""

        self.model = YOLO(f"{model_dir_path}yolov9c.pt")
        self.classes = self.model.names
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'

    def run(self):
        self.object_detector()

    def start_processing(self, daemonic=True):
        self.setDaemon(daemonic=daemonic)
        self.start()

    def load_model(self):
        model_dir_path = getenv_with_default(OBJ_DET_MODEL_DIR_PATH_ENV, "")
        if model_dir_path != "":
            model_dir_path = os.path.join(model_dir_path, '')
        model = YOLO(f"{model_dir_path}yolov9c.pt")
        logging.info(f"Loaded model from: {model_dir_path}yolov9c.pt")
        return model

    def pack_data(self, frame_count, x_shape, y_shape, frame_order, frame, done):
        frame_shape = frame.shape
        frame_data_type = frame.dtype.name
        info_str = f"{self.stream_id}:{frame_count}:{frame_order}:{x_shape}:{y_shape}:{done}:{frame_data_type}:{frame_shape[0]}:{frame_shape[1]}:{frame_shape[2]}:{self.remote_video_save_dir_path}:{self.sftp_host}:{self.sftp_port}:{self.sftp_user}:{self.sftp_pwd}"
        frame_data_str = frame.flatten().tostring()

        self.packed_data = f"{info_str}\n{frame_data_str}"
        # self.packed_data = zlib.compress(packed_data.encode('latin-1')).decode('latin-1')

    def unpack_data(self):
        # packed_data = zlib.decompress(packed_data.encode('latin-1')).decode('latin-1')
        data_split = self.packed_data.split("\n")
        info_split = data_split[0].split(":")

        self.stream_id = info_split[0].strip()
        frame_count = int(info_split[1])
        frame_order = int(info_split[2])
        x_shape = int(info_split[3])
        y_shape = int(info_split[4])
        done = (info_split[5] == 'True')
        frame_data_type = info_split[6].strip()
        frame_shape_x = int(info_split[7])
        frame_shape_y = int(info_split[8])
        frame_shape_z = int(info_split[9])
        self.remote_video_save_dir_path = info_split[10].strip()
        self.sftp_host = info_split[11].strip()
        self.sftp_port = int(info_split[12])
        self.sftp_user = info_split[13].strip()
        self.sftp_pwd = info_split[14].strip()

        frame = np.fromstring(eval(data_split[1]), dtype=frame_data_type).reshape(frame_shape_x, frame_shape_y,
                                                                                  frame_shape_z)
        logging.info(f"Unpacked Payload for {self.stream_id}")
        return frame_count, x_shape, y_shape, frame_order, frame, done

    def score_frame(self, frame):
        self.model.to(self.device)
        frame = [frame]
        result = self.model(frame)[0]
        if self.device == "cuda":
            labels, cord = np.array(result.boxes.cls.cuda(), dtype="int"), np.array(result.boxes.xyxy.cuda(),
                                                                                    dtype="int")
        else:
            labels, cord = np.array(result.boxes.cls.cpu(), dtype="int"), np.array(result.boxes.xyxy.cpu(), dtype="int")
        return labels, cord

    def class_to_label(self, x):
        return self.classes[int(x)]

    def plot_boxes(self, results, frame):
        labels, cord = results
        n = len(labels)
        for i in range(n):
            row = cord[i]
            x1, y1, x2, y2 = row
            bgr = (0, 0, 255)
            cv2.rectangle(frame, (x1, y1), (x2, y2), bgr, 2)
            cv2.putText(frame, self.class_to_label(labels[i]), (x1, y1), cv2.FONT_HERSHEY_SIMPLEX, 0.9,
                        bgr, 2)
        return frame

    def object_detector(self):
        logging.info(f"Starting Processing Payload..")
        frame_count, x_shape, y_shape, frame_order, frame, done = self.unpack_data()
        if not done:
            logging.info(f"Scoring frame {frame_order + 1} out of {frame_count} for {self.stream_id}")
            results = self.score_frame(frame)
            logging.info(f"Plotting boxes on frame {frame_order + 1} out of {frame_count} for {self.stream_id}")
            frame = self.plot_boxes(results, frame)

        logging.info(f"Packing frame {frame_order + 1} out of {frame_count} for {self.stream_id}")
        self.pack_data(frame_count=frame_count, x_shape=x_shape, y_shape=y_shape, frame_order=frame_order,
                       frame=frame, done=done)
        logging.info(f"Calling Combiner for frame {frame_order + 1} out of {frame_count} for {self.stream_id}")
        self.combiner()

    def combiner(self):
        global next_rpc_host, next_rpc_port
        combiner_client = ProcessClient(host=next_rpc_host, port=next_rpc_port)
        combiner_client.TransferPayload(payload=self.packed_data)
