import logging
import os
import threading
import cv2
import numpy as np
import torch

from ultralytics import YOLO

from library.common.constants import OBJ_DET_MODEL_DIR_PATH_ENV
from library.common.utils import getenv_with_default, unpack_data, pack_data

model_dir_path = getenv_with_default(OBJ_DET_MODEL_DIR_PATH_ENV, "")
if model_dir_path != "":
    model_dir_path = os.path.join(model_dir_path, '')


class Object_Detector(threading.Thread):
    def __init__(self, packed_data=""):
        global model
        threading.Thread.__init__(self)

        self.packed_data = packed_data

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
        logging.info(f"object_detector: Starting Processing Payload..")
        stream_id, frame_count, frame_order, x_shape, y_shape, done, frame, remote_video_save_dir_path, sftp_host, sftp_port, sftp_user, sftp_pwd = unpack_data(
            packed_data=self.packed_data)
        if not done:
            logging.info(f"object_detector: Scoring frame {frame_order} out of {frame_count} for {stream_id}")
            results = self.score_frame(frame)
            logging.info(
                f"object_detector: Plotting boxes on frame {frame_order} out of {frame_count} for {stream_id}")
            frame = self.plot_boxes(results, frame)

        logging.info(f"Packing frame {frame_order} out of {frame_count} for {stream_id}")
        self.packed_data = pack_data(stream_id=stream_id, frame_count=frame_count, frame_order=frame_order,
                                     x_shape=x_shape, y_shape=y_shape, done=True, frame=frame,
                                     remote_video_save_dir_path=remote_video_save_dir_path, sftp_host=sftp_host,
                                     sftp_port=sftp_port, sftp_user=sftp_user, sftp_pwd=sftp_pwd)
        return self.packed_data
