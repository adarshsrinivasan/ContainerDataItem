import json
import logging
import uuid

from flask import Flask, request

from library.common.constants import LOCAL_BUFFER_DIR_ENV
from library.common.utils import getenv_with_default
from srvs.extractor.extractor import Extractor
from srvs.extractor.rest_api.submit_task_model import Submit_Task_Model

app = Flask(__name__)

API_URL_PREFIX = "/api/v1"
local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")


@app.route(f"{API_URL_PREFIX}/submitTask", methods=['POST'])
def submit_task():
    logging.info(f"Received task")
    submit_task_model = Submit_Task_Model().load_json(request=request.get_json())
    submit_task_model.stream_id = str(uuid.uuid4())
    logging.info(f"Assigning {submit_task_model.stream_id} as stream ID for the task")
    local_fetch_file_path = f"{local_buffer_dir}/fetched_{submit_task_model.stream_id}.mp4"

    extractor_obj = Extractor(stream_id=submit_task_model.stream_id, local_fetch_file_path=local_fetch_file_path,
                              remote_video_save_dir_path=submit_task_model.remote_video_save_dir_path,
                              remote_video_fetch_path=submit_task_model.remote_video_fetch_path,
                              sftp_host=submit_task_model.sftp_host, sftp_port=submit_task_model.sftp_port,
                              sftp_user=submit_task_model.sftp_user,
                              sftp_pwd=submit_task_model.sftp_pwd)
    extractor_obj.start_processing()
    return json.dumps({
        "stream_id": submit_task_model.stream_id,
        "output_file_path": f"{submit_task_model.remote_video_save_dir_path}processed_{submit_task_model.stream_id}.avi"}), 202


@app.route(f'{API_URL_PREFIX}/health')
def health_check():
    if all_required_services_are_running():
        return 'OK', 200
    else:
        return 'Service Unavailable', 500


def all_required_services_are_running():
    return True


def serve_rest(host, port):
    logging.info(f"Starting Rest on: {host}:{port}")
    app.run(host=host, port=port)
