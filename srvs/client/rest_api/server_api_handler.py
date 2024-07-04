import json
import logging
import uuid

from flask import Flask, request

from library.common.constants import LOCAL_BUFFER_DIR_ENV
from library.common.utils import getenv_with_default
from srvs.client.extractor import Extractor
from srvs.client.rest_api.submit_task_model import Submit_Task_Model

app = Flask(__name__)

API_URL_PREFIX = "/api/v1"
local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")

logging.getLogger('werkzeug').setLevel(logging.WARNING)
app.logger.setLevel(logging.WARNING)



@app.route(f"{API_URL_PREFIX}/submitTask", methods=['POST'])
def submit_task():
    logging.info(f"Received task")
    submit_task_model = Submit_Task_Model().load_json(request=request.get_json())
    logging.info(f"Assigning as stream ID for the task")
    # local_fetch_file_path = f"{local_buffer_dir}/fetched_{submit_task_model.stream_id}.mp4"

    extractor_obj = Extractor(data=submit_task_model.data, size = submit_task_model.size)
    extractor_obj.start_processing()
    return json.dumps({
        "data": submit_task_model.data}), 202


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
