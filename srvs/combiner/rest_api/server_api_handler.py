import logging
import time
from library.db.evaluation_db import update_finish_time
from flask import Flask, request, jsonify
from library.common.constants import LOCAL_BUFFER_DIR_ENV, CACHE_DB_HOST_ENV, CACHE_DB_PORT_ENV, CACHE_DB_PWD_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.cache_ops import init_cache_client
from srvs.combiner.combiner import Combiner, upload_file

app = Flask(__name__)

local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")
cache_host = getenv_with_default(CACHE_DB_HOST_ENV, "0.0.0.0")
cache_port = getenv_with_default(CACHE_DB_PORT_ENV, "50002")
cache_password = getenv_with_default(CACHE_DB_PWD_ENV, "password")
cache_client = init_cache_client(host=cache_host, port=cache_port, password=cache_password)

class ProcessService:
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, payload):
        global cache_client
        logging.info(f"Received Payload.")
        combiner_obj = Combiner(local_buffer_dir=local_buffer_dir, packed_data=payload, cache_client=cache_client)
        done = combiner_obj.combiner()
        if done:
            logging.info("Updating finish time")
            update_finish_time(stream_id=combiner_obj.stream_id, finish_time=time.time())
            upload_file(combiner_obj.stream_id, combiner_obj.local_out_file_path, combiner_obj.remote_video_save_path,
                        combiner_obj.sftp_host, combiner_obj.sftp_port, combiner_obj.sftp_user, combiner_obj.sftp_pwd)
        return {"err": ""}

@app.route("/transfer_payload", methods=["POST"])
def transfer_payload():
    payload = request.json["payload"]
    process_service = ProcessService()
    response = process_service.TransferPayload(payload)
    return jsonify(response)

def serve_rest(rest_host, rest_port):
    logging.info(f"Starting REST API server on: {rest_host}:{rest_port}")
    app.run(host=rest_host, port=rest_port)