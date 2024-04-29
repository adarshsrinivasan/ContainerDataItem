from datetime import datetime
import logging
import socket
import json
import time
from library.db.evaluation_db import update_finish_time
from library.common.constants import LOCAL_BUFFER_DIR_ENV, CACHE_DB_HOST_ENV, CACHE_DB_PORT_ENV, CACHE_DB_PWD_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.cache_ops import init_cache_client
from srvs.combiner.combiner import Combiner, upload_file

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
            update_finish_time(stream_id=combiner_obj.stream_id, finish_time=datetime.now().time())
            upload_file(combiner_obj.stream_id, combiner_obj.local_out_file_path, combiner_obj.remote_video_save_path,
                        combiner_obj.sftp_host, combiner_obj.sftp_port, combiner_obj.sftp_user, combiner_obj.sftp_pwd)
        return {"err": ""}

def serve_tcp(tcp_host, tcp_port):
    logging.info(f"Starting TCP server on: {tcp_host}:{tcp_port}")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((tcp_host, int(tcp_port)))
    server_socket.listen(1)

    while True:
        client_socket, address = server_socket.accept()
        logging.info(f"Connected to client: {address}")

        try:
            data = ""
            while True:
                chunk = client_socket.recv(4096).decode('utf-8')
                if not chunk:
                    break
                data += chunk
                if data.endswith('\n'):
                    break

            payload = json.loads(data)["payload"]
            logging.info("Loaded Json")
            process_service = ProcessService()
            response = process_service.TransferPayload(payload)

            response_data = json.dumps(response).encode('utf-8')
            client_socket.send(response_data)
        except Exception as e:
            logging.error(f"Error processing payload: {str(e)}")
        finally:
            client_socket.close()