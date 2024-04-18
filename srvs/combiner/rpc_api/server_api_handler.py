import logging

import grpc

import srvs.combiner.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.combiner.rpc_api.process_api_pb2 as pb2

from concurrent import futures

from library.common.constants import LOCAL_BUFFER_DIR_ENV, CACHE_DB_HOST_ENV, CACHE_DB_PORT_ENV, CACHE_DB_PWD_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.cache_ops import init_cache_client
from srvs.combiner.combiner import Combiner, upload_file

local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")
cache_host = getenv_with_default(CACHE_DB_HOST_ENV, "0.0.0.0")
cache_port = getenv_with_default(CACHE_DB_PORT_ENV, "50002")
cache_password = getenv_with_default(CACHE_DB_PWD_ENV, "password")

cache_client = init_cache_client(host=cache_host, port=cache_port, password=cache_password)


class ProcessService(pb2_grpc.ProcessServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, request, context):
        global cache_client
        logging.info(f"Received Payload.")
        payload = request.payload
        combiner_obj = Combiner(local_buffer_dir=local_buffer_dir, packed_data=payload, cache_client=cache_client)
        done = combiner_obj.combiner()
        if done:
            upload_file(combiner_obj.stream_id, combiner_obj.local_out_file_path, combiner_obj.remote_video_save_path,
                        combiner_obj.sftp_host, combiner_obj.sftp_port, combiner_obj.sftp_user, combiner_obj.sftp_pwd)
        return pb2.TransferPayloadResponse(err="")


def serve_rpc(rpc_host, rpc_port):
    logging.info(f"Starting RPC server on : {rpc_host}:{rpc_port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    server.start()
    server.wait_for_termination()
