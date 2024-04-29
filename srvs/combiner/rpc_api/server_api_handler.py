from datetime import datetime
import logging
import time

import grpc

import srvs.combiner.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.combiner.rpc_api.process_api_pb2 as pb2

from concurrent import futures
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2 as _health_pb2
from grpc_health.v1 import health_pb2_grpc as _health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from library.common.constants import LOCAL_BUFFER_DIR_ENV, CACHE_DB_HOST_ENV, CACHE_DB_PORT_ENV, CACHE_DB_PWD_ENV
from library.common.utils import getenv_with_default
from library.db.evaluation_db import update_finish_time
from srvs.combiner.cache_ops import init_cache_client
from srvs.combiner.combiner import Combiner, upload_file

_THREAD_POOL_SIZE = 256

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
            logging.info("Updating finish time")
            update_finish_time(stream_id=combiner_obj.stream_id, finish_time=datetime.now().time())
            upload_file(combiner_obj.stream_id, combiner_obj.local_out_file_path, combiner_obj.remote_video_save_path,
                        combiner_obj.sftp_host, combiner_obj.sftp_port, combiner_obj.sftp_user, combiner_obj.sftp_pwd)
        return pb2.TransferPayloadResponse(err="")


def _configure_maintenance_server(server: grpc.Server) -> None:
    # Create a health check servicer. We use the non-blocking implementation
    # to avoid thread starvation.
    health_servicer = health.HealthServicer(
        experimental_non_blocking=True,
        experimental_thread_pool=futures.ThreadPoolExecutor(
            max_workers=_THREAD_POOL_SIZE
        ),
    )

    # Create a tuple of all of the services we want to export via reflection.
    services = tuple(
        service.full_name
        for service in pb2.DESCRIPTOR.services_by_name.values()
    ) + (reflection.SERVICE_NAME, health.SERVICE_NAME)

    # Mark all services as healthy.
    _health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    for service in services:
        health_servicer.set(service, _health_pb2.HealthCheckResponse.SERVING)
    reflection.enable_server_reflection(services, server)


def serve_rpc(rpc_host, rpc_port):
    logging.info(f"Starting RPC server on : {rpc_host}:{rpc_port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    _configure_maintenance_server(server=server)

    server.start()
    server.wait_for_termination()
