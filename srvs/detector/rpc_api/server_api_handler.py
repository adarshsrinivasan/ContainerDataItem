import logging

import grpc
from google.protobuf import reflection

import srvs.detector.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.detector.rpc_api.process_api_pb2 as pb2

from concurrent import futures
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2 as _health_pb2
from grpc_health.v1 import health_pb2_grpc as _health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from srvs.detector.detector import Object_Detector

_THREAD_POOL_SIZE = 256


class ProcessService(pb2_grpc.ProcessServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, request, context):
        logging.info("Received payload.")
        payload = request.payload
        object_detector_obj = Object_Detector(packed_data=payload)
        object_detector_obj.run()
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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_THREAD_POOL_SIZE))

    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    _configure_maintenance_server(server=server)

    server.start()
    server.wait_for_termination()
