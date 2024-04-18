import logging

import grpc

import srvs.object_detector.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.object_detector.rpc_api.process_api_pb2 as pb2

from concurrent import futures

from srvs.object_detector.object_detector import Object_Detector


class ProcessService(pb2_grpc.ProcessServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, request, context):
        logging.info("Received payload.")
        payload = request.payload
        object_detector_obj = Object_Detector(packed_data=payload)
        object_detector_obj.run()
        return pb2.TransferPayloadResponse(err="")


def serve_rpc(rpc_host, rpc_port):
    logging.info(f"Starting RPC server on : {rpc_host}:{rpc_port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    server.start()
    server.wait_for_termination()
