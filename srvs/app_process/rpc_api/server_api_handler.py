import grpc

import srvs.controller.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.controller.rpc_api.process_api_pb2 as pb2

from concurrent import futures

from srvs.app_process.cdi_config_model import Config
from srvs.app_process.cdi_ops import handle_notify_access


class ProcessService(pb2_grpc.ProcessServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def NotifyCDIsAccess(self, request, context):
        config = Config()
        config.load_from_proto_controller_cdi_configs(request)
        handle_notify_access(config)
        return pb2.NotifyCDIsAccessResponse(err="")


def serve(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    server.wait_for_termination()
