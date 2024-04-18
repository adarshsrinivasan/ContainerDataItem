import grpc

from srvs.app_process.rpc_api import controller_api_pb2_grpc as pb2_grpc, controller_api_pb2 as pb2


class ControllerClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.ControllerServiceStub(self.channel)

    def RegisterProcess(self, process_id, node_ip, rpc_ip, rpc_port):
        message = pb2.RegisterProcessRequest(id=process_id, node_ip=node_ip, rpc_ip=rpc_ip, rpc_port=rpc_port)
        return self.stub.RegisterProcess(message)

    def UnregisterProcess(self, process_id):
        message = pb2.UnregisterProcessRequest(id=process_id)
        return self.stub.UnregisterProcess(message)

    def CreateCDIs(self, process_id, config):
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.CreateCDIsRequest(id=process_id, cdi_configs=proto_controller_cdi_configs)
        return self.stub.CreateCDIs(message)

    def TransferCDIs(self, process_id, transfer_process_id, config):
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.TransferCDIsRequest(id=process_id, transfer_id=transfer_process_id, cdi_configs=proto_controller_cdi_configs)
        return self.stub.CreateCDIs(message)

    def DeleteCDIs(self, process_id, config):
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.DeleteCDIsRequest(id=process_id, cdi_configs=proto_controller_cdi_configs)
        return self.stub.DeleteCDIs(message)
