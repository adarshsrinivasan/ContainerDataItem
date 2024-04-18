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

    def RegisterMinion(self, node_ip, rpc_ip, rpc_port):
        message = pb2.RegisterMinionRequest(node_ip=node_ip, rpc_ip=rpc_ip, rpc_port=rpc_port)
        return self.stub.RegisterMinion(message)

    def UnregisterMinion(self, node_ip):
        message = pb2.UnregisterMinionRequest(node_ip=node_ip)
        return self.stub.UnregisterMinion(message)