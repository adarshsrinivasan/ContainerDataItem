import logging

import grpc

from srvs.common.rpc_api import controller_api_pb2_grpc as pb2_grpc, controller_api_pb2 as pb2


class ControllerClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.ControllerServiceStub(self.channel)

    def RegisterMinion(self, name, namespace, node_ip, rpc_ip, rpc_port, rdma_ip, rdma_port):
        logging.info(f"RegisterMinion({self.host}:{self.server_port}): Sending request")
        message = pb2.RegisterMinionRequest(name=name, namespace=namespace, node_ip=node_ip, rpc_ip=rpc_ip,
                                            rpc_port=rpc_port, rdma_ip=rdma_ip, rdma_port=rdma_port)
        return self.stub.RegisterMinion(message)

    def UnregisterMinion(self, node_ip):
        logging.info(f"UnregisterMinion({self.host}:{self.server_port}): Sending request")
        message = pb2.UnregisterMinionRequest(node_ip=node_ip)
        return self.stub.UnregisterMinion(message)


def register_with_controller(name, namespace, node_ip, host, port, controller_host, controller_port, rdma_ip, rdma_port):
    logging.info(f"Registering with controller on {controller_host}:{controller_port}")
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.RegisterMinion(name=name, namespace=namespace, node_ip=node_ip, rpc_ip=host,
                                                rpc_port=port, rdma_ip=rdma_ip, rdma_port=rdma_port)
    if response.err != "":
        raise Exception(
            f"Exception while registering minion with controller on {controller_host}:{controller_port}: {response.err}")
    logging.info(f"Successfully Registered with controller on {controller_host}:{controller_port}!")
