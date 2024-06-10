import logging

import grpc

from library.rdma.client import start_client
from srvs.common.rpc_api import minion_api_pb2_grpc as pb2_grpc, minion_api_pb2 as pb2


class MinionClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.MinionControllerServiceStub(self.channel)

    def CreateCDIs(self, cdi_minion_table_list):
        logging.info(f"CreateCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_minion_table in cdi_minion_table_list:
            request_list.append(cdi_minion_table.as_proto_cdi_config())
        message = pb2.MinionCreateCDIsRequest(cdi_configs=request_list)
        return self.stub.CreateCDIs(message)


class MinionRDMAClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

    def CreateCDIs(self, cdi_minion_table_list):
        logging.info(f"CreateCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_minion_table in cdi_minion_table_list:
            request_list.append(cdi_minion_table.as_proto_cdi_config())
        message = pb2.MinionCreateCDIsRequest(cdi_configs=request_list)
        logging.info(f"message: {message}")
        for _message in message:
            logging.info(f"sending message of len: {len(message)} to client")
            start_client(self.host, self.server_port, _message)
