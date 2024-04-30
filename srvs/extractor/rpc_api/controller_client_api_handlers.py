import logging
import os

import grpc

from srvs.extractor.rpc_api import controller_api_pb2_grpc as pb2_grpc, controller_api_pb2 as pb2


class ControllerClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.ControllerServiceStub(self.channel)

    def RegisterProcess(self, process_id, name, namespace, node_ip, rpc_ip, rpc_port, uid, gid):
        logging.info(f"RegisterProcess({self.host}:{self.server_port}): Sending request")
        message = pb2.RegisterProcessRequest(id=process_id, name=name, namespace=namespace,
                                             node_ip=node_ip, rpc_ip=rpc_ip, rpc_port=rpc_port, uid=uid, gid=gid)
        return self.stub.RegisterProcess(message)

    def UnregisterProcess(self, process_id):
        logging.info(f"UnregisterProcess({self.host}:{self.server_port}): Sending request")
        message = pb2.UnregisterProcessRequest(id=process_id)
        return self.stub.UnregisterProcess(message)

    def CreateCDIs(self, config):
        logging.info(f"CreateCDIs({self.host}:{self.server_port}): Sending request")
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.CreateCDIsRequest(id=config.process_id, cdi_configs=proto_controller_cdi_configs)
        return self.stub.CreateCDIs(message)

    def GetCDIsByProcessID(self, process_id):
        logging.info(f"GetCDIsByProcessID({self.host}:{self.server_port}): Sending request")
        message = pb2.GetCDIsByProcessIDRequest(id=process_id)
        return self.stub.GetCDIsByProcessID(message)

    def TransferCDIs(self, config):
        logging.info(f"TransferCDIs({self.host}:{self.server_port}): Sending request")
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.TransferCDIsRequest(id=config.process_id, transfer_id=config.transfer_id,
                                          transfer_mode=config.transfer_mode, cdi_configs=proto_controller_cdi_configs)
        return self.stub.TransferCDIs(message)

    def DeleteCDIs(self, process_id, config):
        logging.info(f"DeleteCDIs({self.host}:{self.server_port}): Sending request")
        proto_controller_cdi_configs = config.to_proto_controller_cdi_configs()
        message = pb2.DeleteCDIsRequest(id=process_id, cdi_configs=proto_controller_cdi_configs)
        return self.stub.DeleteCDIs(message)


def register_with_controller(process_id, name, namespace, node_ip, host, port, uid, gid, controller_host, controller_port):
    logging.info(f"Registering with controller on {controller_host}:{controller_port}")
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.RegisterProcess(process_id=process_id, name=name, namespace=namespace,
                                                 node_ip=node_ip, rpc_ip=host, rpc_port=port, uid=uid, gid=gid)
    if response.err != "":
        raise Exception(
            f"Exception while registering process with controller on {controller_host}:{controller_port}: {response.err}")
    logging.info(f"Successfully Registered with controller on {controller_host}:{controller_port}!")

