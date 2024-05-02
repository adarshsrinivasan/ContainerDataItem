import logging

import grpc
import srvs.common.rpc_api.minion_api_pb2_grpc as pb2_grpc
import srvs.common.rpc_api.minion_api_pb2 as pb2


class MinionClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.MinionControllerServiceStub(self.channel)

    def CreateCDIs(self, cdi_controller_table_list):
        logging.info(f"CreateCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_controller_table in cdi_controller_table_list:
            request_list.append(cdi_controller_table.as_proto_cdi_config())
        message = pb2.MinionCreateCDIsRequest(cdi_configs=request_list)
        return self.stub.CreateCDIs(message)

    def UpdateCDIs(self, cdi_controller_table_list):
        logging.info(f"UpdateCDIs({self.host}:{self.server_port}): Sending request")
        logging.error(f"UpdateCDIs: Updated: process_id: {cdi_controller_table_list[0].process_id}, process_name: {cdi_controller_table_list[0].process_name}, uid: {cdi_controller_table_list[0].uid}, gid: {cdi_controller_table_list[0].gid}, cdi_access_mode: {cdi_controller_table_list[0].cdi_access_mode}")
        request_list = []
        for cdi_controller_table in cdi_controller_table_list:
            request_list.append(cdi_controller_table.as_proto_cdi_config())
        message = pb2.MinionUpdateCDIsRequest(cdi_configs=request_list)
        return self.stub.UpdateCDIs(message)

    def TransferAndDeleteCDIs(self, transfer_host, transfer_port, cdi_controller_table_list):
        logging.info(f"TransferAndDeleteCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_controller_table in cdi_controller_table_list:
            request_list.append(cdi_controller_table.as_proto_cdi_config())
        message = pb2.MinionTransferAndDeleteCDIsRequest(transfer_host=transfer_host, transfer_port=transfer_port,
                                                   cdi_configs=request_list)
        return self.stub.TransferAndDeleteCDIs(message)

    def DeleteCDIs(self, cdi_controller_table_list):
        logging.info(f"DeleteCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_controller_table in cdi_controller_table_list:
            request_list.append(cdi_controller_table.as_proto_cdi_config())
        message = pb2.MinionDeleteCDIsRequest(cdi_configs=request_list)
        return self.stub.DeleteCDIs(message)
