import logging

from library.rdma.client import start_client
from library.rdma.server import start_server
from srvs.common.rpc_api import minion_api_pb2 as pb2
from srvs.minion.common.cdi_ops_handlers import create_cdis


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
        # logging.info(f"message: {message}")
        for _message in message.cdi_configs:
            logging.info(f"sending message of len: {len(message.cdi_configs)} to client")
            _message_serialized = _message.SerializeToString()
            start_client(self.host, self.server_port, _message_serialized)
        start_client(self.host, self.server_port, b"Done")


def serve_rdma(rdma_host, rdma_port, msq):
    logging.info(f"Starting RDMA server on : {rdma_host}:{rdma_port}")
    start_server(rdma_host, rdma_port, msq)


def handle_rdma_data(serialized_frames):
    logging.info(f"handle_frame: received the frame: {len(serialized_frames)}")
    cdi_configs = []
    for serialized_frame in serialized_frames:
        cdi_config = pb2.CdiConfig()
        cdi_config.ParseFromString(serialized_frame)
        cdi_configs.append(cdi_config)

    request = pb2.MinionCreateCDIsRequest(cdi_configs=cdi_configs)
    logging.info(f"handle_frame: converted!")
    # err = create_cdis(request=request)
    # logging.info(f"handle_rdma_data: exception while creating cdi: {err}")
