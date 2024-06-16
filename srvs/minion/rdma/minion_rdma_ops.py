import logging
from time import sleep
import threading
import concurrent.futures

from library.rdma.client import start_client
from library.rdma.server import start_server
from srvs.common.rpc_api import minion_api_pb2 as pb2
from srvs.common.rpc_api import controller_api_pb2 as cont_pb2
import concurrent.futures
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            for idx, _message in enumerate(message.cdi_configs):
                logging.info(f"sending message of len: {len(message.cdi_configs)} to client")
                _message_serialized = _message.SerializeToString()
                client_future = executor.submit(start_client, self.host, self.server_port, _message_serialized)
                if concurrent.futures.as_completed(client_future):
                    try:
                        res = client_future.result()
                        if res == -1:
                            while True:
                                logging.info(f"Retrying sending {idx}\n")
                                retry_client_fut = executor.submit(start_client, self.host, self.server_port, _message_serialized)
                                if concurrent.futures.as_completed(retry_client_fut):
                                    if retry_client_fut.result() == -1:
                                        continue
                                    else:
                                        logging.info(f"Complete retrying msg: {idx}...\n")
                                        break
                    except Exception as exc:
                        logging.error(f'Thread generated an exception: {exc}\n')
                        return f"Thread generated an exception: {exc}\n"
                    else:
                        logging.info(f'Successful :)\n')
            executor.submit(start_client, self.host, self.server_port, b"Done", )
        return ""

def serve_rdma(rdma_host, rdma_port, msq):
    logging.info(f"Starting RDMA server on : {rdma_host}:{rdma_port}")
    start_server(rdma_host, rdma_port, msq)


def handle_rdma_data(serialized_frames):
    logging.info(f"handle_frame: received the frame: {len(serialized_frames)}")
    cdi_configs = []
    for serialized_frame in serialized_frames:
        cdi_config = cont_pb2.CdiConfig()
        cdi_config.ParseFromString(serialized_frame)
        cdi_configs.append(cdi_config)

    request = pb2.MinionCreateCDIsRequest(cdi_configs=cdi_configs)
    logging.info(f"handle_frame: converted!")
    err = create_cdis(request=request)
    if err != "":
        logging.info(f"handle_rdma_data: exception while creating cdi: {err}")
