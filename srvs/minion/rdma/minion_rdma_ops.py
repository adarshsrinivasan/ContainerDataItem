import logging
import concurrent.futures

from library.common.utils import proto_pack_data, proto_unpack_data, write_string_to_file
from library.rdma.client import start_client
from library.rdma.server import start_server
from srvs.common.rpc_api import minion_api_pb2 as pb2
from srvs.common.rpc_api import controller_api_pb2 as cont_pb2
import concurrent.futures
from srvs.minion.common.cdi_ops_handlers import create_cdis

received_frame = 1
sent_frame = 1

class MinionRDMAClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

    def CreateCDIs(self, cdi_minion_table_list):
        global sent_frame
        logging.info(f"CreateCDIs({self.host}:{self.server_port}): Sending request")
        request_list = []
        for cdi_minion_table in cdi_minion_table_list:
            request_list.append(cdi_minion_table.as_proto_cdi_config())
        message = pb2.MinionCreateCDIsRequest(cdi_configs=request_list)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            for idx, _message in enumerate(message.cdi_configs):
                logging.info(f"sending message of len: {len(message.cdi_configs)} to client")
                _message_serialized = proto_pack_data(_message.process_id, _message.process_name, _message.app_id,
                                                      _message.app_name, _message.cdi_id, _message.cdi_key,
                                                      _message.cdi_size_bytes, _message.cdi_access_mode, _message.uid,
                                                      _message.gid, _message.payload)
                # _message_serialized = _message.SerializeToString()
                write_string_to_file(string=_message.payload, file_name=f"/tmp/sender_{sent_frame}")
                sent_frame += 1
                # test_cdi_config = cont_pb2.CdiConfig()
                # test_cdi_config.ParseFromString(_message_serialized.encode())
                logging.info(f"First time sending frame \n")
                client_future = executor.submit(start_client, self.host, self.server_port, _message_serialized.encode())
                if concurrent.futures.as_completed(client_future):
                    try:
                        res = client_future.result()
                        if res == -1:
                            while True:
                                logging.info(f"Retrying sending {idx}\n")
                                retry_client_fut = executor.submit(start_client, self.host, self.server_port,
                                                                   _message_serialized.encode())
                                if concurrent.futures.as_completed(retry_client_fut):
                                    if retry_client_fut.result() == -1:
                                        continue
                                    else:
                                        logging.info(f"Complete retrying msg: {idx}...\n")
                                        break
                        else:
                            logging.info(f"Received val of res = {res} \n")
                    except Exception as exc:
                        logging.error(f'Thread generated an exception: {exc}\n')
                        return f"Thread generated an exception: {exc}\n"
                    else:
                        logging.info(f'Successful :)\n')
                else:
                    logging.info(f"Not completed? \n")
            logging.info(f"Sending Done \n")
            client_future = executor.submit(start_client, self.host, self.server_port, b"Done")
            if concurrent.futures.as_completed(client_future):
                try:
                    res = client_future.result()
                    if res == -1:
                        while True:
                            logging.info(f"Retrying sending done \n")
                            retry_client_fut = executor.submit(start_client, self.host, self.server_port,
                                                               b"Done")
                            if concurrent.futures.as_completed(retry_client_fut):
                                if retry_client_fut.result() == -1:
                                    continue
                                else:
                                    logging.info(f"Complete retrying done: {idx}...\n")
                                    break
                    else:
                        logging.info(f"Received val of res done = {res} \n")
                except Exception as exc:
                    logging.error(f'Thread generated an exception: {exc}\n')
                    return f"Thread generated an exception: {exc}\n"
                else:
                    logging.info(f'Sending done successful :)\n')
            else:
                logging.info(f"Not completed done? \n")
        return ""


def serve_rdma(rdma_host, rdma_port, msq):
    logging.info(f"Starting RDMA server on : {rdma_host}:{rdma_port}")
    start_server(rdma_host, rdma_port, msq)


def handle_rdma_data(serialized_frames):
    global received_frame
    logging.info(f"received the frame: {len(serialized_frames)}")
    cdi_configs = []
    for idx, serialized_frame in enumerate(serialized_frames):
        cdi_config = cont_pb2.CdiConfig()
        # cdi_config.ParseFromString(serialized_frame)
        split_payload = proto_unpack_data(packed_data=serialized_frame.decode())
        cdi_config.process_id = split_payload[0]
        cdi_config.process_name = split_payload[1]
        cdi_config.app_id = split_payload[2]
        cdi_config.app_name = split_payload[3]
        cdi_config.cdi_id = split_payload[4]
        cdi_config.cdi_key = split_payload[5]
        cdi_config.cdi_size_bytes = split_payload[6]
        cdi_config.cdi_access_mode = split_payload[7]
        cdi_config.uid = split_payload[8]
        cdi_config.gid = split_payload[9]
        cdi_config.payload = split_payload[10]
        cdi_configs.append(cdi_config)

        write_string_to_file(string=cdi_config.payload, file_name=f"/tmp/receiver_{received_frame}")
        received_frame += 1

    request = pb2.MinionCreateCDIsRequest(cdi_configs=cdi_configs)
    logging.info(f"converted!")
    err = create_cdis(request=request)
    if err != "":
        logging.info(f"exception while creating cdi: {err}")
