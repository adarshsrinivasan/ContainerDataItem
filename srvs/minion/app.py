import logging
import threading
import concurrent.futures
import time

import netifaces as ni

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, NODE_IP_ENV, \
    RPC_HOST_ENV, RPC_PORT_ENV, CONTAINER_NAME_ENV, CONTAINER_IP_ENV, CONTAINER_NAMESPACE_ENV, RDMA_HOST_ENV, \
    RDMA_PORT_ENV
from library.common.utils import getenv_with_default
from library.rdma.msq import IPCMsgQueue

from srvs.minion.db.cdi_minion_table_ops import init_cdi_minion_data_table
from srvs.minion.rdma.minion_rdma_ops import serve_rdma, handle_rdma_data
from srvs.minion.rpc_api.controller_client_api_handlers import register_with_controller
from srvs.minion.rpc_api.server_api_handlers import serve_rpc, _GRPC_MSG_SIZE
from library.shm.shm_lib import SHM_Test


def init_db():
    init_cdi_minion_data_table()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Minion 0_0")
    logging.info("Testing SHM ops")
    SHM_Test()

    node_ip = getenv_with_default(NODE_IP_ENV, "0.0.0.0")
    container_name = getenv_with_default(CONTAINER_NAME_ENV, "minion")
    container_ip = getenv_with_default(CONTAINER_IP_ENV, "0.0.0.0")
    container_namespace = getenv_with_default(CONTAINER_NAMESPACE_ENV, "default")

    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50001")
    rdma_host = ni.ifaddresses('net1')[ni.AF_INET][0]['addr']
    logging.info(f"Minion {container_name}: RDMA Host: {rdma_host}")
    rdma_port = getenv_with_default(RDMA_PORT_ENV, "12345")
    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")

    init_db()
    register_with_controller(name=container_name, namespace=container_namespace, node_ip=node_ip, host=container_ip,
                             port=rpc_port, controller_host=controller_host, controller_port=controller_port,
                             rdma_ip=rdma_host, rdma_port=rdma_port)

    # create IPC message queue
    msg_queue = IPCMsgQueue(1234)  # TODO: get from env?
    msq_id = msg_queue.get_queue()
    logging.info("Im here1 :)")

    t_msq = threading.Thread(target=msg_queue.receive_frame_from_queue, args=(_GRPC_MSG_SIZE, handle_rdma_data,)) # CreateCDIs
    t_rpc_server = threading.Thread(target=serve_rpc, args=(rpc_host, rpc_port))

    t_msq.start()
    t_rpc_server.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        while True:
            future = executor.submit(serve_rdma, rdma_host, rdma_port, msg_queue)
            if future.exception():
                logging.info("Server killed itself. Restarting in 5 seconds. \n")
                continue

