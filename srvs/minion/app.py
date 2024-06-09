import logging

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, NODE_IP_ENV, \
    RPC_HOST_ENV, RPC_PORT_ENV, CONTAINER_NAME_ENV, CONTAINER_IP_ENV, CONTAINER_NAMESPACE_ENV
from library.common.utils import getenv_with_default

from srvs.minion.db.cdi_minion_table_ops import init_cdi_minion_data_table
from srvs.minion.rpc_api.controller_client_api_handlers import ControllerClient, register_with_controller
from srvs.minion.rpc_api.server_api_handlers import serve_rpc
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
    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")

    init_db()
    register_with_controller(name=container_name, namespace=container_namespace, node_ip=node_ip, host=container_ip,
                             port=rpc_port, controller_host=controller_host, controller_port=controller_port)

    serve_rpc(rpc_host, rpc_port)
