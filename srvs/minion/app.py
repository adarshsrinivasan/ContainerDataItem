from library.common.constants import HOST_ENV, PORT_ENV, CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, NODE_IP_ENV
from library.common.utils import getenv_with_default
from srvs.minion.db.cdi_minion_table_ops import init_cdi_minion_data_table
from srvs.minion.rpc_api.controller_client_api_handlers import ControllerClient
from srvs.minion.rpc_api.server_api_handlers import serve
import logging


def init_db():
    init_cdi_minion_data_table()


def register_with_controller(node_ip, host, port, controller_host, controller_port):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.RegisterMinion(node_ip=node_ip, rpc_ip=host, rpc_port=port)
    if response.err != "":
        raise Exception(
            f"Exception while registering minion with controller on {controller_host}:{controller_port}: {response.err}")


if __name__ == '__main__':
    logging.info("Starting Minion 0_0")
    node_ip = getenv_with_default(NODE_IP_ENV, "0.0.0.0")
    host = getenv_with_default(HOST_ENV, "0.0.0.0")
    port = getenv_with_default(PORT_ENV, "50001")
    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")

    init_db()
    register_with_controller(node_ip=node_ip, host=host, port=port, controller_host=controller_host,
                             controller_port=controller_port)

    serve(host, port)
