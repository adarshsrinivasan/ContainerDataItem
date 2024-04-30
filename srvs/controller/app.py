import logging

from srvs.controller.db.registered_minion_table_ops import init_registered_minion_table
from srvs.controller.db.registered_process_table_ops import init_registered_process_table
from srvs.controller.rpc_api.server_api_handler import serve_rpc
from srvs.controller.db.cdi_controller_table_ops import init_cdi_controller_table

from library.common.utils import getenv_with_default
from library.common.constants import RPC_HOST_ENV, RPC_PORT_ENV


def init_db():
    init_cdi_controller_table()
    init_registered_minion_table()
    init_registered_process_table()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Controller 0_0")

    init_db()
    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50000")
    serve_rpc(rpc_host, rpc_port)
