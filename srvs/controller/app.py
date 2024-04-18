from srvs.controller.db.registered_minion_table_ops import init_registered_minion_table
from srvs.controller.db.registered_process_table_ops import init_registered_process_table
from srvs.controller.rpc_api.server_api_handler import serve
from srvs.controller.db.cdi_controller_table_ops import init_cdi_controller_table

from library.common.utils import getenv_with_default
from library.common.constants import HOST_ENV, PORT_ENV


def init_db():
    init_cdi_controller_table()
    init_registered_minion_table()
    init_registered_process_table()


if __name__ == '__main__':
    init_db()
    host = getenv_with_default(HOST_ENV, "0.0.0.0")
    port = getenv_with_default(PORT_ENV, "50000")
    serve(host, port)
