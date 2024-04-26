import logging

from library.common.constants import RPC_HOST_ENV, RPC_PORT_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.rpc_api.server_api_handler import serve_rpc
from srvs.combiner.rest_api.server_api_handler import serve_rest

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting combiner 0_0")

    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50002")

    # serve_rpc(rpc_host=rpc_host, rpc_port=rpc_port)
    serve_rest(rest_host=rpc_host, rest_port=rpc_port)
