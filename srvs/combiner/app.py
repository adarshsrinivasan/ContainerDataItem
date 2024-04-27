import logging
from library.common.constants import RPC_HOST_ENV, RPC_PORT_ENV, SERVER_TYPE_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.rpc_api.server_api_handler import serve_rpc
from srvs.combiner.rest_api.server_api_handler import serve_rest
from srvs.combiner.tcp_ip_api.server_api_handler import serve_tcp

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting combiner 0_0")

    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50002")
    server_type = getenv_with_default(SERVER_TYPE_ENV, "rest")

    server_handlers = {
        "rest": lambda: serve_rest(rest_host=rpc_host, rest_port=rpc_port),
        "tcp": lambda: serve_tcp(tcp_host=rpc_host, tcp_port=rpc_port),
        "grpc": lambda: serve_rpc(rpc_host=rpc_host, rpc_port=rpc_port)
    }

    handler = server_handlers.get(server_type)
    if handler:
        logging.info(f"Starting {server_type.upper()} server")
        handler()
    else:
        logging.error(f"Invalid server type: {server_type}")

if __name__ == '__main__':
    main()
