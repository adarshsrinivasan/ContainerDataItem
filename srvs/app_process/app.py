import logging
import os

from library.common.constants import (NODE_IP_ENV, CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, PROCESS_ID_ENV,
                                      RPC_HOST_ENV, RPC_PORT_ENV, CONTAINER_NAME_ENV, CONTAINER_IP_ENV, \
                                      CONTAINER_NAMESPACE_ENV, CONFIG_PATH_ENV)
from library.common.utils import getenv_with_default
from srvs.app_process.rpc_api.controller_client_api_handlers import register_with_controller
from srvs.app_process.rpc_api.server_api_handler import serve_rpc
from srvs.app_process.cdi_handlers import parse_config, handle_cdi_create, handle_cdi_access

config = None

if __name__ == '__main__':
    global config
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Extractor 0_0")

    node_ip = getenv_with_default(NODE_IP_ENV, "0.0.0.0")
    process_id = getenv_with_default(PROCESS_ID_ENV, "0.0.0.0")

    container_name = getenv_with_default(CONTAINER_NAME_ENV, "minion")
    container_ip = getenv_with_default(CONTAINER_IP_ENV, "0.0.0.0")
    container_namespace = getenv_with_default(CONTAINER_NAMESPACE_ENV, "default")

    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50001")
    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
    config_path = getenv_with_default(CONFIG_PATH_ENV, "/etc/config/process_config.yaml")
    uid = os.getuid()
    gid = os.getgid()

    register_with_controller(process_id=process_id, name=container_name, namespace=container_namespace,
                             node_ip=node_ip, host=container_ip, port=rpc_port, controller_host=controller_host,
                             controller_port=controller_port, uid=uid, gid=gid)

    parse_config(config_path=config_path, process_id=process_id)
    handle_cdi_create(controller_host=controller_host, controller_port=controller_port)

    serve_rpc(rpc_host, rpc_port)

