import logging
import os

from library.common.constants import (NODE_IP_ENV, CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, PROCESS_ID_ENV,
                                      RPC_HOST_ENV, RPC_PORT_ENV, CONTAINER_NAME_ENV, CONTAINER_IP_ENV, \
                                      CONTAINER_NAMESPACE_ENV, CONFIG_PATH_ENV, OBJ_DET_MODEL_DIR_PATH_ENV,
                                      CACHE_DB_HOST_ENV, CACHE_DB_PORT_ENV, CACHE_DB_PWD_ENV, LOCAL_BUFFER_DIR_ENV)
from library.common.utils import getenv_with_default
from library.common.cdi_config_model import parse_config
from srvs.combiner.db.cache_ops import init_cache_client
from srvs.combiner.rpc_api.controller_client_api_handlers import register_with_controller
from srvs.combiner.rpc_api.server_api_handler import serve_rpc

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d:%(funcName)s] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO)
    logging.info("Starting Combiner 0_0")

    node_ip = getenv_with_default(NODE_IP_ENV, "0.0.0.0")
    process_id = getenv_with_default(PROCESS_ID_ENV, "combiner")

    container_name = getenv_with_default(CONTAINER_NAME_ENV, "combiner")
    container_ip = getenv_with_default(CONTAINER_IP_ENV, "0.0.0.0")
    container_namespace = getenv_with_default(CONTAINER_NAMESPACE_ENV, "default")

    rpc_host = getenv_with_default(RPC_HOST_ENV, "0.0.0.0")
    rpc_port = getenv_with_default(RPC_PORT_ENV, "50001")
    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
    config_path = getenv_with_default(CONFIG_PATH_ENV, "/etc/config/process_config.yaml")
    uid = os.getuid()
    gid = os.getgid()
    cache_host = getenv_with_default(CACHE_DB_HOST_ENV, "0.0.0.0")
    cache_port = getenv_with_default(CACHE_DB_PORT_ENV, "50002")
    cache_password = getenv_with_default(CACHE_DB_PWD_ENV, "password")

    local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")

    init_cache_client(host=cache_host, port=cache_port, password=cache_password)

    model_dir_path = getenv_with_default(OBJ_DET_MODEL_DIR_PATH_ENV, "")

    register_with_controller(process_id=process_id, name=container_name, namespace=container_namespace,
                             node_ip=node_ip, host=container_ip, port=rpc_port, controller_host=controller_host,
                             controller_port=controller_port, uid=uid, gid=gid)

    parse_config(config_path=config_path, process_id=process_id)

    serve_rpc(rpc_host, rpc_port)
