import argparse
import json
import logging

from library.common.constants import NODE_IP_ENV, CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, PROCESS_ID_ENV, HOST_ENV, \
    PORT_ENV
from library.common.utils import getenv_with_default
from srvs.app_process.cdi_config_model import Config
from srvs.app_process.rpc_api.controller_client_api_handlers import ControllerClient
from srvs.app_process.rpc_api.server_api_handler import serve


def register_with_controller(process_id, node_ip, host, port, controller_host, controller_port):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.RegisterProcess(process_id=process_id, node_ip=node_ip, rpc_ip=host, rpc_port=port)
    if response.err != "":
        raise Exception(
            f"Exception while registering process with controller on {controller_host}:{controller_port}: {response.err}")


if __name__ == '__main__':
    logging.info("Starting App Process 0_0")

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--config_file', nargs=1,
    #                     help="Config JSON file",
    #                     type=argparse.FileType('r'))
    # arguments = parser.parse_args()
    #
    # config_json = json.load(arguments.config_file[0])
    #
    # print(f"Parsed Config: {config_json}")
    # config = Config()
    # config.load_from_json(config_json)

    node_ip = getenv_with_default(NODE_IP_ENV, "0.0.0.0")
    process_id = getenv_with_default(PROCESS_ID_ENV, "0.0.0.0")

    host = getenv_with_default(HOST_ENV, "0.0.0.0")
    port = getenv_with_default(PORT_ENV, "50002")

    controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
    controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")

    register_with_controller(process_id=process_id, node_ip=node_ip, host=host, port=port,
                             controller_host=controller_host, controller_port=controller_port)
    serve(host, port)
