import logging

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, LOCAL_BUFFER_DIR_ENV
from library.common.utils import getenv_with_default
from srvs.server.rpc_api.controller_client_api_handlers import ControllerClient
from library.common.cdi_config_model import populate_config_from_parent_config
import time

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")


def populate_and_transfer_cdis(config):
    logging.info("perform_cdi_ops: starting...")
    global controller_host, controller_port
    populate_config_from_parent_config(config=config)
    for cdi_key, cdi in config.cdis.items():
        packed_data = cdi.read_data()
        logging.info("Read")
    logging.info(f"Finish time {time.time()}")
    

    logging.info(f"perform_cdi_ops: Transferring CDI to process_id: {config.transfer_id}")
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.TransferCDIs(config=config)
    if response.err != "":
        raise Exception(f"perform_cdi_ops: exception while transferring cdis: {response.err}")
