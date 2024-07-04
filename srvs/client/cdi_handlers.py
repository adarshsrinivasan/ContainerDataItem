import logging

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV
from library.common.utils import getenv_with_default
from srvs.client.rpc_api.controller_client_api_handlers import ControllerClient
from library.common.cdi_config_model import populate_config_from_parent_config
from library.common.cdi_config_model import populate_config_from_parent_config, get_parent_config
import time

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")


def populate_and_transfer_cdis(config,data):
    logging.info("perform_cdi_ops: starting...")
    global controller_host, controller_port
    populate_config_from_parent_config(config=config)
    to_send_cdis = {}
    for cdi_key, cdi in config.cdis.items():
        cdi.clear_data()

        packed_data = f"{data}"
        logging.info(f"Write Start Time: {time.time()}")
        cdi.write_data(data=packed_data)
        logging.info(f"Write Finish Time: {time.time()}")
        
        to_send_cdis[0] = cdi
        break

    logging.info(f"perform_cdi_ops: Transferring CDI")
    config.cdis = to_send_cdis
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    logging.info(f"data {config.cdis[0]}")
    
    
    response = controller_client.TransferCDIs(config=config)
    logging.info(f"Finish Time: {time.time()}")
    if response.err != "":
        raise Exception(f"perform_cdi_ops: exception while transferring cdis: {response.err}")

def delete_cdis(controller_host, controller_port, process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.DeleteCDIs(process_id=process_id, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while deleting cdis: {response.err}")
    
def handle_cdi_create(controller_host, controller_port):
    parent_config = get_parent_config()
    if parent_config.create:
        controller_client = ControllerClient(host=controller_host, port=controller_port)
        response = controller_client.CreateCDIs(config=parent_config)
        if response.err != "":
            raise Exception(f"handle_cdi_create: exception while creating cdis: {response.err}")