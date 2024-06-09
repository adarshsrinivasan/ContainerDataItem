import logging

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV, LOCAL_BUFFER_DIR_ENV
from library.common.utils import getenv_with_default
from srvs.combiner.combiner import Combiner, upload_file
from srvs.combiner.rpc_api.controller_client_api_handlers import ControllerClient
from library.common.cdi_config_model import populate_config_from_parent_config

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")
local_buffer_dir = getenv_with_default(LOCAL_BUFFER_DIR_ENV, "/tmp")


def populate_and_transfer_cdis(config):
    logging.info("perform_cdi_ops: starting...")
    global controller_host, controller_port
    populate_config_from_parent_config(config=config)
    done = False
    combiner_obj = Combiner()
    for cdi_key, cdi in config.cdis.items():
        packed_data = cdi.read_data()
        combiner_obj = Combiner(local_buffer_dir=local_buffer_dir, packed_data=packed_data)
        done = combiner_obj.combiner()
        cdi.clear_data()
        if done:
            break
    if done:
        upload_file(combiner_obj.stream_id, combiner_obj.local_out_file_path, combiner_obj.remote_video_save_path,
                    combiner_obj.sftp_host, combiner_obj.sftp_port, combiner_obj.sftp_user, combiner_obj.sftp_pwd)

    logging.info(f"perform_cdi_ops: Transferring CDI to process_id: {config.transfer_id}")
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.TransferCDIs(config=config)
    if response.err != "":
        raise Exception(f"perform_cdi_ops: exception while transferring cdis: {response.err}")
