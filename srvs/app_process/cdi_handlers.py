from srvs.app_process.rpc_api.controller_client_api_handlers import ControllerClient
from srvs.app_process.cdi_config_model import Config

parent_config = Config()


def parse_config(config_path, process_id):
    global parent_config
    parent_config.process_id = process_id
    parent_config.from_yaml(config_path)


def __populate_from_parent_config(config):
    for key, cdi in config.cdis:
        if key in parent_config.cdis.keys():
            parent_cdi = parent_config.cdis[key]
            cdi.destroy_if_no_new_data = parent_cdi.destroy_if_no_new_data
            cdi.create = parent_cdi.create
            cdi.transfer_id = parent_cdi.transfer_id
            cdi.transfer_mode = parent_cdi.transfer_mode


def get_next_data_block():
    # TODO: Implement
    return "Data"


def handle_cdi_access(config):
    # TODO: Implement
    for _, cdi in config.cdis:
        current_data = cdi.read_data()
        cdi.clear_data()
        processed_data = f"{current_data}\nProcessed"
        cdi.write_data(data=processed_data)


def handle_cdi_create(controller_host, controller_port):
    global parent_config
    new_config = Config()
    new_config.process_id = parent_config.process_id
    new_config.process_name = parent_config.process_name
    new_config.app_id = parent_config.app_id
    new_config.app_name = parent_config.app_name

    for _, cdi in parent_config.cdis.items():
        if cdi.create:
            new_config.cdis[cdi.cdi_id] = cdi

    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.CreateCDIs(config=new_config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while creating cdis: {response.err}")


def handle_transfer_cdis(controller_host, controller_port, process_id, transfer_process_id, transfer_mode, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.TransferCDIs(process_id=process_id, transfer_id=transfer_process_id,
                                              transfer_mode=transfer_mode, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while transferring cdis: {response.err}")


def delete_cdis(controller_host, controller_port, process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.DeleteCDIs(process_id=process_id, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while deleting cdis: {response.err}")
