from srvs.app_process.rpc_api.controller_client_api_handlers import ControllerClient


def handle_notify_access(config):
    # TODO: Implement
    pass


def create_cdis(controller_host, controller_port, process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.CreateCDIs(process_id=process_id, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while creating cdis: {response.err}")


def transfer_cdis(controller_host, controller_port, process_id, transfer_process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.TransferCDIs(process_id=process_id, transfer_process_id=transfer_process_id,
                                              config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while transferring cdis: {response.err}")


def delete_cdis(controller_host, controller_port, process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.DeleteCDIs(process_id=process_id, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while deleting cdis: {response.err}")
