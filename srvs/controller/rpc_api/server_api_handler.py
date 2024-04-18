import grpc

import srvs.controller.rpc_api.controller_api_pb2_grpc as pb2_grpc
import srvs.controller.rpc_api.controller_api_pb2 as pb2

from concurrent import futures

from srvs.controller.db.cdi_controller_table_ops import CDI_Controller_Table
from srvs.controller.db.registered_minion_table_ops import Registered_Minion_Table
from srvs.controller.db.registered_process_table_ops import Registered_Process_Table
from srvs.controller.rpc_api.minion_client_api_handlers import MinionClient
from srvs.controller.rpc_api.process_client_api_handlers import ProcessClient


class ControllerService(pb2_grpc.ControllerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def RegisterProcess(self, request, context):
        process_table = Registered_Process_Table(process_id=request.id)
        # Check if we already registered the process
        result = process_table.get_by_process_id()
        process_table.node_ip = request.node_ip
        process_table.rpc_ip = request.rpc_ip
        process_table.rpc_port = request.rpc_port
        if result is None:
            # if not, then create the record
            process_table.insert()
        else:
            # if yes, then update the record
            process_table.update_by_process_id()
        return pb2.RegisterProcessResponse(err="")

    def UnregisterProcess(self, request, context):
        process_table = Registered_Process_Table(process_id=request.id)
        # Check if we have registered the process
        result = process_table.get_by_process_id()
        if result is None:
            # if not, then return an error
            err = f"Controller-UnregisterProcess: Couldn't find process with id: {process_table.process_id}"
            return pb2.UnregisterProcessResponse(err=err)
        # if yes, then delete the record
        process_table.delete_by_process_id()
        return pb2.UnregisterProcessResponse(err="")

    def RegisterMinion(self, request, context):
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        # Check if we already registered the minion for the node
        result = minion_table.get_by_node_ip()
        minion_table.rpc_ip = request.rpc_ip
        minion_table.rpc_port = request.rpc_port
        if result is None:
            # if not, then create the record
            minion_table.insert()
        else:
            # if yes, then update the record
            minion_table.update_by_node_ip()
        return pb2.RegisterMinionResponse(err="")

    def UnregisterMinion(self, request, context):
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        # Check if we already registered the minion for the node
        result = minion_table.get_by_node_ip()
        if result is None:
            # if not, then return an error
            err = f"Controller-UnregisterMinion: Couldn't find minion for node_ip: {minion_table.node_ip}"
            return pb2.UnregisterMinionResponse(err=err)
        # if yes, then delete the record
        minion_table.delete_by_node_ip()
        return pb2.UnregisterMinionResponse(err="")

    def CreateCDIs(self, request, context):
        # Fetch the process details to identify the node
        process_table = Registered_Process_Table(process_id=request.id)
        result = process_table.get_by_process_id()
        if result is None:
            # if process is not present, then return an error
            err = f"Controller-CreateCDIs: Couldn't find process with id: {process_table.process_id}"
            return pb2.CreateCDIsResponse(err=err)

        # Fetch the minion details deployed on the selected process the node
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        result = minion_table.get_by_node_ip()
        if result is None:
            # if minion is not present, then return an error
            err = f"Controller-CreateCDIs: Couldn't find minion for node_ip: {minion_table.node_ip}"
            return pb2.CreateCDIsResponse(err=err)

        # Prepare the request body - list of CDI config which need to be created on the node
        cdi_controller_table_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_controller_table = CDI_Controller_Table()
            cdi_controller_table.load_proto_cdi_config(cdi_config)
            cdi_controller_table_list.append(cdi_controller_table)

        # Create a client to interact with the minion and call the CreateCDIs api of the minion
        minion_client = MinionClient(host=minion_table.rpc_ip, port=minion_table.rpc_port)
        response = minion_client.CreateCDIs(cdi_controller_table_list)
        if response.err != "":
            # CDIs were not created by the minion
            err = f"Controller-CreateCDIs: exception from minion while creating CDIs: {response.err}"
            return pb2.CreateCDIsResponse(err=err)
        return pb2.CreateCDIsResponse(err="")

    def TransferCDIs(self, request, context):
        # Fetch the current process details to identify the node
        current_process_table = Registered_Process_Table(process_id=request.id)
        result = current_process_table.get_by_process_id()
        if result is None:
            # if current process is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find current process with id: {current_process_table.process_id}"
            return pb2.TransferCDIsResponse(err=err)

        # Fetch the next process details to identify the node
        next_process_table = Registered_Process_Table(process_id=request.transfer_id)
        result = next_process_table.get_by_process_id()
        if result is None:
            # if next process is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find next process with id: {next_process_table.process_id}"
            return pb2.TransferCDIsResponse(err=err)

        # Fetch the current minion details deployed on the current process the node
        current_minion_table = Registered_Minion_Table(node_ip=current_process_table.node_ip)
        result = current_minion_table.get_by_node_ip()
        if result is None:
            # if minion is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find current minion for node_ip: {current_minion_table.node_ip}"
            return pb2.TransferCDIsResponse(err=err)

        # Create a client to interact with the minion and call the CreateCDIs api of the minion
        current_minion_client = MinionClient(host=current_minion_table.rpc_ip, port=current_minion_table.rpc_port)

        # Prepare the request body - list of CDI config which need to be created on the node
        cdi_controller_table_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_controller_table = CDI_Controller_Table()
            cdi_controller_table.load_proto_cdi_config(cdi_config)
            cdi_controller_table_list.append(cdi_controller_table)

        if current_process_table.node_ip == next_process_table.node_ip:
            # Both the processes are on the same node. We just need to update CDI's permissions
            response = current_minion_client.UpdateCDIs(cdi_controller_table_list)
            if response.err != "":
                # CDIs were not updated by the minion
                err = f"Controller-TransferCDIs: exception from minion while updating CDIs: {response.err}"
                return pb2.TransferCDIsResponse(err=err)
        else:
            # Both the processes are on different nodes. We transfer the CDI from current to next node.
            # Fetch the nest minion details deployed on the nest process the node
            next_minion_table = Registered_Minion_Table(node_ip=next_process_table.node_ip)
            result = next_minion_table.get_by_node_ip()
            if result is None:
                # if minion is not present, then return an error
                err = f"Controller-TransferCDIs: Couldn't find next minion for node_ip: {next_minion_table.node_ip}"
                return pb2.TransferCDIsResponse(err=err)
            response = current_minion_client.TransferAndDeleteCDIs(next_minion_table.rpc_ip, next_minion_table.rpc_port,
                                                                   cdi_controller_table_list)
            if response.err != "":
                # CDIs were not transferred by the minion
                err = f"Controller-TransferCDIs: exception from minion while transferred CDIs: {response.err}"
                return pb2.TransferCDIsResponse(err=err)

            # Finally, notify the next process about the access transfer
            next_process_client = ProcessClient(host=next_process_table.rpc_ip, port=next_process_table.rpc_port)
            response = next_process_client.NotifyCDIsAccess(cdi_controller_table_list)
            if response.err != "":
                # CDIs access transfer was not notified to the next process
                err = f"Controller-TransferCDIs: exception from next process {next_process_table.process_id} while notifying access: {response.err}"
                return pb2.TransferCDIsResponse(err=err)
        return pb2.TransferCDIsResponse(err="")

    def DeleteCDIs(self, request, context):
        # Fetch the process details to identify the node
        process_table = Registered_Process_Table(process_id=request.id)
        result = process_table.get_by_process_id()
        if result is None:
            # if process is not present, then return an error
            err = f"Controller-DeleteCDIs: Couldn't find process with id: {process_table.process_id}"
            return pb2.DeleteCDIsResponse(err=err)

        # Fetch the minion details deployed on the selected process the node
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        result = minion_table.get_by_node_ip()
        if result is None:
            # if minion is not present, then return an error
            err = f"Controller-DeleteCDIs: Couldn't find minion for node_ip: {minion_table.node_ip}"
            return pb2.DeleteCDIsResponse(err=err)

        # Prepare the request body - list of CDI config which need to be created on the node
        cdi_controller_table_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_controller_table = CDI_Controller_Table()
            cdi_controller_table.load_proto_cdi_config(cdi_config)
            cdi_controller_table_list.append(cdi_controller_table)

        # Create a client to interact with the minion and call the CreateCDIs api of the minion
        minion_client = MinionClient(host=minion_table.rpc_ip, port=minion_table.rpc_port)
        response = minion_client.DeleteCDIs(cdi_controller_table_list)
        if response.err != "":
            # CDIs were not deleted by the minion
            err = f"Controller-DeleteCDIs: exception from minion while deleting CDIs: {response.err}"
            return pb2.DeleteCDIsResponse(err=err)
        return pb2.DeleteCDIsResponse(err="")


def serve(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ControllerServiceServicer_to_server(ControllerService(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    server.wait_for_termination()
