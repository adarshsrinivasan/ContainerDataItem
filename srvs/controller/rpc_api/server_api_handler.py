import logging

import grpc

import srvs.common.rpc_api.controller_api_pb2_grpc as pb2_grpc
import srvs.common.rpc_api.controller_api_pb2 as pb2

from concurrent import futures
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2 as _health_pb2
from grpc_health.v1 import health_pb2_grpc as _health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from library.common.constants import CACHE_DB_HOST_ENV, DEPLOY_PLATFORM_ENV
from library.common.utils import get_kube_dns_url, getenv_with_default
from srvs.controller.db.cdi_controller_table_ops import CDI_Controller_Table
from srvs.controller.db.registered_minion_table_ops import Registered_Minion_Table
from srvs.controller.db.registered_process_table_ops import Registered_Process_Table
from srvs.controller.rpc_api.minion_client_api_handlers import MinionClient
from srvs.controller.rpc_api.process_client_api_handlers import ProcessClient

_THREAD_POOL_SIZE = 256

deploy_platform = getenv_with_default(DEPLOY_PLATFORM_ENV, "kubernetes")

class ControllerService(pb2_grpc.ControllerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def RegisterProcess(self, request, context):
        logging.info(f"RegisterProcess: Processing request from process {request.id}")
        process_table = Registered_Process_Table(process_id=request.id)
        # Check if we already registered the process
        result = process_table.get_by_process_id()
        process_table.name = request.name
        process_table.namespace = request.namespace
        process_table.node_ip = request.node_ip
        process_table.rpc_ip = request.rpc_ip
        process_table.rpc_port = request.rpc_port
        process_table.uid = request.uid
        process_table.gid = request.gid
        if result is None:
            logging.info(f"RegisterProcess: Creating entry for process {request.id}")
            # if not, then create the record
            process_table.insert()
        else:
            logging.info(f"RegisterProcess: Updating entry of process {request.id}")
            # if yes, then update the record
            process_table.update_by_process_id()
        return pb2.RegisterProcessResponse(err="")

    def UnregisterProcess(self, request, context):
        logging.info(f"UnregisterProcess: Processing request from process {request.id}")
        process_table = Registered_Process_Table(process_id=request.id)
        # Check if we have registered the process
        result = process_table.get_by_process_id()
        if result is None:
            logging.error(f"UnregisterProcess: Couldn't find process with id {request.id}")
            # if not, then return an error
            err = f"Controller-UnregisterProcess: Couldn't find process with id: {process_table.process_id}"
            return pb2.UnregisterProcessResponse(err=err)
        logging.info(f"UnregisterProcess: Deleting {request.id}")
        # if yes, then delete the record
        process_table.delete_by_process_id()
        return pb2.UnregisterProcessResponse(err="")

    def RegisterMinion(self, request, context):
        logging.info(f"RegisterMinion: Processing request from minion on node {request.node_ip}")
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        # Check if we already registered the minion for the node
        result = minion_table.get_by_node_ip()
        minion_table.name = request.name
        minion_table.namespace = request.namespace
        minion_table.rpc_ip = request.rpc_ip
        minion_table.rpc_port = request.rpc_port
        minion_table.rdma_ip = request.rdma_ip
        minion_table.rdma_port = request.rdma_port
        if result is None:
            logging.info(f"RegisterMinion: Creating entry for minion on node{request.node_ip}")
            # if not, then create the record
            minion_table.insert()
        else:
            logging.info(f"RegisterMinion: Updating entry of minion on node {request.node_ip}")
            # if yes, then update the record
            minion_table.update_by_node_ip()
        return pb2.RegisterMinionResponse(err="")

    def UnregisterMinion(self, request, context):
        logging.info(f"UnregisterMinion: Processing request from minion on node {request.node_ip}")
        minion_table = Registered_Minion_Table(node_ip=request.node_ip)
        # Check if we already registered the minion for the node
        result = minion_table.get_by_node_ip()
        if result is None:
            logging.error(f"UnregisterMinion: Couldn't find minion on node {request.node_ip}")
            # if not, then return an error
            err = f"Controller-UnregisterMinion: Couldn't find minion on node: {minion_table.node_ip}"
            return pb2.UnregisterMinionResponse(err=err)
        logging.info(f"UnregisterMinion: Deleting {request.node_ip}")
        # if yes, then delete the record
        minion_table.delete_by_node_ip()
        return pb2.UnregisterMinionResponse(err="")

    def CreateCDIs(self, request, context):
        logging.info(f"CreateCDIs: Processing request from process {request.id}")
        logging.info(f"CreateCDIs: Fetching process {request.id} record")
        # Fetch the process details to identify the node
        process_table = Registered_Process_Table(process_id=request.id)
        result = process_table.get_by_process_id()
        if result is None:
            logging.error(f"CreateCDIs: Couldn't find process {request.id}")
            # if process is not present, then return an error
            err = f"Controller-CreateCDIs: Couldn't find process with id: {process_table.process_id}"
            return pb2.CreateCDIsResponse(err=err)

        logging.info(f"CreateCDIs: Fetching minion on node {process_table.node_ip} record")
        # Fetch the minion details deployed on the selected process the node
        minion_table = Registered_Minion_Table(node_ip=process_table.node_ip)
        result = minion_table.get_by_node_ip()
        if result is None:
            logging.error(f"CreateCDIs: Couldn't find minion on node {process_table.node_ip}")
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
        minion_host = get_kube_dns_url(node_ip=minion_table.node_ip, pod_ip=minion_table.rpc_ip,
                                       pod_namespace=minion_table.namespace, deploy_platform=deploy_platform)
        minion_client = MinionClient(host=minion_host, port=minion_table.rpc_port)
        response = minion_client.CreateCDIs(cdi_controller_table_list)
        if response.err != "":
            logging.error(f"CreateCDIs: Exception while creating CDI with minion on node {process_table.node_ip} for process {request.id}: {response.err}")
            # CDIs were not created by the minion
            err = f"Controller-CreateCDIs: exception from minion while creating CDIs: {response.err}"
            return pb2.CreateCDIsResponse(err=err)
        for cdi_controller_table in cdi_controller_table_list:
            logging.info(f"CreateCDIs: Creating CDI record for the key {cdi_controller_table.cdi_key}")
            # create record in DB
            cdi_controller_table.insert()
        return pb2.CreateCDIsResponse(err="")

    def GetCDIsByProcessID(self, request, context):
        logging.info(f"GetCDIsByProcessID: Processing request from process {request.id}")
        logging.info(f"GetCDIsByProcessID: Fetching process {request.id} record")
        # Fetch the process details to identify the node
        process_table = Registered_Process_Table(process_id=request.id)
        result = process_table.get_by_process_id()
        if result is None:
            logging.error(f"GetCDIsByProcessID: Couldn't find process {request.id}")
            # if process is not present, then return an error
            err = f"Controller-GetCDIsByProcessID: Couldn't find process with id: {process_table.process_id}"
            return pb2.GetCDIsByProcessIDResponse(err=err)
        cdi_controller_table_list = CDI_Controller_Table(process_id=request.id).list_by_process_id()
        cdi_controller_proto_list = []
        for cdi_controller_table in cdi_controller_table_list:
            cdi_controller_proto_list.append(cdi_controller_table.as_proto_cdi_config())

        return pb2.GetCDIsByProcessIDResponse(cdi_configs=cdi_controller_proto_list, err="")

    def TransferCDIs(self, request, context):
        logging.info(f"TransferCDIs: Processing request from process {request.id}")
        logging.info(f"TransferCDIs: Fetching current process {request.id} record")
        # Fetch the current process details to identify the node
        current_process_table = Registered_Process_Table(process_id=request.id)
        result = current_process_table.get_by_process_id()
        if result is None:
            logging.error(f"TransferCDIs: Exception while fetching current process {request.id}")
            # if current process is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find current process with id: {current_process_table.process_id}"
            return pb2.TransferCDIsResponse(err=err)

        logging.info(f"TransferCDIs: Fetching next process {request.transfer_id} record")
        # Fetch the next process details to identify the node
        next_process_table = Registered_Process_Table(process_id=request.transfer_id)
        result = next_process_table.get_by_process_id()
        if result is None:
            logging.error(f"TransferCDIs: Exception while fetching next process {request.transfer_id}")
            # if next process is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find next process with id: {next_process_table.process_id}"
            return pb2.TransferCDIsResponse(err=err)

        logging.info(f"TransferCDIs: Fetching current minion on node {current_process_table.node_ip} record")
        # Fetch the current minion details deployed on the current process the node
        current_minion_table = Registered_Minion_Table(node_ip=current_process_table.node_ip)
        result = current_minion_table.get_by_node_ip()
        if result is None:
            logging.error(f"TransferCDIs: Exception while fetching current minion on node {current_process_table.node_ip}")
            # if minion is not present, then return an error
            err = f"Controller-TransferCDIs: Couldn't find current minion for node_ip: {current_minion_table.node_ip}"
            return pb2.TransferCDIsResponse(err=err)

        # Create a client to interact with the minion and call the CreateCDIs api of the minion
        current_minion_host = get_kube_dns_url(node_ip=current_minion_table.node_ip, pod_ip=current_minion_table.rpc_ip,
                                               pod_namespace=current_minion_table.namespace, deploy_platform=deploy_platform)
        current_minion_client = MinionClient(host=current_minion_host, port=current_minion_table.rpc_port)

        # Prepare the request body - list of CDI config which need to be created on the node
        cdi_controller_table_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_controller_table = CDI_Controller_Table()
            cdi_controller_table.load_proto_cdi_config(cdi_config)
            cdi_controller_table.process_id = next_process_table.process_id
            cdi_controller_table.process_name = next_process_table.name
            cdi_controller_table.uid = next_process_table.uid
            cdi_controller_table.gid = next_process_table.gid
            cdi_controller_table.cdi_access_mode = int(request.transfer_mode)
            cdi_controller_table_list.append(cdi_controller_table)
            logging.error(f"TransferCDIs: Updated: process_id: {cdi_controller_table_list[0].process_id}, process_name: {cdi_controller_table_list[0].process_name}, uid: {cdi_controller_table_list[0].uid}, gid: {cdi_controller_table_list[0].gid}, cdi_access_mode: {cdi_controller_table_list[0].cdi_access_mode}")

        if current_process_table.node_ip == next_process_table.node_ip:
            logging.info(f"TransferCDIs: Current process {current_process_table.process_id} and next process {next_process_table.process_id} are on the same node.")
            # Both the processes are on the same node. We just need to update CDI's permissions
            response = current_minion_client.UpdateCDIs(cdi_controller_table_list)
            if response.err != "":
                logging.error(f"TransferCDIs: Exception while updating CDI with minion on node {current_process_table.node_ip}: {response.err}")
                # CDIs were not updated by the minion
                err = f"Controller-TransferCDIs: exception from minion while updating CDIs: {response.err}"
                return pb2.TransferCDIsResponse(err=err)
        else:
            logging.info(f"TransferCDIs: Current process {current_process_table.process_id} and next process {next_process_table.process_id} are on different nodes.")
            # Both the processes are on different nodes. We transfer the CDI from current to next node.
            # Fetch the next minion details deployed on the nest process the node
            next_minion_table = Registered_Minion_Table(node_ip=next_process_table.node_ip)
            result = next_minion_table.get_by_node_ip()
            if result is None:
                logging.error(f"TransferCDIs: Exception while fetching next minion on node {current_process_table.node_ip}")
                # if minion is not present, then return an error
                err = f"Controller-TransferCDIs: Couldn't find next minion for node_ip: {next_minion_table.node_ip}"
                return pb2.TransferCDIsResponse(err=err)
            # next_minion_host = get_kube_dns_url(node_ip=next_minion_table.node_ip, pod_ip=next_minion_table.rpc_ip,
            #                                     pod_namespace=next_minion_table.namespace, deploy_platform=deploy_platform)
            next_minion_host = next_minion_table.rdma_ip
            next_minion_port = str(next_minion_table.rdma_port)
            response = current_minion_client.TransferAndDeleteCDIs(
                transfer_host=next_minion_host,
                transfer_port=next_minion_port,
                cdi_controller_table_list=cdi_controller_table_list)
            if response.err != "":
                logging.error(f"TransferCDIs: Exception while transferring CDIs from minion on node {current_process_table.node_ip} to minion on node {next_process_table.node_ip}: {response.err}")
                # CDIs were not transferred by the minion
                err = f"Controller-TransferCDIs: exception from minion while transferred CDIs: {response.err}"
                return pb2.TransferCDIsResponse(err=err)

        logging.info(f"TransferCDIs: Notifying next process {next_process_table.process_id} about CDI access.")
        # Finally, notify the next process about the access transfer
        #next_process_host = get_kube_dns_url(pod_ip=next_process_table.rpc_ip, pod_namespace=next_process_table.namespace)
        next_process_client = ProcessClient(host=next_process_table.rpc_ip, port=next_process_table.rpc_port)
        response = next_process_client.NotifyCDIsAccess(cdi_controller_table_list)
        if response.err != "":
            logging.error(f"TransferCDIs: Exception while notifying next process {next_process_table.process_id} about CDI access: {response.err}")
            # CDIs access transfer was not notified to the next process
            err = f"Controller-TransferCDIs: exception from next process {next_process_table.process_id} while notifying access: {response.err}"
            return pb2.TransferCDIsResponse(err=err)
        for cdi_controller_table in cdi_controller_table_list:
            logging.info(f"TransferCDIs: Updating CDI record for the key {cdi_controller_table.cdi_key}")
            # update record in DB
            cdi_controller_table.update_by_cdi_id()
        return pb2.TransferCDIsResponse(err="")

    def DeleteCDIs(self, request, context):
        logging.info(f"DeleteCDIs: Processing request from process {request.id}")
        logging.info(f"DeleteCDIs: Fetching current process {request.id} record")
        # Fetch the process details to identify the node
        process_table = Registered_Process_Table(process_id=request.id)
        result = process_table.get_by_process_id()
        if result is None:
            logging.error(f"DeleteCDIs: Exception while fetching current process {request.id}")
            # if process is not present, then return an error
            err = f"Controller-DeleteCDIs: Couldn't find process with id: {process_table.process_id}"
            return pb2.DeleteCDIsResponse(err=err)

        logging.info(f"DeleteCDIs: Fetching current minion on node {process_table.node_ip} record")
        # Fetch the minion details deployed on the selected process the node
        minion_table = Registered_Minion_Table(node_ip=process_table.node_ip)
        result = minion_table.get_by_node_ip()
        if result is None:
            logging.info(f"DeleteCDIs: Couldn't find minion on node {process_table.node_ip} record")
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
        minion_host = get_kube_dns_url(node_ip=minion_table.node_ip, pod_ip=minion_table.rpc_ip,
                                       pod_namespace=minion_table.namespace, deploy_platform=deploy_platform)
        minion_client = MinionClient(host=minion_host, port=minion_table.rpc_port)
        response = minion_client.DeleteCDIs(cdi_controller_table_list)
        if response.err != "":
            logging.info(f"DeleteCDIs: Exception from minion on node {process_table.node_ip} deleting CDIs: {response.err}")
            # CDIs were not deleted by the minion
            err = f"Controller-DeleteCDIs: exception from minion while deleting CDIs: {response.err}"
            return pb2.DeleteCDIsResponse(err=err)
        for cdi_controller_table in cdi_controller_table_list:
            logging.info(f"DeleteCDIs: Updating CDI record for the key {cdi_controller_table.cdi_key}")
            # delete record in DB
            cdi_controller_table.delete_by_cdi_id()
        return pb2.DeleteCDIsResponse(err="")


def _configure_maintenance_server(server: grpc.Server) -> None:
    # Create a health check servicer. We use the non-blocking implementation
    # to avoid thread starvation.
    health_servicer = health.HealthServicer(
        experimental_non_blocking=True,
        experimental_thread_pool=futures.ThreadPoolExecutor(
            max_workers=_THREAD_POOL_SIZE
        ),
    )

    # Create a tuple of all of the services we want to export via reflection.
    services = tuple(
        service.full_name
        for service in pb2.DESCRIPTOR.services_by_name.values()
    ) + (reflection.SERVICE_NAME, health.SERVICE_NAME)

    # Mark all services as healthy.
    _health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    for service in services:
        health_servicer.set(service, _health_pb2.HealthCheckResponse.SERVING)
    reflection.enable_server_reflection(services, server)


def serve_rpc(rpc_host, rpc_port):
    logging.info(f"Starting RPC server on : {rpc_host}:{rpc_port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_THREAD_POOL_SIZE))

    pb2_grpc.add_ControllerServiceServicer_to_server(ControllerService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    _configure_maintenance_server(server=server)

    server.start()
    server.wait_for_termination()
