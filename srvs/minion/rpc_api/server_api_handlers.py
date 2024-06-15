import logging
from concurrent import futures

import grpc
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2 as _health_pb2
from grpc_health.v1 import health_pb2_grpc as _health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from srvs.common.rpc_api import minion_api_pb2_grpc as pb2_grpc, minion_api_pb2 as pb2
from library.common.utils import encode_payload
from library.shm.shm_lib import SharedMemory
from library.shm.shm_ops import SHM_access
from srvs.minion.common.cdi_ops_handlers import create_cdis
from srvs.minion.db.cdi_minion_table_ops import CDI_Minion_Table
from srvs.minion.rdma.minion_rdma_ops import MinionRDMAClient

_THREAD_POOL_SIZE = 256
_GRPC_MSG_SIZE = 20 * 1024 * 1024


class MinionControllerService(pb2_grpc.MinionControllerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    # CreateCDIs handles the RPC request from Controller and other Minions to create a CDI locally on the node
    def CreateCDIs(self, request, context):
        logging.info(f"CreateCDIs: Processing request")
        err = create_cdis(request=request)
        return pb2.MinionCreateCDIsResponse(err=err)

    # UpdateCDIs handles the RPC request from Controller to update the permissions of a CDI
    def UpdateCDIs(self, request, context):
        logging.info(f"UpdateCDIs: Processing request")
        for cdi_config in request.cdi_configs:
            cdi_minion_table = CDI_Minion_Table(cdi_id=cdi_config.cdi_id)
            logging.info(f"UpdateCDIs: Fetching cdi record with key: {cdi_minion_table.cdi_id}")
            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                logging.error(f"UpdateCDIs: Couldn't find cdi record with key: {cdi_minion_table.cdi_id}")
                # if not, then return an error
                err = f"Minion-UpdateCDIs: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.MinionUpdateCDIsResponse(err=err)
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # if present, then update the permissions of the CDI
            shm = SharedMemory(size=cdi_minion_table.cdi_size_bytes, key=cdi_minion_table.cdi_key,
                               shm_mode=cdi_minion_table.cdi_access_mode, uid=cdi_minion_table.uid,
                               gid=cdi_minion_table.gid)
            logging.info(f"UpdateCDIs: Before update")
            shm.print_stat()

            try:
                shm.set(uid=cdi_minion_table.uid, gid=cdi_minion_table.gid, shm_mode=cdi_minion_table.cdi_access_mode)
            except Exception as err:
                err = f"Exception while changing permission of SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                logging.error(f"UpdateCDIs: {err}")
                return pb2.MinionUpdateCDIsResponse(err=err)

            logging.info(f"UpdateCDIs: After update")
            shm.print_stat()

            # update the recode in the DB
            cdi_minion_table.update_by_cdi_id()
            logging.info(f"UpdateCDIs: Successfully updated cdi record with key: {cdi_minion_table.cdi_id}")
        return pb2.MinionUpdateCDIsResponse(err="")

    # TransferAndDeleteCDIs handles the RPC request from Controller to transfer a set of CDIs from local to a
    # different node.
    def TransferAndDeleteCDIs(self, request, context):
        logging.info(f"TransferAndDeleteCDIs: Processing request")
        cdi_minion_table_list = []
        for cdi_config in request.cdi_configs:
            cdi_minion_table = CDI_Minion_Table(cdi_id=cdi_config.cdi_id)
            logging.info(f"TransferAndDeleteCDIs: Fetching cdi record with key: {cdi_minion_table.cdi_id}")
            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                logging.error(f"TransferAndDeleteCDIs: Couldn't find cdi record with key: {cdi_minion_table.cdi_id}")
                # if not, then return an error
                err = f"Minion-TransferAndDeleteCDIs: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.MinionTransferAndDeleteCDIsResponse(err=err)
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # if present, fetch the CID's payload
            shm = SharedMemory(size=cdi_minion_table.cdi_size_bytes, key=cdi_minion_table.cdi_key,
                               shm_mode=cdi_minion_table.cdi_access_mode, uid=cdi_minion_table.uid,
                               gid=cdi_minion_table.gid)
            shm_access = SHM_access()
            try:
                shm_access.shm_id = shm.create()
                shm_access.size = cdi_minion_table.cdi_size_bytes
            except Exception as err:
                err = f"Minion-TransferAndDeleteCDIs: Exception while fetching SharedMemory details for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                logging.error(f"TransferAndDeleteCDIs: {err}")
                return pb2.MinionTransferAndDeleteCDIsResponse(err=err)

            logging.info(
                f"Minion-TransferAndDeleteCDIs: Fetching payload of SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}")
            cdi_minion_table.payload = encode_payload(shm_access.read_data())

            # collect the CDI_Minion_Table in a list
            cdi_minion_table_list.append(cdi_minion_table)

        # Transfer the data to the requested host
        client = MinionRDMAClient(host=request.transfer_host, port=request.transfer_port)
        error = client.CreateCDIs(cdi_minion_table_list)  # make sure cdi_minion_table.payload is populated
        if error != "":
            err = f"Minion-TransferAndDeleteCDIs: exception while transferring CDI to {request.transfer_host}:{request.transfer_port}: {error}"
            logging.error(f"TransferAndDeleteCDIs: {err}")
            return pb2.MinionTransferAndDeleteCDIsResponse(err=err)

        # Clean up the transferred CDIs from local
        for cdi_minion_table in cdi_minion_table_list:
            shm = SharedMemory(size=cdi_minion_table.cdi_size_bytes, key=cdi_minion_table.cdi_key,
                               shm_mode=cdi_minion_table.cdi_access_mode, uid=cdi_minion_table.uid,
                               gid=cdi_minion_table.gid)
            try:
                shm.remove()
            except Exception as err:
                err = f"Exception while deleting SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                # Just log if we couldn't delete the shared memory - TODO: Should we handle this differently?
                logging.error(f"TransferAndDeleteCDIs: {err}")
            logging.info(f"TransferAndDeleteCDIs: Successfully deleted cdi record with key: {cdi_minion_table.cdi_id}")
        return pb2.MinionTransferAndDeleteCDIsResponse(err="")

    # DeleteCDIs handles the RPC request from Controller to delete a set of CDIs from local.
    def DeleteCDIs(self, request, context):
        logging.info(f"DeleteCDIs: Processing request")
        for cdi_config in request.cdi_configs:
            cdi_minion_table = CDI_Minion_Table(cdi_id=cdi_config.cdi_id)
            logging.info(f"DeleteCDIs: Fetching cdi record with key: {cdi_minion_table.cdi_id}")
            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                logging.error(f"DeleteCDIs: Couldn't find cdi record with key: {cdi_minion_table.cdi_id}")
                # if not, then return an error
                err = f"Minion-DeleteCDIs: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.MinionDeleteCDIsResponse(err=err)
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table.load_proto_cdi_config(cdi_config)
            # if present, delete the CID
            shm = SharedMemory(size=cdi_minion_table.cdi_size_bytes, key=cdi_minion_table.cdi_key,
                               shm_mode=cdi_minion_table.cdi_access_mode, uid=cdi_minion_table.uid,
                               gid=cdi_minion_table.gid)
            try:
                shm.remove()
            except Exception as err:
                err = f"Exception while deleting SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                # Just log if we couldn't delete the shared memory - TODO: Should we handle this differently?
                logging.error(f"DeleteCDIs: {err}")
            # delete the CID record from DB
            cdi_minion_table.delete_by_cdi_id()
        return pb2.MinionDeleteCDIsResponse(err="")


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
    channel_opt = [('grpc.max_send_message_length', _GRPC_MSG_SIZE),
                   ('grpc.max_receive_message_length', _GRPC_MSG_SIZE)]
    server = grpc.server(thread_pool=futures.ThreadPoolExecutor(max_workers=_THREAD_POOL_SIZE), options=channel_opt)

    pb2_grpc.add_MinionControllerServiceServicer_to_server(MinionControllerService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    _configure_maintenance_server(server=server)

    server.start()
    server.wait_for_termination()

