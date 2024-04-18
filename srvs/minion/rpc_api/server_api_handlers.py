import logging
from concurrent import futures

import grpc

from srvs.minion.rpc_api import minion_api_pb2_grpc as pb2_grpc, minion_api_pb2 as pb2
from library.common.utils import decode_payload, encode_payload
from library.shm.shm_lib import SharedMemory
from library.shm.shm_ops import SHM_access
from srvs.minion.db.cdi_minion_table_ops import CDI_Minion_Table
from srvs.minion.rpc_api.minion_client_api_handlers import MinionClient


class MinionControllerService(pb2_grpc.MinionControllerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    # CreateCDIs handles the RPC request from Controller and other Minions to create a CDI locally on the node
    def CreateCDIs(self, request, context):
        result_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table = CDI_Minion_Table()
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # check if we already created the CDI
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                # if not created, the creat it and set right permissions
                shm = SharedMemory(size=cdi_minion_table.shm_segsz, key=cdi_minion_table.shm_key,
                                   shm_mode=cdi_minion_table.shm_mode, uid=cdi_minion_table.shm_uid,
                                   gid=cdi_minion_table.shm_gid)
                shm_access = SHM_access()
                logging.info(
                    f"Minion-CreateCDIs: Creating new SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}")
                try:
                    cdi_minion_table.shm_shmid = shm.create()
                    cdi_minion_table.shm_shmid = shm.shm_id
                    shm_access.shm_id = cdi_minion_table.shm_shmid
                    shm_access.size = cdi_minion_table.shm_segsz
                except Exception as err:
                    err = f"Exception while creating SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                    return pb2.CreateCDIsResponse(cdi_configs=None, err=err)

                # Set the right permissions
                try:
                    shm.set(uid=cdi_minion_table.shm_uid, gid=cdi_minion_table.shm_gid,
                            shm_mode=cdi_minion_table.shm_mode)
                except Exception as err:
                    err = f"Exception while changing permission of SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                    return pb2.CreateCDIsResponse(cdi_configs=None, err=err)

                # If the request contains a payload, then populate the CDI with the payload.
                if cdi_minion_table.payload != "":
                    shm_access.clear_data()
                    shm_access.write_data(decode_payload(cdi_minion_table.payload))
                # insert the new record into the DB
                cdi_minion_table.insert()
            # collect the CDI_Minion_Table in a list by converting it back to proto cdi config
            result_list.append(cdi_minion_table.as_proto_cdi_config())
        return pb2.CreateCDIsResponse(cdi_configs=result_list, err="")

    # UpdateCDIs handles the RPC request from Controller to update the permissions of a CDI
    def UpdateCDIs(self, request, context):
        result_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table = CDI_Minion_Table()
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                # if not, then return an error
                err = f"Minion-Update: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.UpdateCDIsResponse(cdi_configs=None, err=err)
            # if present, then update the permissions of the CDI
            shm = SharedMemory(size=cdi_minion_table.shm_segsz, key=cdi_minion_table.shm_key,
                               shm_mode=cdi_minion_table.shm_mode, uid=cdi_minion_table.shm_uid,
                               gid=cdi_minion_table.shm_gid)
            try:
                shm.set(uid=cdi_minion_table.shm_uid, gid=cdi_minion_table.shm_gid, shm_mode=cdi_minion_table.shm_mode)
            except Exception as err:
                err = f"Exception while changing permission of SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                return pb2.CreateCDIsResponse(cdi_configs=None, err=err)

            # update the recode in the DB
            cdi_minion_table.update_shm_uid_and_shm_gid_by_cdi_id()
            # collect the CDI_Minion_Table in a list by converting it back to proto cdi config
            result_list.append(cdi_minion_table.as_proto_cdi_config())
        return pb2.UpdateCDIsResponse(cdi_configs=result_list, err="")

    # TransferAndDeleteCDIs handles the RPC request from Controller to transfer a set of CDIs from local to a
    # different node.
    def TransferAndDeleteCDIs(self, request, context):
        cdi_minion_table_list = []
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table = CDI_Minion_Table()
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                # if not, then return an error
                err = f"Minion-TransferAndDelete: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.TransferAndDeleteCDIsResponse(err=err)
            # if present, fetch the CID's payload
            shm_access = SHM_access(shm_id=cdi_minion_table.shm_shmid, size=cdi_minion_table.shm_segsz)
            cdi_minion_table.payload = encode_payload(shm_access.read_data())

            # collect the CDI_Minion_Table in a list by converting it back to proto cdi config
            cdi_minion_table_list.append(cdi_minion_table.as_proto_cdi_config())

        # Transfer the data to the requested host
        client = MinionClient(host=request.transfer_host, port=request.transfer_port)
        response = client.CreateCDIs(cdi_minion_table_list)  # make sure cdi_minion_table.payload is populated
        if response.err != "":
            err = f"Minion-TransferAndDelete: exception while transferring CDI to {request.transfer_host}:{request.transfer_port}: {response.err}"
            return pb2.TransferAndDeleteCDIsResponse(err=err)

        # Clean up the transferred CDIs from local
        for cdi_minion_table in cdi_minion_table_list:
            shm = SharedMemory(size=cdi_minion_table.shm_segsz,  shm_id=cdi_minion_table.shm_shmid,
                               key=cdi_minion_table.shm_key, shm_mode=cdi_minion_table.shm_mode,
                               uid=cdi_minion_table.shm_uid, gid=cdi_minion_table.shm_gid)
            try:
                shm.remove()
            except Exception as err:
                err = f"Exception while deleting SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                # Just log if we couldn't delete the shared memory - TODO: Should we handle this differently?
                logging.error(err)
            cdi_minion_table.delete_by_cdi_id()
        return pb2.TransferAndDeleteCDIsResponse(err="")

    # DeleteCDIs handles the RPC request from Controller to delete a set of CDIs from local.
    def DeleteCDIs(self, request, context):
        for cdi_config in request.cdi_configs:
            # convert the proto cdi config to CDI_Minion_Table model
            cdi_minion_table = CDI_Minion_Table()
            cdi_minion_table.load_proto_cdi_config(cdi_config)

            # check if the CDI is managed by the Minion
            result = cdi_minion_table.get_by_cdi_id()
            if result is None:
                # if not, then return an error
                err = f"Minion-Delete: Couldn't find CDI with id: {cdi_minion_table.cdi_id}"
                return pb2.DeleteCDIsResponse(err=err)
            # if present, delete the CID
            shm = SharedMemory(size=cdi_minion_table.shm_segsz,  shm_id=cdi_minion_table.shm_shmid,
                               key=cdi_minion_table.shm_key, shm_mode=cdi_minion_table.shm_mode,
                               uid=cdi_minion_table.shm_uid, gid=cdi_minion_table.shm_gid)
            try:
                shm.remove()
            except Exception as err:
                err = f"Exception while deleting SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
                # Just log if we couldn't delete the shared memory - TODO: Should we handle this differently?
                logging.error(err)
            # delete the CID record from DB
            cdi_minion_table.delete_by_cdi_id()
        return pb2.DeleteCDIsResponse(err="")


def serve(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_MinionControllerServiceServicer_to_server(MinionControllerService(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    server.wait_for_termination()
