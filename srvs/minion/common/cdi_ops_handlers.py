import logging

from library.shm.shm_lib import SharedMemory
from library.shm.shm_ops import SHM_access
from srvs.minion.db.cdi_minion_table_ops import CDI_Minion_Table


def create_cdis(request):
    logging.info(f"create_cdis: Processing request")
    for cdi_config in request.cdi_configs:
        # convert the proto cdi config to CDI_Minion_Table model
        cdi_minion_table = CDI_Minion_Table(cdi_id=cdi_config.cdi_id)
        logging.info(f"create_cdis: Fetching cdi record with key: {cdi_minion_table.cdi_id}")
        # check if we already created the CDI
        result = cdi_minion_table.get_by_cdi_id()
        cdi_minion_table.load_proto_cdi_config(cdi_config)

        logging.error(f"create_cdis: Processing cdi record with key: {cdi_minion_table.cdi_id}")
        # if not created, the creat it and set right permissions
        shm = SharedMemory(size=cdi_minion_table.cdi_size_bytes, key=cdi_minion_table.cdi_key,
                           shm_mode=cdi_minion_table.cdi_access_mode, uid=cdi_minion_table.uid,
                           gid=cdi_minion_table.gid, create_shm=True)
        shm_access = SHM_access()
        logging.info(
            f"Minion-create_cdis: Accessing SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}")
        try:
            shm_access.shm_id = shm.create()
            shm_access.size = cdi_minion_table.cdi_size_bytes
        except Exception as err:
            logging.error(f"create_cdis: {err}")
            err = f"Exception while creating SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
            return err

        # Set the right permissions
        try:
            shm.print_stat()
            shm.set(uid=cdi_minion_table.uid, gid=cdi_minion_table.gid,
                    shm_mode=cdi_minion_table.cdi_access_mode)
        except Exception as err:
            err = f"Exception while changing permission of SharedMemory for App: {cdi_minion_table.app_id}, CDI: {cdi_minion_table.cdi_id}: {err}"
            logging.error(f"create_cdis: {err}")
            return err

        if result is None:
            logging.error(f"create_cdis: Inserting cdi record with key: {cdi_minion_table.cdi_id}")
            # insert the new record into the DB
            cdi_minion_table.insert()
        else:
            logging.error(f"create_cdis: Updating cdi record with key: {cdi_minion_table.cdi_id}")
            # If the request contains a payload, then populate the CDI with the payload.
            if cdi_minion_table.payload != "":
                shm_access.clear_data()
                shm_access.write_data(cdi_minion_table.payload)
            cdi_minion_table.update_by_cdi_id()

        logging.info(f"create_cdis: Successfully created cdi record for key: {cdi_minion_table.cdi_id}")
    return ""
