import logging

from library.shm.shm_lib import SharedMemory
from srvs.app_process.rpc_api import controller_api_pb2 as pb2


class CDI:
    def __init__(self, cdi_id="", shm_key=0, shm_segsz=0, shm_mode=0, shm_uid=0, shm_gid=0):
        self.cdi_id = cdi_id
        self.shm_key = shm_key
        self.shm_segsz = shm_segsz
        self.shm_mode = shm_mode
        self.shm_uid = shm_uid
        self.shm_gid = shm_gid
        self.__myshm = SharedMemory(size=self.shm_segsz, key=self.shm_key, shm_mode=self.shm_mode, uid=self.shm_uid,
                                    gid=self.shm_gid)
        self.__ptr = None

    def __is_access_allowed(self):
        try:
            shmid_ds = self.__myshm.stat()
        except Exception as err:
            logging.error(f"CDI_Config - Stat Error: {err}")
            return
        return shmid_ds.contents.shm_perm.uid == self.shm_uid and shmid_ds.contents.shm_perm.gid == self.shm_gid

    def __attach(self):
        if not self.__is_access_allowed():
            raise Exception(
                f"CDI_Config - Attach Error: User {self.shm_uid}:{self.shm_gid} does not have access on cdi: {self.cdi_id}")
        self.__ptr = self.__myshm.attach()

    def __detach(self):
        if not self.__is_access_allowed():
            raise Exception(
                f"CDI_Config - Detach Error: User {self.shm_uid}:{self.shm_gid} does not have access on cdi: {self.cdi_id}")
        self.__myshm.detach()
        self.__ptr = None

    def write_data(self, data: str):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        self.__myshm.write_data(data)
        self.__detach()
        logging.info(f"CDI_Config - Write: wrote '{data}' to shared memory with shm_id: {self.shm_id}")

    def read_data(self, length=0) -> str:
        if length == 0:
            length = self.size
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        data = self.__myshm.read_data(length)
        self.__detach()
        if len(data) > 0:
            logging.info(f"CDI_Config - Read: read '{data}' from shared memory with shm_id: {self.shm_id}")
        return data

    def clear_data(self):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        self.__myshm.clear_data()
        self.__detach()
        logging.info(f"CDI_Config - Clear: cleared data of shared memory with shm_id: {self.shm_id}")

    def change_owner(self, uid, gid):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        try:
            shmid_ds = self.__myshm.stat()
        except Exception as err:
            logging.error(f"CDI_Config - Change Owner - Stat Error: {err}")
            return

        try:
            self.__myshm.set(uid=uid, gid=gid, shm_mode=shmid_ds.contents.shm_perm.mode)
        except Exception as err:
            logging.error(f"CDI_Config - Change Owner - Set Error: {err}")
            return

    def print_cdi_info(self):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        try:
            shmid_ds = self.stat()
        except Exception as err:
            logging.error(f"CDI_Config - Print CDI Info - Stat Error: {err}")
            return

        logging.info("\nCDI Info:")
        logging.info(f"CDI ID: {self.cdi_id}")
        logging.info(f"Key: {self.shm_key}")
        logging.info(f"SHMID: {self.__myshm.shm_id}")
        logging.info(f"Access UID: {shmid_ds.contents.shm_perm.uid}")
        logging.info(f"Access GID: {shmid_ds.contents.shm_perm.gid}")
        logging.info(f"Creator UID: {shmid_ds.contents.shm_perm.cuid}")
        logging.info(f"Creator GID: {shmid_ds.contents.shm_perm.cgid}")
        logging.info(f"Access Mode: {shmid_ds.contents.shm_perm.mode}")
        logging.info(f"Size (Bytes): {shmid_ds.contents.shm_segsz}")
        logging.info(f"Last Operator PID: {shmid_ds.contents.lpid}")
        logging.info(f"Creator PID: {shmid_ds.contents.cpid}")
        logging.info(f"Number of attached Processes: {shmid_ds.contents.shm_nattch}\n")
        logging.info(f"Last attach time: {shmid_ds.contents.shm_atime}")
        logging.info(f"Last detach time: {shmid_ds.contents.shm_dtime}")
        logging.info(f"Last change time: {shmid_ds.contents.shm_ctime}")


class Config:
    def __init__(self, process_id="", app_id="", app_name="", cdis=None):
        if cdis is None:
            cdis = []

        self.process_id = process_id
        self.app_id = app_id
        self.app_name = app_name
        self.cdis = cdis

    def to_proto_controller_cdi_configs(self):
        proto_controller_cdi_configs = []
        for cdi_config in self.cdis:
            proto_controller_cdi_config = pb2.ControllerCdiConfig()

            proto_controller_cdi_config.process_id = self.process_id
            proto_controller_cdi_config.app_id = self.app_id
            proto_controller_cdi_config.app_name = self.app_name
            proto_controller_cdi_config.cdi_id = cdi_config.cdi_id
            proto_controller_cdi_config.shm_key = cdi_config.shm_key
            proto_controller_cdi_config.shm_segsz = cdi_config.shm_segsz
            proto_controller_cdi_config.shm_mode = cdi_config.shm_mode
            proto_controller_cdi_config.shm_uid = cdi_config.shm_uid
            proto_controller_cdi_config.shm_gid = cdi_config.shm_gid

            proto_controller_cdi_configs.append(proto_controller_cdi_config)
        return proto_controller_cdi_configs

    @classmethod
    def load_from_proto_controller_cdi_configs(cls, proto_controller_cdi_configs):
        cdis = []
        for proto_controller_cdi_config in proto_controller_cdi_configs:
            cdi = CDI()

            cdi.cdi_id = proto_controller_cdi_config.cdi_id
            cdi.shm_key = proto_controller_cdi_config.shm_key
            cdi.shm_segsz = proto_controller_cdi_config.shm_segsz
            cdi.shm_mode = proto_controller_cdi_config.shm_mode
            cdi.shm_uid = proto_controller_cdi_config.shm_uid
            cdi.shm_gid = proto_controller_cdi_config.shm_gid

            cdis.append(cdi)
        return cls(process_id=proto_controller_cdi_configs[0].process_id, app_id=proto_controller_cdi_configs[0].app_id,
                   app_name=proto_controller_cdi_configs[0].app_name, cdis=cdis)

    @classmethod
    def load_from_json(cls, json_config):
        cdis = []
        for json_cdi_config in json_config["cdi_configs"]:
            cdi = CDI()

            cdi.cdi_id = json_cdi_config["cdi_id"]
            cdi.shm_key = json_cdi_config["shm_key"]
            cdi.shm_segsz = json_cdi_config["shm_segsz"]
            cdi.shm_mode = json_cdi_config["shm_mode"]
            cdi.shm_uid = json_cdi_config["shm_uid"]
            cdi.shm_gid = json_cdi_config["shm_gid"]

            cdis.append(cdi)
        return cls(process_id=json_config["process_id"], app_id=json_config["app_id"], app_name=json_config["app_name"],
                   cdis=cdis)
