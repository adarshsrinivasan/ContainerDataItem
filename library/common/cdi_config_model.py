import logging
import os

import yaml

from library.shm.shm_lib import SharedMemory
from library.common import controller_api_pb2 as pb2

parent_config = None


def parse_config(config_path, process_id):
    global parent_config
    parent_config = Config()
    parent_config.process_id = process_id
    parent_config.from_yaml(config_path)


def get_parent_config():
    global parent_config
    if parent_config is None:
        raise Exception("get_parent_config: parent_config not populated")
    return parent_config


def populate_config_from_parent_config(config, all_fields=False):
    global parent_config
    if parent_config is None:
        raise Exception("populate_config_from_parent_config: parent_config not populated")

    if all_fields:
        config.process_id = parent_config.process_id
        config.process_name = parent_config.process_name
        config.app_id = parent_config.app_id
        config.app_name = parent_config.app_name
        config.cdis = parent_config.cdis

    config.destroy_if_no_new_data = parent_config.destroy_if_no_new_data
    config.create = parent_config.create
    config.transfer_id = parent_config.transfer_id
    config.transfer_mode = parent_config.transfer_mode


def print_cdi_infos():
    global parent_config
    if parent_config is None:
        raise Exception("populate_config_from_parent_config: parent_config not populated")
    for _, cdi in parent_config.cdis.items():
        cdi.print_stat()


class PrettyPrinter(object):
    def __str__(self):
        lines = [self.__class__.__name__ + ':']
        for key, val in vars(self).items():
            lines += '{}: {}'.format(key, val).split('\n')
        return '\n    '.join(lines)


class CDI(PrettyPrinter):
    def __init__(self, cdi_id="", cdi_key=0, cdi_size_bytes=0, cdi_access_mode=0, uid=0, gid=0):
        self.cdi_id = cdi_id
        self.cdi_key = cdi_key
        self.cdi_size_bytes = cdi_size_bytes
        self.cdi_access_mode = cdi_access_mode
        self.uid = uid
        self.gid = gid
        self.__myshm = SharedMemory(size=self.cdi_size_bytes, key=self.cdi_key, shm_mode=self.cdi_access_mode,
                                    uid=self.uid, gid=self.gid)
        self.__ptr = None

    def __is_access_allowed(self):
        try:
            shmid_ds = self.__myshm.stat()
        except Exception as err:
            logging.error(f"CDI_Config - Stat Error: {err}")
            return
        return shmid_ds.contents.shm_perm.uid == self.uid and shmid_ds.contents.shm_perm.gid == self.gid

    def __attach(self):
        if not self.__is_access_allowed():
            raise Exception(
                f"CDI_Config - Attach Error: User {self.uid}:{self.gid} does not have access on cdi: {self.cdi_id}")
        self.__ptr = self.__myshm.attach()

    def __detach(self):
        if not self.__is_access_allowed():
            raise Exception(
                f"CDI_Config - Detach Error: User {self.uid}:{self.gid} does not have access on cdi: {self.cdi_id}")
        self.__myshm.detach()
        self.__ptr = None

    def write_data(self, data: str):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        self.__myshm.write_data(data)
        self.__detach()
        logging.info(f"CDI_Config - Write: wrote '{data}' to shared memory with shm_id: {self.__myshm.shm_id}")

    def read_data(self, length=0) -> str:
        if length == 0:
            length = self.cdi_size_bytes
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        data = self.__myshm.read_data(length)
        self.__detach()
        if len(data) > 0:
            logging.info(f"CDI_Config - Read: read '{data}' from shared memory with shm_id: {self.__myshm.shm_id}")
        return data

    def clear_data(self):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__attach()
        self.__myshm.clear_data()
        self.__detach()
        logging.info(f"CDI_Config - Clear: cleared data of shared memory with shm_id: {self.__myshm.shm_id}")

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

    def print_stat(self):
        if self.__myshm.shm_id == -1:
            self.__myshm.create()
        self.__myshm.print_stat()


class Config(PrettyPrinter):
    def __init__(self, process_id="", process_name="", app_id="", app_name="", cdis=None, destroy_if_no_new_data=False,
                 create=False, transfer_id="", transfer_mode=666):
        if cdis is None:
            cdis = {}

        self.process_id = process_id
        self.process_name = process_name
        self.app_id = app_id
        self.app_name = app_name
        self.cdis = cdis
        self.destroy_if_no_new_data = destroy_if_no_new_data
        self.create = create
        self.transfer_id = transfer_id
        self.transfer_mode = transfer_mode

    def to_proto_controller_cdi_configs(self):
        proto_controller_cdi_configs = []
        for _, cdi_config in self.cdis.items():
            proto_controller_cdi_config = pb2.CdiConfig()

            proto_controller_cdi_config.process_id = self.process_id
            proto_controller_cdi_config.process_name = self.process_name
            proto_controller_cdi_config.app_id = self.app_id
            proto_controller_cdi_config.app_name = self.app_name
            proto_controller_cdi_config.cdi_id = cdi_config.cdi_id
            proto_controller_cdi_config.cdi_key = cdi_config.cdi_key
            proto_controller_cdi_config.cdi_size_bytes = cdi_config.cdi_size_bytes
            proto_controller_cdi_config.cdi_access_mode = cdi_config.cdi_access_mode
            proto_controller_cdi_config.uid = cdi_config.uid
            proto_controller_cdi_config.gid = cdi_config.gid

            proto_controller_cdi_configs.append(proto_controller_cdi_config)
        return proto_controller_cdi_configs

    def from_proto_controller_cdi_configs(self, proto_controller_cdi_configs):
        cdis = {}
        for proto_controller_cdi_config in proto_controller_cdi_configs:
            cdi = CDI()

            cdi.cdi_id = proto_controller_cdi_config.cdi_id
            cdi.cdi_key = proto_controller_cdi_config.cdi_key
            cdi.cdi_size_bytes = proto_controller_cdi_config.cdi_size_bytes
            cdi.cdi_access_mode = proto_controller_cdi_config.cdi_access_mode
            cdi.uid = proto_controller_cdi_config.uid
            cdi.gid = proto_controller_cdi_config.gid

            cdis[cdi.cdi_id] = cdi
        self.process_id = proto_controller_cdi_configs[0].process_id
        self.process_name = proto_controller_cdi_configs[0].process_name
        self.app_id = proto_controller_cdi_configs[0].app_id
        self.app_name = proto_controller_cdi_configs[0].app_name
        self.cdis = cdis

    def from_yaml(self, path_to_yaml_config):
        # Read config YAML file
        with open(path_to_yaml_config, 'r') as stream:
            process_config_dict = yaml.safe_load(stream)
        logging.debug(f"Parsed config: {process_config_dict}")

        process_configs_dict = process_config_dict.get('process_configs', [])
        my_config_dict = None

        for process_config_dict in process_configs_dict:
            if self.process_id == process_config_dict["id"]:
                my_config_dict = process_config_dict
                break
        if my_config_dict is None:
            raise Exception(f"Exception while parsing config. No config found for : {self.process_id}")

        cdis_dict = my_config_dict["cdis"]

        cdis = {}

        for cdi_dict in cdis_dict:
            cdis[cdi_dict["cdi_id"]] = CDI(
                cdi_id=cdi_dict["cdi_id"],
                cdi_key=cdi_dict["cdi_key"],
                cdi_size_bytes=int(cdi_dict["cdi_size_bytes"]),
                cdi_access_mode=int(cdi_dict["cdi_access_mode"]),
                uid=os.getuid(),
                gid=os.getgid()
            )

        self.process_name = my_config_dict["name"]
        self.app_id = process_config_dict.get('app_id', "")
        self.app_name = process_config_dict.get('app_name', "")
        self.cdis = cdis
        self.destroy_if_no_new_data = my_config_dict["destroy_if_no_new_data"]
        self.create = my_config_dict["create"]
        self.transfer_id = my_config_dict["transfer_id"]
        self.transfer_mode = int(my_config_dict["transfer_mode"])
        return


if __name__ == "__main__":
    dummy_config = Config(process_id="b8269e66-e6d4-4b66-8f1e-acd9715372c9")
    dummy_config.from_yaml("../../deployment/kube/common_config/process_config.yaml")
    logging.info(dummy_config)
