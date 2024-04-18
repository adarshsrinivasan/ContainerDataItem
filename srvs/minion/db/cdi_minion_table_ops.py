import logging

import srvs.minion.rpc_api.minion_api_pb2 as pb2

from typing import List
from library.db.sql_db import execute_sql_command

TABLE_NAME = "cdi_controller_data"


def init_cdi_minion_data_table():
    logging.info("initializing CDI Minion table...")
    execute_sql_command(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                id SERIAL PRIMARY KEY,
                cdi_id VARCHAR (50) UNIQUE NOT NULL,
                app_id VARCHAR (50) NOT NULL,
                app_name VARCHAR (50) NOT NULL,
                shm_key BIGINT NOT NULL,
                shm_shmid BIGINT NOT NULL,
                shm_segsz BIGINT NOT NULL,
                shm_mode BIGINT NOT NULL,
                shm_uid BIGINT NOT NULL,
                shm_gid BIGINT NOT NULL);""")


class CDI_Minion_Table:
    def __init__(self, cdi_id="", app_id="", app_name="",shm_key="", shm_shmid="", shm_segsz="",
                 shm_mode="", shm_uid="", shm_gid="", payload=""):
        self.id = -1
        self.cdi_id = cdi_id
        self.app_id = app_id
        self.app_name = app_name
        self.shm_key = shm_key
        self.shm_shmid = shm_shmid
        self.shm_segsz = shm_segsz
        self.shm_mode = shm_mode
        self.shm_uid = shm_uid
        self.shm_gid = shm_gid
        self.payload = payload  # will not be persisted in the DB. This is a volatile field use by the process.

    def insert(self):
        execute_sql_command(
            f"""INSERT INTO {TABLE_NAME}(cdi_id, app_id, app_name, shm_key, shm_shmid, shm_segsz, shm_mode, shm_uid, shm_gid) VALUES('{self.cdi_id}', '{self.app_id}', '{self.app_name}', {self.shm_key}, {self.shm_shmid}, {self.shm_segsz}, {self.shm_mode}, {self.shm_uid}, {self.shm_gid});""")

    def list_by_app_id(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE app_id='{self.app_id}';""", True)
        if len(result_rows) > 0:
            result: List[CDI_Minion_Table] = []
            for row in result_rows:
                cdi_controller_data = CDI_Minion_Table()
                cdi_controller_data.load_tuple(row)
                result.append(cdi_controller_data)
        return result

    def get_by_cdi_id(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE cdi_id='{self.cdi_id}';""", True)
        if len(result_rows) > 0:
            self.load_tuple(result_rows[0])
            result = self
        return result

    def update_shm_perm_by_cdi_id(self):
        execute_sql_command(
            f"""UPDATE {TABLE_NAME} SET shm_uid = {self.shm_uid}, shm_gid = {self.shm_gid}, shm_mode = {self.shm_mode} WHERE cdi_id = '{self.cdi_id}';""")

    def delete_by_cdi_id(self):
        execute_sql_command(f"""DELETE FROM {TABLE_NAME} WHERE cdi_id='{self.cdi_id}';""")

    def load_tuple(self, tuple_data):




        self.id = tuple_data[0]
        self.app_id = tuple_data[2]
        self.app_name = tuple_data[2]
        self.cdi_id = tuple_data[3]
        self.shm_key = tuple_data[4]
        self.shm_shmid = tuple_data[5]
        self.shm_segsz = tuple_data[6]
        self.shm_mode = tuple_data[7]
        self.shm_uid = tuple_data[8]
        self.shm_gid = tuple_data[9]

    def load_proto_cdi_config(self, proto_cdi_config):
        self.app_id = proto_cdi_config.app_id
        self.app_name = proto_cdi_config.app_name
        self.cdi_id = proto_cdi_config.cdi_id
        self.shm_key = proto_cdi_config.shm_key
        self.shm_shmid = proto_cdi_config.shm_shmid
        self.shm_segsz = proto_cdi_config.shm_segsz
        self.shm_mode = proto_cdi_config.shm_mode
        self.shm_uid = proto_cdi_config.shm_uid
        self.shm_gid = proto_cdi_config.shm_gid
        self.payload = proto_cdi_config.payload

    def as_proto_cdi_config(self):
        proto_cdi_config = pb2.CdiConfig()
        proto_cdi_config.app_id = self.app_id
        proto_cdi_config.app_name = self.app_name
        proto_cdi_config.cdi_id = self.cdi_id
        proto_cdi_config.shm_key = self.shm_key
        proto_cdi_config.shm_shmid = self.shm_shmid
        proto_cdi_config.shm_segsz = self.shm_segsz
        proto_cdi_config.shm_mode = self.shm_mode
        proto_cdi_config.shm_uid = self.shm_uid
        proto_cdi_config.shm_gid = self.shm_gid
        proto_cdi_config.payload = self.payload
        return proto_cdi_config
