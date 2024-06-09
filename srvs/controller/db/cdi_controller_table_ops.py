import logging
from typing import List
from library.db.sql_db import execute_sql_command

import srvs.common.rpc_api.controller_api_pb2 as pb2

TABLE_NAME = "cdi_controller_data"

def init_cdi_controller_table():
    logging.info(f"Creating {TABLE_NAME} Table...")
    execute_sql_command(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                id SERIAL PRIMARY KEY,
                cdi_id VARCHAR (50) UNIQUE NOT NULL,
                process_id VARCHAR (50) NOT NULL,
                process_name VARCHAR (50) NOT NULL,
                app_id VARCHAR (50) NOT NULL,
                app_name VARCHAR (50) NOT NULL,
                cdi_key BIGINT NOT NULL,
                cdi_size_bytes BIGINT NOT NULL,
                cdi_access_mode BIGINT NOT NULL,
                uid BIGINT NOT NULL,
                gid BIGINT NOT NULL);""")
    logging.info(f"Created {TABLE_NAME} Table!")


class CDI_Controller_Table:
    def __init__(self, cdi_id="", process_id="", process_name="", app_id="", app_name="", cdi_key=0, cdi_size_bytes=0,
                 cdi_access_mode=0, uid=0, gid=0):
        self.id = -1
        self.cdi_id = cdi_id
        self.process_id = process_id
        self.process_name = process_name
        self.app_id = app_id
        self.app_name = app_name
        self.cdi_key = cdi_key
        self.cdi_size_bytes = cdi_size_bytes
        self.cdi_access_mode = cdi_access_mode
        self.uid = uid
        self.gid = gid

    def insert(self):
        execute_sql_command(
            f"""INSERT INTO {TABLE_NAME}(cdi_id, process_id, process_name, app_id, app_name, cdi_key, cdi_size_bytes, cdi_access_mode, uid, gid) VALUES('{self.cdi_id}', '{self.process_id}', '{self.process_name}', '{self.app_id}', '{self.app_name}', {self.cdi_key}, {self.cdi_size_bytes}, {self.cdi_access_mode}, {self.uid}, {self.gid});""")

    def list_by_app_id(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE app_id='{self.app_id}';""", True)
        if len(result_rows) > 0:
            result: List[CDI_Controller_Table] = []
            for row in result_rows:
                cdi_controller_data = CDI_Controller_Table()
                cdi_controller_data.load_tuple(row)
                result.append(cdi_controller_data)
        return result

    def list_by_process_id(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE process_id='{self.process_id}';""", True)
        if len(result_rows) > 0:
            result: List[CDI_Controller_Table] = []
            for row in result_rows:
                cdi_controller_data = CDI_Controller_Table()
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

    def update_by_cdi_id(self):
        execute_sql_command(
            f"""UPDATE {TABLE_NAME} SET process_id = '{self.process_id}', process_name = '{self.process_name}', uid = {self.uid}, gid = {self.gid}, cdi_access_mode = {self.cdi_access_mode} WHERE cdi_id = '{self.cdi_id}';""")

    def delete_by_cdi_id(self):
        execute_sql_command(f"""DELETE FROM {TABLE_NAME} WHERE cdi_id='{self.cdi_id}';""")

    def load_tuple(self, tuple_data):
        self.id = tuple_data[0]
        self.cdi_id = tuple_data[1]
        self.process_id = tuple_data[2]
        self.process_name = tuple_data[3]
        self.app_id = tuple_data[4]
        self.app_name = tuple_data[5]
        self.cdi_key = tuple_data[6]
        self.cdi_size_bytes = tuple_data[7]
        self.cdi_access_mode = tuple_data[8]
        self.uid = tuple_data[9]
        self.gid = tuple_data[10]

    def load_proto_cdi_config(self, proto_cdi_config):
        self.cdi_id = proto_cdi_config.cdi_id
        self.process_id = proto_cdi_config.process_id
        self.process_name = proto_cdi_config.process_name
        self.app_id = proto_cdi_config.app_id
        self.app_name = proto_cdi_config.app_name
        self.cdi_key = proto_cdi_config.cdi_key
        self.cdi_size_bytes = proto_cdi_config.cdi_size_bytes
        self.cdi_access_mode = proto_cdi_config.cdi_access_mode
        self.uid = proto_cdi_config.uid
        self.gid = proto_cdi_config.gid

    def as_proto_cdi_config(self):
        proto_cdi_config = pb2.CdiConfig()

        proto_cdi_config.cdi_id = self.cdi_id
        proto_cdi_config.process_id = self.process_id
        proto_cdi_config.process_name = self.process_name
        proto_cdi_config.app_id = self.app_id
        proto_cdi_config.app_name = self.app_name
        proto_cdi_config.cdi_key = int(self.cdi_key)
        proto_cdi_config.cdi_size_bytes = int(self.cdi_size_bytes)
        proto_cdi_config.cdi_access_mode = int(self.cdi_access_mode)
        proto_cdi_config.uid = int(self.uid)
        proto_cdi_config.gid = int(self.gid)

        return proto_cdi_config
