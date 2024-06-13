import logging

from library.db.sql_db import execute_sql_command

TABLE_NAME = "registered_minion_data"


def init_registered_minion_table():
    logging.info(f"Creating {TABLE_NAME} Table...")
    execute_sql_command(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                id SERIAL PRIMARY KEY,
                name VARCHAR (50) UNIQUE NOT NULL,
                namespace VARCHAR (50) NOT NULL,
                node_ip VARCHAR (50) UNIQUE NOT NULL,
                rpc_ip VARCHAR (50) UNIQUE NOT NULL,
                rpc_port BIGINT NOT NULL,
                rdma_ip VARCHAR (50) UNIQUE NOT NULL,
                rdma_port BIGINT NOT NULL);""")
    logging.info(f"Created {TABLE_NAME} Table!")


class Registered_Minion_Table:
    def __init__(self, name="", namespace="", node_ip="", rpc_ip="", rpc_port=0, rdma_ip="", rdma_port=0):
        self.id = -1
        self.name = name
        self.namespace = namespace
        self.node_ip = node_ip
        self.rpc_ip = rpc_ip
        self.rpc_port = rpc_port
        self.rdma_ip = rdma_ip
        self.rdma_port = rdma_port


    def insert(self):
        execute_sql_command(
            f"""INSERT INTO {TABLE_NAME}(name, namespace, node_ip, rpc_ip, rpc_port, rdma_ip, rdma_port) VALUES('{self.name}', '{self.namespace}', '{self.node_ip}', '{self.rpc_ip}', {self.rpc_port}, '{self.rdma_ip}', {self.rdma_port});""")

    def get_by_node_ip(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE node_ip='{self.node_ip}';""", True)
        if len(result_rows) > 0:
            self.load_tuple(result_rows[0])
            result = self
        return result

    def update_by_node_ip(self):
        execute_sql_command(
            f"""UPDATE {TABLE_NAME} SET name = '{self.name}', namespace = '{self.namespace}', rpc_ip = '{self.rpc_ip}', rpc_port = {self.rpc_port}, rdma_ip = '{self.rdma_ip}', rdma_port = {self.rdma_port} WHERE node_ip = '{self.node_ip}';""")

    def delete_by_node_ip(self):
        execute_sql_command(f"""DELETE FROM {TABLE_NAME} WHERE node_ip='{self.node_ip}';""")

    def load_tuple(self, tuple_data):
        self.id = tuple_data[0]
        self.name = tuple_data[1]
        self.namespace = tuple_data[2]
        self.node_ip = tuple_data[3]
        self.rpc_ip = tuple_data[4]
        self.rpc_port = int(tuple_data[5])
        self.rdma_ip = tuple_data[6]
        self.rdma_port = int(tuple_data[7])