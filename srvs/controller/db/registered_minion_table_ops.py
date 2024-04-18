from library.db.sql_db import execute_sql_command

TABLE_NAME = "registered_minion_data"


def init_registered_minion_table():
    execute_sql_command(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                id SERIAL PRIMARY KEY,
                node_ip VARCHAR (50) UNIQUE NOT NULL,
                rpc_ip VARCHAR (50) UNIQUE NOT NULL,
                rpc_port VARCHAR (50) NOT NULL);""")


class Registered_Minion_Table:
    def __init__(self, node_ip="", rpc_ip="", rpc_port=""):
        self.id = -1
        self.node_ip = node_ip
        self.rpc_ip = rpc_ip
        self.rpc_port = rpc_port

    def insert(self):
        execute_sql_command(
            f"""INSERT INTO {TABLE_NAME}(node_ip, rpc_ip, rpc_port) VALUES('{self.node_ip}', '{self.rpc_ip}', '{self.rpc_port}');""")

    def get_by_node_ip(self):
        result = None
        result_rows = execute_sql_command(f"""SELECT * FROM {TABLE_NAME} WHERE node_ip='{self.node_ip}';""", True)
        if len(result_rows) > 0:
            self.load_tuple(result_rows[0])
            result = self
        return result

    def update_by_node_ip(self):
        execute_sql_command(
            f"""UPDATE {TABLE_NAME} SET rpc_ip = '{self.rpc_ip}', rpc_port = '{self.rpc_port}' WHERE node_ip = '{self.node_ip}';""")

    def delete_by_node_ip(self):
        execute_sql_command(f"""DELETE FROM {TABLE_NAME} WHERE node_ip='{self.node_ip}';""")

    def load_tuple(self, tuple_data):
        self.id = tuple_data[0]
        self.node_ip = tuple_data[1]
        self.rpc_ip = tuple_data[2]
        self.rpc_port = tuple_data[3]
