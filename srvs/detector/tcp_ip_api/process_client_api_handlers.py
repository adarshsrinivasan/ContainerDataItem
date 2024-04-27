import logging
import socket
import json

class TCPProcessClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = int(port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def connect(self):
        self.socket.connect((self.host, self.server_port))

    def close(self):
        self.socket.close()

    def TransferPayload(self, payload):
        logging.info(f"Transferring Payload to {self.host}:{self.server_port}...")
        data = {"payload": payload}
        json_data = json.dumps(data)
        try:
            self.socket.send(json_data.encode('utf-8'))
            self.socket.send('\n'.encode('utf-8'))
            response_data = self.socket.recv(1024).decode('utf-8')
            return json.loads(response_data)
        except BrokenPipeError:
            logging.error(f"Connection closed by the server ({self.host}:{self.server_port})")
            return None
        except ConnectionRefusedError:
            logging.error(f"Connection refused by the server ({self.host}:{self.server_port})")
            return None
        finally:
            self.close()