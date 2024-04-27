import logging
import requests
import json

class RESTProcessClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port
        self.base_url = f"http://{self.host}:{self.server_port}"

    def TransferPayload(self, payload):
        logging.info(f"Transferring Payload to {self.host}:{self.server_port}...")
        url = f"{self.base_url}/transfer_payload"
        headers = {"Content-Type": "application/json"}
        data = {"payload": payload}
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()