import logging
import socket
import json
from srvs.detector.detector import Object_Detector

class ProcessService:
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, payload):
        logging.info("Received payload.")
        object_detector_obj = Object_Detector(packed_data=payload)
        object_detector_obj.run()
        return {"err": ""}

def serve_tcp(tcp_host, tcp_port):
    logging.info(f"Starting TCP server on : {tcp_host}:{tcp_port}")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((tcp_host, tcp_port))
    server_socket.listen(1)

    while True:
        client_socket, address = server_socket.accept()
        logging.info(f"Connected to client: {address}")

        try:
            data = client_socket.recv(1024).decode('utf-8')
            payload = json.loads(data)["payload"]

            process_service = ProcessService()
            response = process_service.TransferPayload(payload)

            response_data = json.dumps(response).encode('utf-8')
            client_socket.send(response_data)
        except Exception as e:
            logging.error(f"Error processing payload: {str(e)}")
        finally:
            client_socket.close()