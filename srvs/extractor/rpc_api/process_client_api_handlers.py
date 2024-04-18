import logging

import grpc
import srvs.extractor.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.extractor.rpc_api.process_api_pb2 as pb2


class ProcessClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.ProcessServiceStub(self.channel)

    def TransferPayload(self, payload):
        logging.info(f"Transferring Payload to {self.host}:{self.server_port}...")
        message = pb2.TransferPayloadRequest(payload=payload)
        return self.stub.TransferPayload(message)
