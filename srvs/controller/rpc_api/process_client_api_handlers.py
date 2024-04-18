import grpc
import srvs.controller.rpc_api.process_api_pb2_grpc as pb2_grpc
import srvs.controller.rpc_api.process_api_pb2 as pb2


class ProcessClient(object):
    def __init__(self, host, port):
        self.host = host
        self.server_port = port

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = pb2_grpc.ProcessServiceStub(self.channel)

    def NotifyCDIsAccess(self, cdi_controller_table_list):
        request_list = []
        for cdi_controller_table in cdi_controller_table_list:
            request_list.append(cdi_controller_table.as_proto_cdi_config())
        message = pb2.NotifyCDIsAccessRequest(cdi_configs=request_list)
        return self.stub.NotifyCDIsAccess(message)
