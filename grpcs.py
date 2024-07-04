import grpc
from concurrent import futures
import rtt_service_pb2
import rtt_service_pb2_grpc

class RTTServicer(rtt_service_pb2_grpc.RTTServiceServicer):
    def MeasureRTT(self, request, context):
        received_bytes = len(request.data)
        return rtt_service_pb2.RTTResponse(received_bytes=received_bytes)

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ]
    )
    rtt_service_pb2_grpc.add_RTTServiceServicer_to_server(RTTServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()