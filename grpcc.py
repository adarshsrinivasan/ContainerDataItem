import grpc
import time
import statistics
import rtt_service_pb2
import rtt_service_pb2_grpc

def measure_rtt(experiments=100):
    channel = grpc.insecure_channel(
        '128.110.216.215:50051',
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ]
    )
    stub = rtt_service_pb2_grpc.RTTServiceStub(channel)
    
    data = b'a' * 10_000_000  # 10 MB of data
    rtts = []

    for i in range(experiments):
        start_time = time.time()
        response = stub.MeasureRTT(rtt_service_pb2.RTTRequest(data=data))
        end_time = time.time()
        
        rtt = end_time - start_time
        rtts.append(rtt)
        print(f'Experiment {i+1}: Client RTT: {rtt*1000:.2f} ms, Received: {response.received_bytes} bytes')

    mean_rtt = statistics.mean(rtts)
    print(f'\nMean Client RTT over {experiments} experiments: {mean_rtt*1000:.2f} ms')

if __name__ == '__main__':
    measure_rtt()