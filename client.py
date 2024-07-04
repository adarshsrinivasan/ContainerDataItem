import socket
import time
import statistics

def measure_rtt(host='128.110.216.215', port=65432):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        data = b'a' * 1000000  # 100KB of data
        start_time = time.time()
        s.sendall(data)
        # s.recv(1)
        end_time = time.time()
        rtt = end_time - start_time
        # print(f'Received {len(received_data)} bytes')
        return rtt

if __name__ == '__main__':
    rtts = []
    for i in range (100):
        time.sleep(0.3)
        rtts.append(measure_rtt())
    print(f'Client Round Trip Time (RTT): {statistics.mean(rtts)*1000} milliseconds')
