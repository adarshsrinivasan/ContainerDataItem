import socket
import time

def start_server(host='0.0.0.0', port=65432):
    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            while True:
                s.listen()
                print(f'Server listening on {host}:{port}')
                conn, addr = s.accept()
                with conn:
                    print('Connected by', addr)
                    data = b''
                    start_time = time.time()
                    while len(data) < 10000000:
                        packet = conn.recv(10000000)
                        if not packet:
                            break
                        data += packet
                    print(f'Received {len(data)} bytes')
                    # conn.sendall(b'1')  # Wait for a single byte acknowledgment
                    end_time = time.time()
                    server_rtt = end_time - start_time
                    print(f'Server Round Trip Time (RTT): {server_rtt*1000} milliseconds')

if __name__ == '__main__':
    start_server()
