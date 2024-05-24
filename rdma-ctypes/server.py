import ctypes
import socket
import sys
from utils import sockaddr_in, PF_INET, to_sockaddr

server_libc = ctypes.CDLL('libs/librdma_server_lib.so')

server_libc.start_rdma_server.argtypes = [ctypes.POINTER(sockaddr_in)]
server_libc.start_rdma_server.restype = ctypes.c_char_p


def start_server(sockaddr):
    received_frame = server_libc.start_rdma_server(sockaddr)
    print(f"Received Frame: {received_frame}")


# python server.py -l 10.10.1.1 -p 12345
if __name__ == '__main__':
    sock = socket.socket(PF_INET, socket.SOCK_DGRAM)
    af = socket.AF_INET
    if len(sys.argv) > 3 and sys.argv[1] == '-l':
        bind_addr = sys.argv[2]
    else:
        bind_addr = "0.0.0.0"
    if len(sys.argv) == 5 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345
    sockaddr = to_sockaddr(af, bind_addr, port)
    start_server(sockaddr)
