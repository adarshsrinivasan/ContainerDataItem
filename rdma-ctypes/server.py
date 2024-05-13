import ctypes
import socket
import sys
from utils import PF_INET, to_sockaddr, sockaddr_in, DATA_SIZE

server_libc = ctypes.CDLL('libs/librdma_server_lib.so')

server_libc.start_rdma_server.argtypes = [ctypes.POINTER(sockaddr_in)]
server_libc.start_rdma_server.restype = ctypes.c_char_p


def server_listen(sockaddr):
    received_frame = server_libc.start_rdma_server(sockaddr)
    print("Received frame: ", ctypes.string_at(received_frame, DATA_SIZE).decode())


if __name__ == '__main__':
    sock = socket.socket(PF_INET, socket.SOCK_DGRAM)
    af = socket.AF_INET
    bind_addr = sys.argv[2] or "0.0.0.0"
    if len(sys.argv) == 5 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345
    sockaddr = to_sockaddr(af, bind_addr, port)
    server_listen(sockaddr)
