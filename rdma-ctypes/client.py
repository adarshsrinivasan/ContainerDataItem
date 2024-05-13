import ctypes
import socket
import sys
from utils import sockaddr_in, generate_big_data, to_sockaddr, PF_INET

client_libc = ctypes.CDLL('libs/librdma_client_lib.so')

client_libc.connect_server.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_char_p]


def start_client(sockaddr, str_to_send):
    print(f"Sending frame: {str_to_send}")
    buf = ctypes.create_string_buffer(str_to_send.encode(), len(str_to_send))
    client_libc.connect_server(sockaddr, buf)


if __name__ == '__main__':
    sock = socket.socket(PF_INET, socket.SOCK_DGRAM)
    af = socket.AF_INET
    bind_addr = sys.argv[2] or "0.0.0.0"
    if len(sys.argv) == 5 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345
    sockaddr = to_sockaddr(af, bind_addr, port)
    str_to_send = generate_big_data()
    start_client(sockaddr, str_to_send)
