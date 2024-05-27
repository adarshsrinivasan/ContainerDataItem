import ctypes
import socket
import sys
from utils import sockaddr_in, generate_big_data, to_sockaddr, PF_INET

client_libc = ctypes.CDLL('libs/librdma_client_lib.so')

client_libc.start_client.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_char_p]
client_libc.start_client.restype = ctypes.c_int


def start_client(sockaddr, str_to_send):
    print(f"Sending frame: {str_to_send}")
    print(str_to_send.encode('utf-16'))
    buf = ctypes.create_string_buffer(str_to_send.encode('utf-8'), len(str_to_send))
    if client_libc.start_client(sockaddr, buf) == 0:
        return True


# python client.py -c 10.10.1.1 -p 12345
if __name__ == '__main__':
    sock = socket.socket(PF_INET, socket.SOCK_DGRAM)
    af = socket.AF_INET
    if len(sys.argv) > 3 and sys.argv[1] == '-c':
        bind_addr = sys.argv[2]
    else:
        bind_addr = "0.0.0.0"
    if len(sys.argv) == 5 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345
    sockaddr = to_sockaddr(af, bind_addr, port)
    for x in range(1, 2):
        print(f"Sending {x}")
        str_to_send = generate_big_data()
        start_client(sockaddr, str_to_send)
    print("Done")