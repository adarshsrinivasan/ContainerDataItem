import ctypes
import socket
import sys
import time
import logging

from utils import sockaddr_in, generate_big_data, to_sockaddr, PF_INET

client_libc = ctypes.CDLL('libs/librdma_client_lib.so')

client_libc.start_client.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_char_p]


def start_client(sockaddr, str_to_send):
    logging.info("start_client: sending string of size: ", len(str_to_send))
    buf = ctypes.create_string_buffer(str_to_send.encode(), len(str_to_send))
    client_libc.start_client(sockaddr, buf)


# python client.sh.py -c 10.10.1.1 -p 12345
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
    list_of_big_data = []

    print("Generating data...")
    for x in range(0, 10):
        data = generate_big_data()
        list_of_big_data.append(data)
    print("Data generation completed!")

    time.sleep(1)
    for x in range(0, 5):
        print(f"Sending data of size : {len(list_of_big_data[x])}")
        start_client(sockaddr, list_of_big_data[x])
    print("Done")