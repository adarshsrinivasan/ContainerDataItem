import ctypes
import socket
import sys
import os
from utils import sockaddr_in, PF_INET, to_sockaddr, DATA_SIZE
import threading
from msq import IPCMsgQueue

server_libc = ctypes.CDLL('libs/librdma_server_lib.so')

server_libc.start_rdma_server.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_int]
server_libc.start_rdma_server.restype = ctypes.c_char_p


# *implement your logic here*
def handle_frame(frame):
    print(f"Handle frame called with frame of size: ", len(frame))


def start_server(sockaddr, msg_queue):
    try:
        server_libc.start_rdma_server(sockaddr, msg_queue.msq_id)
    except KeyboardInterrupt:
        msg_queue.clear_queue()
        os._exit(0)


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

    msg_queue = IPCMsgQueue(1234)
    msq_id = msg_queue.get_queue()

    try:
        t1 = threading.Thread(target=msg_queue.receive_frame_from_queue, args=(DATA_SIZE, handle_frame,))
        t2 = threading.Thread(target=start_server, args=(sockaddr, msg_queue))
        t1.start()
        t2.start()

        t1.join()
        t2.join()

    except KeyboardInterrupt:
        msg_queue.clear_queue()
        sys.exit(1)
