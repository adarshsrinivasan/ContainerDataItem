import ctypes
import logging
import os
import socket
import sys

from library.common.constants import SHM_DLL_DIR_PATH_ENV
from library.common.utils import getenv_with_default
from library.rdma.msq import IPCMsgQueue
from library.rdma.utils import to_sockaddr, sockaddr_in

# load c library
dll_path = getenv_with_default(SHM_DLL_DIR_PATH_ENV, "")
if dll_path != "":
    dll_path = os.path.join(dll_path, '')
lib = ctypes.CDLL(f"{dll_path}rdma_server_lib_{sys.platform}.so")

lib.start_rdma_server.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_int]
lib.start_rdma_server.restype = ctypes.c_char_p


def start_server(host, port, msq: IPCMsgQueue):
    try:
        af = socket.AF_INET
        logging.info(type(host), type(port), type(msq.msq_id))
        sockaddr = to_sockaddr(af, host, port)
        logging.info("socket created")
        lib.start_rdma_server(sockaddr, msq.msq_id)
    except Exception as e:
        err = f"Error: exception in starting the server: {e}"
        logging.error(f"{err}\n")
        logging.error(f"clearing the queue because of the exception\n")
        msq.clear_queue()
        raise e

