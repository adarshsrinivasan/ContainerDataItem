import ctypes
import os
import socket
import logging
import sys

from library.common.constants import SHM_DLL_DIR_PATH_ENV
from library.common.utils import getenv_with_default
from library.rdma.utils import to_sockaddr, sockaddr_in

# load c library
dll_path = getenv_with_default(SHM_DLL_DIR_PATH_ENV, "")
if dll_path != "":
    dll_path = os.path.join(dll_path, '')
lib = ctypes.CDLL(f"{dll_path}rdma_client_lib_{sys.platform}.so")

lib.start_client.argtypes = [ctypes.POINTER(sockaddr_in), ctypes.c_char_p]


def start_client(host, port, payload):
    try:
        af = socket.AF_INET
        sockaddr = to_sockaddr(af, host, port)
        logging.info("start_client: sending string of size: ", len(payload))
        buf = ctypes.create_string_buffer(payload, len(payload))
        lib.start_client(sockaddr, buf)
    except Exception as e:
        err = f"Error: exception while running the client: {e}"
        logging.error(f"{err}\n")
        raise e