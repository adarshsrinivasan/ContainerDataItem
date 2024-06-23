import ctypes
import socket
import string
import random

UNIX_PATH_MAX = 108
PF_UNIX = socket.AF_UNIX
PF_INET = socket.AF_INET

DATA_SIZE = 1024 * 1024


def SUN_LEN(path):
    """For AF_UNIX the addrlen is *not* sizeof(struct sockaddr_un)"""
    return ctypes.c_int(2 + len(path))


class sockaddr_un(ctypes.Structure):
    _fields_ = [("sa_family", ctypes.c_ushort),  # sun_family
                ("sun_path", ctypes.c_char * UNIX_PATH_MAX)]


class sockaddr_in(ctypes.Structure):
    _fields_ = [("sa_family", ctypes.c_ushort),  # sin_family
                ("sin_port", ctypes.c_ushort),
                ("sin_addr", ctypes.c_byte * 4),
                ("__pad", ctypes.c_byte * 8)]


def generate_big_data():
    alphanumeric_chars = string.ascii_letters + string.digits
    return ''.join(random.choice(alphanumeric_chars) for _ in range(DATA_SIZE - 1))


def to_sockaddr(family, address, port):
    if family == socket.AF_INET:
        addr = sockaddr_in()
        addr.sa_family = ctypes.c_ushort(family)
        if port:
            addr.sin_port = ctypes.c_ushort(socket.htons(int(port)))
        if address:
            bytes_ = [int(i) for i in address.split('.')]
            addr.sin_addr = (ctypes.c_byte * 4)(*bytes_)
    else:
        raise NotImplementedError('Not implemented family %s' % (family,))

    return addr
