#!/usr/bin/env python
import logging
import os
import sys

from ctypes import c_ushort, c_int, c_uint, c_ulong, c_size_t, c_void_p, string_at, c_char, c_long, cast, c_char_p
from ctypes import CDLL, POINTER, Structure, pointer, create_string_buffer, memmove

from library.common.constants import SHM_DLL_DIR_PATH_ENV
from library.common.utils import getenv_with_default

# define constants
IPC_CREAT = 0o1000
IPC_EXCL = 0o2000
IPC_NOWAIT = 0o4000

IPC_PRIVATE = 0

IPC_RMID = 0
IPC_SET = 1
IPC_STAT = 2
IPC_INFO = 3

SHM_RDONLY = 0o10000
SHM_RND = 0o20000
SHM_REMAP = 0o40000
SHM_EXEC = 0o100000

SHM_LOCK = 11
SHM_UNLOCK = 12

SHM_HUGETLB = 0o4000
SHM_HUGE_2MB = 21 << 26
SHM_HUGE_1GB = 30 << 26
SHM_NORESERVE = 0o10000

SHM_SIZE = 1024
SHM_KEY = 5678

# load c library
dll_path = getenv_with_default(SHM_DLL_DIR_PATH_ENV, "")
if dll_path != "":
    dll_path = os.path.join(dll_path, '')
lib = CDLL(f"{dll_path}shm_lib_{sys.platform}.so")


# define structs
class ipc_perm_new(Structure):
    _fields_ = [("uid", c_uint),
                ("gid", c_uint),
                ("cuid", c_uint),
                ("cgid", c_uint),
                ("mode", c_ushort),
                ]


class shmid_ds_new(Structure):
    _fields_ = [("shm_perm", ipc_perm_new),
                ("shm_segsz", c_size_t),
                ("shm_lpid", c_int),
                ("shm_cpid", c_int),
                ("shm_nattch", c_ushort),
                ("shm_atime", c_long),
                ("shm_dtime", c_long),
                ("shm_ctime", c_long),
                ]


# Define methods

# int Create(key_t key, size_t size, int flags);
lib.Create.argtypes = [c_int, c_size_t, c_int]
lib.Create.restype = c_int

# void* Attach(int shmid);
lib.Attach.argtypes = [c_int]
lib.Attach.restype = c_void_p

# int Detach(void* ptr);
lib.Detach.argtypes = [c_void_p]
lib.Detach.restype = c_int

# struct shmid_ds_new* Stat(int shmid);
lib.Stat.argtypes = [c_int]
lib.Stat.restype = POINTER(shmid_ds_new)

# int Set(int shmid, uid_t uid, gid_t gid, mode_t mode);
lib.Set.argtypes = [c_int, c_uint, c_uint, c_ushort]
lib.Set.restype = c_int

# int Remove(int shmid);
lib.Remove.argtypes = [c_int]
lib.Remove.restype = c_int

# void Write(void* ptr, const char* str);
lib.Write.argtypes = [c_void_p, POINTER(c_char)]
lib.Write.restype = None

# char* Read(void* ptr);
lib.Read.argtypes = [c_void_p]
lib.Read.restype = c_char_p

# void Clear(void* ptr, size_t size);
lib.Clear.argtypes = [c_void_p, c_size_t]
lib.Read.restype = None


class SharedMemory:
    def __init__(self, size=0, shm_id=-1, key=-1, shm_mode=666, uid=0, gid=0) -> None:
        self.size = size
        self.shm_id = shm_id
        self.key = key
        self.shm_mode = int("{}".format(shm_mode), 8)
        self.uid = uid
        self.gid = gid
        self.at_ptr = None

    def create(self, print_stat=False):
        self.shm_id = lib.Create(self.key, self.size, IPC_CREAT | self.shm_mode)
        if self.shm_id < 0:
            err = f"Error: exception while create operation. shm_id: {self.shm_id}"
            logging.error(f"{err}\n")
            raise Exception(err)
        assert self.shm_id >= 0

        try:
            shmid_ds = self.stat()
        except Exception as err:
            logging.error(f"Stat Error: {err}")
            return

        if print_stat:
            logging.info("\nCreate: shmid_ds Information:")
            logging.info(f"key: {self.key}")
            logging.info(f"shmid: {self.shm_id}")
            logging.info(f"Mode: {shmid_ds.contents.shm_perm.mode}")
            logging.info(f"UID: {shmid_ds.contents.shm_perm.uid}")
            logging.info(f"GID: {shmid_ds.contents.shm_perm.gid}")
            logging.info(f"Size: {shmid_ds.contents.shm_segsz} bytes")
            logging.info(f"Last attach time: {shmid_ds.contents.shm_atime}")
            logging.info(f"Last detach time: {shmid_ds.contents.shm_dtime}")
            logging.info(f"Last change time: {shmid_ds.contents.shm_ctime}")
            logging.info(f"Number of attaches: {shmid_ds.contents.shm_nattch}\n")

        return self.shm_id

    def load_with_shmid(self):
        try:
            shmid_ds = self.stat()
        except Exception as err:
            logging.error(f"Stat Error: {err}")
            return

        self.size = shmid_ds.contents.shm_segsz
        self.shm_mode = shmid_ds.contents.shm_perm.mode
        self.uid = shmid_ds.contents.shm_perm.uid
        self.gid = shmid_ds.contents.shm_perm.gid

    def attach(self):
        self.at_ptr = lib.Attach(self.shm_id)
        if self.at_ptr is None:
            err = f"Error: exception while attach operation. at_ptr: None"
            logging.error(f"{err}\n")
            raise Exception(err)
        return self.at_ptr

    def detach(self):
        if self.at_ptr is not None:
            ret = lib.Detach(self.at_ptr)
            if self.at_ptr is None:
                err = f"Error: exception while detach operation. ret: {ret}"
                logging.error(f"{err}\n")
                raise Exception(err)
            self.at_ptr = None
        else:
            raise Exception("attach pointer not set")

    def stat(self):
        stat_res = lib.Stat(self.shm_id)
        if stat_res is None:
            err = f"Error: exception while stat operation. stat_res: None"
            logging.error(f"{err}\n")
            raise Exception(err)
        stat_res.contents.shm_perm.mode = int(str(oct(stat_res.contents.shm_perm.mode)).removeprefix('0o'))
        return stat_res

    def set(self, uid: int, gid: int, shm_mode: int):
        self.uid = uid
        self.gid = gid
        self.shm_mode = int("{}".format(shm_mode), 8)
        set_res = lib.Set(self.shm_id, self.uid, self.gid, self.shm_mode)
        if set_res < 0:
            err = f"Error: exception while set operation. set_res: {set_res}"
            logging.error(f"{err}\n")
            raise Exception(err)

        try:
            shmid_ds = self.stat()
        except Exception as err:
            logging.error(f"Stat Error: {err}")
            return

        logging.info("\nSet: shmid_ds Information:")
        logging.info(f"key: {self.key}")
        logging.info(f"shmid: {self.shm_id}")
        logging.info(f"Mode: {shmid_ds.contents.shm_perm.mode}")
        logging.info(f"UID: {shmid_ds.contents.shm_perm.uid}")
        logging.info(f"GID: {shmid_ds.contents.shm_perm.gid}")
        logging.info(f"Size: {shmid_ds.contents.shm_segsz} bytes")
        logging.info(f"Last attach time: {shmid_ds.contents.shm_atime}")
        logging.info(f"Last detach time: {shmid_ds.contents.shm_dtime}")
        logging.info(f"Last change time: {shmid_ds.contents.shm_ctime}")
        logging.info(f"Number of attaches: {shmid_ds.contents.shm_nattch}\n")

    def write_data(self, data: str):
        assert self.at_ptr is not None
        buf = create_string_buffer(data.encode())
        lib.Write(self.at_ptr, buf)

    def read_data(self, length) -> str:
        assert self.at_ptr is not None
        data = string_at(self.at_ptr, length).decode('utf-8').split('\x00')[0]
        return data

    def clear_data(self):
        assert self.at_ptr is not None
        lib.Clear(self.at_ptr, self.size)

    def is_empty(self):
        return len(self.read_data(self.size)) == 0

    def remove(self):
        try:
            self.detach()
        except Exception as err:
            if f"{err}" != "attach pointer not set":
                raise err

        rmv_res = lib.Remove(self.shm_id)
        if rmv_res < 0:
            err = f"Error: exception while remove operation. rmv_res: {rmv_res}"
            logging.error(f"{err}\n")
            raise Exception(err)
        self.shm_id = -1


def Test():
    data = "testdata1test"
    myshm = SharedMemory(size=SHM_SIZE, key=SHM_KEY, shm_mode=644)
    shm_id = -1
    try:
        shm_id = myshm.create()
    except Exception as err:
        logging.error(f"Create Error: {err}")
        return

    try:
        myshm.attach()
    except Exception as err:
        print(f"Attach Error: {err}")
        return

    try:
        shmid_ds = myshm.stat()
    except Exception as err:
        print(f"Stat Error: {err}")
        return

    print("\nshmid_ds Information:")
    print(f"key: {SHM_KEY}")
    print(f"shmid: {shm_id}")
    print(f"Mode: {shmid_ds.contents.shm_perm.mode}")
    print(f"UID: {shmid_ds.contents.shm_perm.uid}")
    print(f"GID: {shmid_ds.contents.shm_perm.gid}")
    print(f"Size: {shmid_ds.contents.shm_segsz} bytes")
    print(f"Last attach time: {shmid_ds.contents.shm_atime}")
    print(f"Last detach time: {shmid_ds.contents.shm_dtime}")
    print(f"Last change time: {shmid_ds.contents.shm_ctime}")
    print(f"Number of attaches: {shmid_ds.contents.shm_nattch}\n")

    myshm.write_data(data)
    print(f"Data: {myshm.read_data(len(data))}")
    print(f"IsEmpty: {myshm.is_empty()}")
    myshm.clear_data()
    print(f"Data after clear: {myshm.read_data(len(data))}")
    print(f"IsEmpty: {myshm.is_empty()}")

    try:
        myshm.set(0, 0, 666)
    except Exception as err:
        print(f"Set Error: {err}")
        return

    try:
        shmid_ds_new1 = myshm.stat()
    except Exception as err:
        print(f"Stat Error: {err}")
        return

    print("\nshmid_ds Information:")
    print(f"key: {SHM_KEY}")
    print(f"shmid: {shm_id}")
    print(f"Mode: {shmid_ds_new1.contents.shm_perm.mode}")
    print(f"UID: {shmid_ds_new1.contents.shm_perm.uid}")
    print(f"GID: {shmid_ds_new1.contents.shm_perm.gid}")
    print(f"Size: {shmid_ds_new1.contents.shm_segsz} bytes")
    print(f"Last attach time: {shmid_ds_new1.contents.shm_atime}")
    print(f"Last detach time: {shmid_ds_new1.contents.shm_dtime}")
    print(f"Last change time: {shmid_ds_new1.contents.shm_ctime}")
    print(f"Number of attaches: {shmid_ds_new1.contents.shm_nattch}\n")

    try:
        myshm.set(shmid_ds.contents.shm_perm.uid, shmid_ds.contents.shm_perm.gid, shmid_ds.contents.shm_perm.mode)
    except Exception as err:
        print(f"Set Error: {err}")
        return

    try:
        shmid_ds_new2 = myshm.stat()
    except Exception as err:
        print(f"Stat Error: {err}")
        return

    print("\nshmid_ds Information:")
    print(f"key: {SHM_KEY}")
    print(f"shmid: {shm_id}")
    print(f"Mode: {shmid_ds_new2.contents.shm_perm.mode}")
    print(f"UID: {shmid_ds_new2.contents.shm_perm.uid}")
    print(f"GID: {shmid_ds_new2.contents.shm_perm.gid}")
    print(f"Size: {shmid_ds_new2.contents.shm_segsz} bytes")
    print(f"Last attach time: {shmid_ds_new2.contents.shm_atime}")
    print(f"Last detach time: {shmid_ds_new2.contents.shm_dtime}")
    print(f"Last change time: {shmid_ds_new2.contents.shm_ctime}")
    print(f"Number of attaches: {shmid_ds_new2.contents.shm_nattch}\n")

    try:
        myshm.detach()
    except Exception as err:
        print(f"Detach Error: {err}")
        return

    print("detached")

    try:
        myshm.remove()
    except Exception as err:
        print(f"Remove Error: {err}")
        return

    print("removed")


if __name__ == '__main__':
    Test()
