import logging
import os
import time

from library.shm.shm_lib import SharedMemory


class SHM_access:

    def __init__(self, shm_id=0, size=0):
        self.shm_id = shm_id
        self.size = size
        self.__myshm = SharedMemory()
        self.__ptr = None

    def __shm_check_access(self, uid, gid):
        self.__myshm.shm_id = self.shm_id
        try:
            shmid_ds = self.__myshm.stat()
        except Exception as err:
            logging.error(f"Stat Error: {err}")
            return
        return shmid_ds.contents.shm_perm.uid == uid and shmid_ds.contents.shm_perm.gid == gid

    def __wait_for_access(self, timeout_seconds=1000):
        self.__myshm.shm_id = self.shm_id
        # logging.info(f"checking if current user uid: {os.getuid()}, gid: {os.getgid()} has access on the shared memory with shmid: {self.shm_id}")
        start_time = time.time()

        while not self.__shm_check_access(os.getuid(), os.getgid()):
            if timeout_seconds != -1 and time.time() - start_time > timeout_seconds:
                raise Exception(f"wait_for_access(shm_id={self.shm_id}, timeout_seconds={timeout_seconds}): Time out")
            else:
                continue
        # logging.info(f"user uid: {os.getuid()}, gid: {os.getgid()} has access on the shared memory with shmid: {self.shm_id}")
        return

    def __attach(self):
        self.__myshm.shm_id = self.shm_id
        self.__wait_for_access()
        self.__ptr = self.__myshm.attach()

    def __detach(self):
        self.__myshm.shm_id = self.shm_id
        self.__wait_for_access()
        self.__myshm.detach()
        self.__ptr = None

    def write_data(self, data: str):
        self.__myshm.shm_id = self.shm_id
        self.__attach()
        self.__myshm.write_data(data)
        self.__detach()
        logging.info(f"shm_write_data: wrote '{data}' to shared memory with shm_id: {self.shm_id}")

    def read_data(self, length=0) -> str:
        self.__myshm.shm_id = self.shm_id
        if length == 0:
            length = self.size
        self.__attach()
        data = self.__myshm.read_data(length)
        self.__detach()
        if len(data) > 0:
            logging.info(f"shm_read_data: read '{data}' from shared memory with shm_id: {self.shm_id}")
        return data

    def clear_data(self):
        self.__myshm.shm_id = self.shm_id
        self.__attach()
        self.__myshm.clear_data()
        self.__detach()
        # logging.info(f"shm_read_data: cleared data of shared memory with shm_id: {self.shm_id}")

    def change_owner(self, uid, gid):
        self.__myshm.shm_id = self.shm_id
        try:
            shmid_ds = self.__myshm.stat()
        except Exception as err:
            logging.error(f"change_owner - Stat Error: {err}")
            return

        try:
            self.__myshm.set(uid=uid, gid=gid, shm_mode=shmid_ds.contents.shm_perm.mode)
        except Exception as err:
            logging.error(f"change_owner - Set Error: {err}")
            return
