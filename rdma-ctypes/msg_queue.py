import ctypes
import os
import logging

# constants
IPC_CREAT = 0o1000
IPC_EXCL = 0o2000
IPC_NOWAIT = 0o4000
IPC_RMID = 0
IPC_SET = 1
IPC_STAT = 2
IPC_INFO = 3

libc = ctypes.CDLL("libc.so.6")

libc.msgrcv.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_int]


class IPCMsgQueue:
    def __init__(self, key):
        self.msq_id = None
        self.key = key

    def receive_frame_from_queue(self, buf_size, callback_fn = None):
        cnt = 0
        while True:
            buf = ctypes.create_string_buffer(buf_size + 8)
            lbuf = ctypes.cast(buf, ctypes.POINTER(ctypes.c_long))  # for type
            len = libc.msgrcv(self.msq_id, buf, buf_size + 8, 1, 0)
            if len != -1:
                frame = buf[8: len + 8].decode('utf-8')
                cnt += 1
                logging.info("Received:", lbuf.contents.value, frame)
                if callback_fn: callback_fn(frame)
            else:
                logging.error(f"Error in receiving frame from the queue: cnt: {cnt} missing")
                os._exit(0)

    def get_queue(self):
        logging.info("Getting the queue")
        self.msq_id = libc.msgget(self.key, IPC_CREAT | 0o644)
        return self.msq_id

    def clear_queue(self):
        logging.info("Clearing the queue")
        if libc.msgctl(self.msq_id, IPC_RMID, 0) == -1:
            logging.error("Error in deleting the queue")
