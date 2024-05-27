from ctypes import *
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

libc = CDLL("libc.so.6")

libc.msgrcv.argtypes = [c_int, c_char_p, c_ulong, c_ulong, c_int]


class ipc_perm(Structure):
    _fields_ = [("__key", c_int),
                ("uid", c_uint),
                ("gid", c_uint),
                ("cuid", c_uint),
                ("cgid", c_uint),
                ("mode", c_ushort),
                ]


class msqid_ds(Structure):
    _fields_ = [("msg_perm", ipc_perm),
                ("msg_stime", c_ulong),
                ("msg_rtime", c_ulong),
                ("msg_ctime", c_ulong),
                ("__msg_cbytes", c_ulong),
                ("msg_qnum", c_ulong),
                ("msg_qbytes", c_ulong),
                ("msg_lspid", c_int),
                ("msg_lrpid", c_int)
                ]


class FrameMsg(Structure):
    _fields_ = [("ftype", c_ulong),
                ("ftext", c_char_p)]


class IPCMsgQueue:
    def __init__(self, key):
        self.msq_id = None
        self.key = key

    def receive_frame_from_queue(self, buf_size, callback_fn=None):
        cnt = 0
        while True:
            buf = create_string_buffer(buf_size + 16)
            len = libc.msgrcv(self.msq_id, buf, buf_size + 16, 1, 0)
            if len != -1:
                frame_msg = FrameMsg.from_buffer(buf)
                cnt += 1
                print("Received:", frame_msg.ftype)
                if callback_fn: callback_fn(frame_msg.ftext)
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
