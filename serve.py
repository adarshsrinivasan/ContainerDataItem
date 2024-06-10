from library.rdma.msq import IPCMsgQueue
from library.rdma.server import start_server

if __name__ == '__main__':
    msq = IPCMsgQueue(567)
    msq_queue = msq.get_queue()
    start_server("10.10.1.1", 12345, msq)