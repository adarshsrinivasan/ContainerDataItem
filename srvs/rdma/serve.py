import sys

from library.rdma.msq import IPCMsgQueue
import concurrent.futures
from library.rdma.server import start_server

if __name__ == '__main__':
    # python server.py -l 10.1.1.1 -p 12345
    bind_addr = sys.argv[2]
    if len(sys.argv) == 5 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345

    msq = IPCMsgQueue(567)
    msq_queue = msq.get_queue()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        while True:
            try:
                future = executor.submit(start_server, bind_addr, port, msq)
                if future.exception():
                    print(future.exception())
                    continue
            except Exception as e:
                print(e)
