from library.rdma.msq import IPCMsgQueue
import concurrent.futures
from library.rdma.server import start_server

if __name__ == '__main__':
    msq = IPCMsgQueue(567)
    msq_queue = msq.get_queue()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Start the load operations and mark each future with its URL
        # for x in range(0, 5):
        while True:
            try:
                future = executor.submit(start_server, "10.10.1.1", 1234, msq)
                if future.exception():
                    print(future.exception())
                    continue
            except Exception as e:
                print(e)