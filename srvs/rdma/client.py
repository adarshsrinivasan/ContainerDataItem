import concurrent.futures
import logging
import sys

from library.common.utils import generate_data_of_size_kb
from library.rdma.client import start_client

if __name__ == '__main__':
    # python client.py -c 10.1.1.1 -p 12345 -d 10
    bind_addr = sys.argv[2]
    if len(sys.argv) == 7 and sys.argv[3] == '-p':
        port = int(sys.argv[4])
    else:
        port = 12345
    if sys.argv[5] == '-d':
        data_size = int(sys.argv[6])
    else:
        data_size = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for x in range(0, 1):
            data = generate_data_of_size_kb(data_size)

            print(f"{x} : Sending data of size: {len(data)}")
            client_future = executor.submit(start_client, bind_addr, port, data.encode())
            if concurrent.futures.as_completed(client_future):
                try:
                    res = client_future.result()
                    logging.info(res)
                    if res == -1:
                        while True:
                            logging.info(f"Retrying sending {x}\n")
                            retry_client_fut = executor.submit(start_client, bind_addr, port, data.encode())
                            if concurrent.futures.as_completed(retry_client_fut):
                                if retry_client_fut.result() == -1:
                                    continue
                                else:
                                    logging.info("Complete")
                                    break
                except Exception as exc:
                    logging.info(f'Thread generated an exception: {exc}\n')
                else:
                    logging.info(f'Successful\n')
        print(f"Sending data of size: {len('Done')}")
        client_futures = executor.submit(start_client, bind_addr, port, b"Done", )
        if concurrent.futures.as_completed(client_future):
            try:
                res = client_future.result()
                if res == -1:
                    while True:
                        logging.info(f"Retrying sending done \n")
                        retry_client_fut = executor.submit(start_client, bind_addr, port,
                                                           b"Done")
                        if concurrent.futures.as_completed(retry_client_fut):
                            if retry_client_fut.result() == -1:
                                continue
                            else:
                                logging.info(f"Complete retrying done...\n")
                                break
                else:
                    logging.info(f"Received val of res done = {res} \n")
            except Exception as exc:
                logging.error(f'Thread generated an exception: {exc}\n')
            else:
                logging.info(f'Sending done successful :)\n')
        else:
            logging.info(f"Not completed done? \n")