import concurrent.futures
import logging

from library.common.utils import generate_data_of_size_kb
from library.rdma.client import start_client

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Start the load operations and mark each future with its URL
        for x in range(0, 5):
            data = generate_data_of_size_kb(100 * x)
            client_future = executor.submit(start_client, "10.10.1.1", 1234, data.encode())
            if concurrent.futures.as_completed(client_future):
                try:
                    res = client_future.result()
                    logging.info(res)
                    if res == -1:
                        while True:
                            logging.info(f"Retrying sending {x}\n")
                            retry_client_fut = executor.submit(start_client, "10.10.1.1", 1234, data.encode())
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
        client_futures = executor.submit(start_client, "10.10.1.1", 1234, b"Done", )