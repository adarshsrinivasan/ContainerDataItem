from library.rdma.client import start_client

if __name__ == '__main__':
    for i in range(0, 5):
        start_client("10.10.1.1", 12345, "hello world")