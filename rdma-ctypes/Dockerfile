FROM python:3.9-slim


WORKDIR /app
RUN apt-get update
RUN apt-get install libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev rdmacm-utils ibverbs-utils rdma-core -y
RUN apt-get install nano perftest -y

COPY libs/ libs/
COPY rdma_py/ rdma_py/
COPY src/ src/
COPY CMakeLists.txt .
COPY Makefile .



