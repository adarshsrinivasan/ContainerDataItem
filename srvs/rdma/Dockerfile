FROM python:3.9-slim

ARG service=rdma

WORKDIR /app

COPY library/common library/common
COPY library/rdma library/rdma
COPY Makefile Makefile
COPY srvs/$service srvs/$service

RUN apt-get update
RUN apt-get install libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev rdmacm-utils ibverbs-utils rdma-core -y
RUN apt-get install -y cmake libzmq3-dev nano vim perftest

RUN make build-rdma-c-lib

RUN pip3 install -r srvs/$service/requirements.txt

ENV SERVICE=${service}
ENV SHM_DLL_DIR_PATH=/app
ENV PYTHONPATH=/app:/app/srvs/rdma
