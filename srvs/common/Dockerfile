FROM python:3.9

ARG service=common

WORKDIR /python-cdi

RUN chmod 777 /python-cdi
RUN mkdir srvs

COPY library library
COPY Makefile Makefile
COPY srvs/$service srvs/$service

RUN apt-get update && apt-get -y install ffmpeg libsm6 libxext6 make
RUN apt-get install rdma-core libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev rdmacm-utils ibverbs-utils -y

RUN pip3 install -r srvs/$service/requirements.txt

RUN make build-shm-c-lib
RUN make build-rdma-c-lib

ENV SERVICE=${service}
ENV PYTHONPATH=/python-cdi:/python-cdi/srvs/${SERVICE}/rpc_api
ENV OBJ_DET_MODEL_DIR_PATH=/python-cdi/srvs/${SERVICE}
ENV SHM_DLL_DIR_PATH=/python-cdi