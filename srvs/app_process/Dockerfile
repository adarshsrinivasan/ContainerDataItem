FROM python:3.9

ARG service=extractor

WORKDIR /python-$service

RUN chmod 777 /python-$service
RUN mkdir srvs

COPY requirements.txt requirements.txt
COPY library library
COPY srvs/$service srvs/$service
COPY Makefile Makefile

RUN pip3 install -r requirements.txt
RUN apt-get -y install make

ENV SERVICE=${service}
ENV PYTHONPATH=/python-${SERVICE}:/python-${SERVICE}/srvs/${SERVICE}/rpc_api

RUN make build-shm-c-lib

CMD PYTHONPATH=${PYTHONPATH} SHM_DLL_DIR_PATH=/python-${SERVICE} python3 -u srvs/${SERVICE}/app.py