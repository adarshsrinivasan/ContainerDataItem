FROM adarshzededa/cdi-common:latest

RUN apt-get install -y rdma-core libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev rdmacm-utils ibverbs-utils
RUN apt-get install -y cmake libzmq3-dev nano vim perftest

ARG service=minion

WORKDIR /python-cdi

COPY srvs/$service srvs/$service

ENV SERVICE=${service}

CMD python3 -u srvs/${SERVICE}/app.py


