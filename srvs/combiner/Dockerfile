FROM python:3.9

ARG service=combiner

WORKDIR /python-$service

RUN chmod 777 /python-$service
RUN mkdir srvs

COPY library library
COPY srvs/$service srvs/$service
COPY Makefile Makefile

RUN pip3 install -r srvs/$service/requirements.txt
RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6 make  -y

ENV SERVICE=${service}
ENV PYTHONPATH=/python-${SERVICE}:/python-${SERVICE}/srvs/${SERVICE}/rpc_api

CMD PYTHONPATH=${PYTHONPATH} python3 -u srvs/${SERVICE}/app.py