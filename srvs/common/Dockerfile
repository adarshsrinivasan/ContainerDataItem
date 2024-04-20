FROM python:3.9

ARG service=common

WORKDIR /python-cdi

RUN chmod 777 /python-cdi
RUN mkdir srvs

COPY library library
COPY Makefile Makefile
COPY srvs/$service srvs/$service

RUN apt-get update && apt-get -y install ffmpeg libsm6 libxext6 make

RUN pip3 install -r srvs/$service/requirements.txt

ENV SERVICE=${service}
ENV PYTHONPATH=/python-cdi
ENV OBJ_DET_MODEL_DIR_PATH=/python-cdi/srvs/${SERVICE}