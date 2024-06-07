FROM python:3.9

ARG service=testpod

WORKDIR /python-cdi

RUN chmod 777 /python-cdi
RUN mkdir srvs

COPY srvs/$service srvs/$service

RUN pip3 install -r srvs/$service/requirements.txt

ENV SERVICE=${service}

CMD python3 -u srvs/${SERVICE}/client.py