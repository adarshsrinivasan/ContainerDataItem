FROM adarshzededa/cdi-common:latest

ARG service=workflow_worker

WORKDIR /python-cdi


COPY srvs/$service srvs/$service

RUN pip3 install -r srvs/$service/requirements.txt

ENV SERVICE=${service}

CMD python3 -u srvs/${SERVICE}/app.py