FROM adarshzededa/cdi-common:latest

ARG service=workflow_worker

WORKDIR /python-cdi


COPY srvs/$service srvs/$service

ENV SERVICE=${service}

CMD python3 -u srvs/${SERVICE}/app.py