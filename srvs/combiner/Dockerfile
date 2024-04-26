FROM adarshzededa/cdi-common:latest

ARG service=combiner

WORKDIR /python-cdi

COPY . srvs/$service

ENV SERVICE=${service}
ENV PYTHONPATH=${PYTHONPATH}:/python-cdi/srvs/${SERVICE}/rpc_api

CMD python3 -u srvs/${SERVICE}/app.py