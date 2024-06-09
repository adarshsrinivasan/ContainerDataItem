FROM adarshzededa/cdi-common:latest

ARG service=extractor

WORKDIR /python-cdi

COPY srvs/$service srvs/$service

ENV SERVICE=${service}

CMD python3 -u srvs/${SERVICE}/app.py