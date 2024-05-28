import logging

from library.common.constants import HOST_ENV, PORT_ENV
from library.common.utils import getenv_with_default
from srvs.extractor.rest_api.server_api_handler import serve_rest
from library.db.evaluation_db import create_table


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting extractor 0_0")

    rest_host = getenv_with_default(HOST_ENV, "0.0.0.0")
    rest_port = getenv_with_default(PORT_ENV, "50002")

    # create_table()
    serve_rest(host=rest_host, port=rest_port)

