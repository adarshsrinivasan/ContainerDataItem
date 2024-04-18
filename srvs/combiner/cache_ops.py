from library.db.cache_db import Cache_DB


def init_cache_client(host, port, password):
    client = Cache_DB(host=host, port=port, password=password)
    return client
