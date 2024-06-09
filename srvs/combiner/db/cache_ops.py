from library.db.cache_db import Cache_DB

cache_client = None


def init_cache_client(host, port, password):
    global cache_client
    cache_client = Cache_DB(host=host, port=port, password=password)
    return cache_client


def add_frame_obj_to_cache(key, frame_obj):
    global cache_client
    cache_client.insert_data(key=key, value=frame_obj)


def get_frame_obj_from_cache(key):
    global cache_client
    return cache_client.read_data(key=key)


def delete_frame_obj_from_cache(key):
    global cache_client
    return cache_client.delete_data(key=key)
