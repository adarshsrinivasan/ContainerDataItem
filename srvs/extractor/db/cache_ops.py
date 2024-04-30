import pickle

from library.db.cache_db import Cache_DB

cache_client = None
cache_queue_name = "cdi_extractor_queue"


class Extractor_cache_data:
    def __init__(self, player=None, frame_count=0, x_shape=0, y_shape=0, frame_order=0):
        self.player = player
        self.frame_count = frame_count
        self.x_shape = x_shape
        self.y_shape = y_shape
        self.frame_order = frame_order


def init_cache_client(host, port, password):
    global cache_client
    cache_client = Cache_DB(host=host, port=port, password=password)
    return cache_client


def add_obj_to_cache(stream_id, obj):
    global cache_client
    pickled_object = pickle.dumps(obj)
    cache_client.insert_data(key=stream_id, value=pickled_object)


def get_obj_from_cache(stream_id):
    global cache_client
    pickled_object = cache_client.read_data(key=stream_id)
    if pickled_object is not None:
        obj = pickle.loads(pickled_object)
        return obj
    return None


def delete_obj_from_cache(stream_id):
    global cache_client
    return cache_client.delete_data(key=stream_id)


def enqueue_obj_to_cache_queue(obj):
    global cache_queue_name, cache_client
    pickled_object = pickle.dumps(obj)
    cache_client.enqueue_to_queue(cache_queue_name, pickled_object)


def front_obj_of_cache_queue():
    global cache_queue_name, cache_client
    front_pickled_object = cache_client.front_of_queue(cache_queue_name)
    if front_pickled_object is not None:
        obj = pickle.loads(front_pickled_object)
        return obj
    return None


def dequeue_obj_from_cache_queue():
    global cache_queue_name, cache_client
    pickled_object = cache_client.dequeue_from_queue(cache_queue_name)
    if pickled_object is not None:
        obj = pickle.loads(pickled_object)
        return obj
    return None
