import pickle

import numpy as np

from library.db.cache_db import Cache_DB

cache_client = None
cache_queue_name = "cdi_extractor_queue"
__local_cache = {}
__local_queue = []


def cache_pack_frame_data(frame):
    frame_shape = frame.shape
    frame_data_type = frame.dtype.name
    info_str = f"{frame_data_type}:{frame_shape[0]}:{frame_shape[1]}:{frame_shape[2]}"
    frame_data_str = frame.flatten().tostring()

    packed_data = f"{info_str}\n{frame_data_str}"
    return packed_data


def cache_unpack_frame_data(packed_data):
    data_split = packed_data.split("\n")
    info_split = data_split[0].split(":")

    frame_data_type = info_split[0].strip()
    frame_shape_x = int(info_split[1])
    frame_shape_y = int(info_split[2])
    frame_shape_z = int(info_split[3])

    frame = np.fromstring(eval(data_split[1]), dtype=frame_data_type).reshape(frame_shape_x, frame_shape_y,
                                                                              frame_shape_z)

    return frame


class Extractor_cache_data:
    def __init__(self, frame_count=0, x_shape=0, y_shape=0, frame_order=0):
        self.frame_count = frame_count
        self.x_shape = x_shape
        self.y_shape = y_shape
        self.frame_order = frame_order


def init_cache_client(host, port, password):
    global cache_client
    cache_client = Cache_DB(host=host, port=port, password=password)
    return cache_client


def add_frame_to_cache(stream_id, frame_order, frame):
    global cache_client
    key = f"extractor_{stream_id}_{frame_order}"
    packed_frame = cache_pack_frame_data(frame)
    cache_client.insert_data(key=key, value=packed_frame)


def get_frame_from_cache(stream_id, frame_order):
    global cache_client
    key = f"extractor_{stream_id}_{frame_order}"
    packed_frame = cache_client.read_data(key=key)
    if packed_frame is None:
        return None
    frame = cache_unpack_frame_data(packed_frame)
    return frame


def delete_frame_from_cache(stream_id, frame_order):
    global cache_client
    key = f"extractor_{stream_id}_{frame_order}"
    cache_client.delete_data(key=key)


def add_obj_to_cache(stream_id, obj):
    # global cache_client
    # pickled_object = pickle.dumps(obj)
    # cache_client.insert_data(key=stream_id, value=pickled_object)
    __local_cache[stream_id] = obj


def get_obj_from_cache(stream_id):
    # global cache_client
    # pickled_object = cache_client.read_data(key=stream_id)
    # if pickled_object is not None:
    #     obj = pickle.loads(pickled_object)
    #     return obj
    # return None
    if stream_id not in __local_cache.keys():
        return None
    return __local_cache[stream_id]


def delete_obj_from_cache(stream_id):
    # global cache_client
    # return cache_client.delete_data(key=stream_id)
    del __local_cache[stream_id]


def enqueue_obj_to_cache_queue(obj):
    # global cache_queue_name, cache_client
    # pickled_object = pickle.dumps(obj)
    # cache_client.enqueue_to_queue(cache_queue_name, pickled_object)
    __local_queue.append(obj)


def front_obj_of_cache_queue():
    # global cache_queue_name, cache_client
    # front_pickled_object = cache_client.front_of_queue(cache_queue_name)
    # if front_pickled_object is not None:
    #     obj = pickle.loads(front_pickled_object)
    #     return obj
    # return None
    if len(__local_queue) == 0:
        return None
    return __local_queue[0]


def dequeue_obj_from_cache_queue():
    # global cache_queue_name, cache_client
    # pickled_object = cache_client.dequeue_from_queue(cache_queue_name)
    # if pickled_object is not None:
    #     obj = pickle.loads(pickled_object)
    #     return obj
    # return None
    if len(__local_queue) == 0:
        return None
    front = __local_queue[0]
    del __local_queue[0]
    return front
