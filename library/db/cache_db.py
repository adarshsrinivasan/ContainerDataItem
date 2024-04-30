import redis
from rq import Queue


class Cache_DB:
    def __init__(self, host="0.0.0.0", port="6379", password="password"):
        self.client = redis.Redis(
            host=host,
            port=int(port),
            decode_responses=True,  # <-- this will ensure that binary data is decoded
            password=password
        )
        self.client.ping()

    def insert_data(self, key, value, is_value_dict=False):
        if is_value_dict:
            return self.client.hset(key, mapping=value)
        return self.client.set(key, value)

    def read_data(self, key, is_value_dict=False):
        if is_value_dict:
            return self.client.hgetall(key)
        return self.client.get(key)

    def delete_data(self, key):
        return self.client.delete(key)

    def enqueue_to_queue(self, queue_name, data):
        return self.client.rpush(queue_name, data)

    def front_of_queue(self, queue_name):
        front = self.client.rpop(queue_name)
        if front is not None:
            self.client.rpush(queue_name, front)
        return front

    def dequeue_from_queue(self, queue_name):
        return self.client.rpop(queue_name)

    def close(self):
        self.client.close()
