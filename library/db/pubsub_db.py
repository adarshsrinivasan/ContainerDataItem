import redis


class PubSub_DB:
    def __int__(self, host="0.0.0.0", port="6379"):
        self.client = redis.Redis(
            host=host,
            port=int(port),
            decode_responses=True  # <-- this will ensure that binary data is decoded
        )
        self.client.ping()
        self.pubsub_obj = None

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

    def publish_data(self, key, value):
        return self.client.publish(key, value)

    def wait_for_change(self, key):
        if self.pubsub_obj is None:
            self.pubsub_obj = self.client.pubsub()
            self.pubsub_obj.subscribe(key)
        return self.pubsub_obj.listen()
