import os
import random
import string
import base64


def getenv_with_default(env_name, default):
    value = os.getenv(env_name)
    if value is None or len(value) == 0:
        value = default
    return value


def generate_data_of_size_kb(date_size_kb: int) -> str:
    size_bytes = date_size_kb * 1024

    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))

    return random_string


def encode_payload(decoded_payload):
    return base64.b64encode(decoded_payload.encode("ascii")).decode("ascii")

def decode_payload(encoded_payload):
    return base64.b64decode(encoded_payload).decode("ascii")