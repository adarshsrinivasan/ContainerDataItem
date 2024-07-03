import os
import grpc
import logging
import threading
import base64
import redis
import json
from concurrent import futures
from uuid import uuid4
from rpc_api.controller_client_api_handlers import register_with_controller, ControllerClient
from srvs.common.rpc_api import process_api_pb2_grpc as pb2_grpc, process_api_pb2 as pb2
from library.common.cdi_config_model import Config
from library.common.cdi_config_model import populate_config_from_parent_config, CDI
import cv2
import numpy as np
import requests
import time
import boto3
from PIL import Image
from io import BytesIO
Image.MAX_IMAGE_PIXELS = None

# Set up logging
logging.basicConfig(level=logging.INFO)

# CDI key and process ID
CDI_KEY = int(os.environ.get('CDI_KEY', "50067"))
uid = 998
guid = 22
controller_host = os.environ.get('CONTROLLER_HOST', '0.0.0.0')
controller_port = os.environ.get('CONTROLLER_PORT', '50000')
controller_client = ControllerClient(host=controller_host, port=controller_port)
PROCESS_ID = os.environ.get('PROCESS_ID', 'worker')

redis_host = 'redis-service'
redis_port = 6379
password = "supersecurepassword"
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, password=password)
try:
    r.ping()
    print("Connected to Redis successfully!")
except redis.exceptions.ConnectionError as e:
    print("Error: Failed to connect to Redis", e)

headers = {
    "Host": "cdi"
}
SERVER_URL = os.environ.get('WORKFLOW_CONTROLLER_URL', 'http://workflow-controller-service:5002/api/v1')

s3 = boto3.client('s3',
                        aws_access_key_id='AKIAUTA4UV6WXMYCSA74',
                        aws_secret_access_key='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',
                        region_name='ap-south-1')

def register_worker(ip, port, queue_name, pool_count, process_id):
    data = {
        'ip': ip,
        'port': port,
        'queue_name': queue_name,
        'pool_count': pool_count,
        'process_id': process_id
    }
    response = requests.post(f'{SERVER_URL}/register_worker', json=data, headers=headers)
    print(f"Register worker response: {response.json()}")


class ProcessService(pb2_grpc.ProcessServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def NotifyCDIsAccess(self, request, context):
        logging.info(f"NotifyCDIsAccess: Received access for the : {request} {time.time()}")
        return pb2.NotifyCDIsAccessResponse(err="")


def serve_rpc(rpc_host, rpc_port):
    logging.info(f"Starting RPC server on: {rpc_host}:{rpc_port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ProcessServiceServicer_to_server(ProcessService(), server)
    server.add_insecure_port(f"{rpc_host}:{rpc_port}")
    server.start()
    try:
        server.wait_for_termination()
    except Exception as e:
        logging.error(f"Error in serve_rpc: {e}")
        server.stop(0)
        serve_rpc(rpc_host, rpc_port)


def blur_image(image):
    return cv2.GaussianBlur(image, (15, 15), 0)


def rotate_image(image):
    (h, w) = image.shape[:2]
    center = (w / 2, h / 2)
    M = cv2.getRotationMatrix2D(center, 180, 1.0)
    return cv2.warpAffine(image, M, (w, h))


def flip_image(image):
    return cv2.flip(image, 1)


def filter_cdi(cdi_config, cdi_id):
    cdi_items = list(cdi_config.cdis.items())
    for id, _ in cdi_items:
        if id != cdi_id:
            del cdi_config.cdis[id]


def process_image(process_id, cdi_id, task_name):
    response = controller_client.GetCDIsByProcessID(process_id=process_id)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    filter_cdi(cdi_config, cdi_id)
    for id, cdi in cdi_config.cdis.items():
        if id == cdi_id:
            data = cdi.read_data()
            image_data = base64.b64decode(data)
            image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)

            if task_name == 'UNBLUR_IMAGE':
                processed_image = blur_image(image)
            elif task_name == 'CROP_IMAGE':
                processed_image = rotate_image(image)
            else:
                processed_image = flip_image(image)

            _, buffer = cv2.imencode('.jpg', processed_image)
            processed_image_data = base64.b64encode(buffer).decode("utf-8")
            cdi.write_data(processed_image_data)

def saveWorkflowOutput(cdi_config, cdi_id):
    for id, cdi in cdi_config.cdis.items():
        if id == cdi_id:
            data = cdi.read_data()
            image_data = base64.b64decode(data)
            image = Image.open(BytesIO(image_data))
            
            buffered = BytesIO()
            image.save(buffered, format="JPEG")
            buffered.seek(0)  # Rewind the buffer to the beginning
            s3.put_object(Bucket='orkes-image-data', Key=f'cdi_{cdi_id}.jpg', Body=buffered, ContentType='image/jpeg')
            print(f'Uploaded file cdi_{cdi_id}.jpg to S3')
            break

def completeWorkflow(pId, cdi_id):
    response = controller_client.GetCDIsByProcessID(pId)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    filter_cdi(cdi_config, cdi_id)
    print(f'completing workflow {cdi_id}')
    saveWorkflowOutput(cdi_config, cdi_id)
    response = controller_client.DeleteCDIs(pId, cdi_config)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    else:
        print(f'Deleted CDI for the process {pId}')

def process_tasks(queue):
    while True:
        try:
            task_object = r.blpop(queue, timeout=0)
            if not task_object:
                continue

            time.sleep(5)
            _, task_json = task_object
            task_dict = json.loads(task_json)
            task_name = task_dict['task_name']
            request = task_dict['request']
            task_id = task_dict['task_id']
            workflow_name = task_dict['workflow_name']
            workflow_id = task_dict['workflow_id']
            workflow_exec_id = task_dict['workflow_exec_id']
            tasks_id = task_dict['tasks_id']
            worker_id = task_dict['worker_id']
            print('Received task', task_name, task_id, 'from workflow', workflow_name, workflow_id, workflow_exec_id, tasks_id,len(tasks_id)-1, tasks_id[len(tasks_id)-1], task_id)
            process_image(PROCESS_ID, request, task_name)

            if(int(tasks_id[len(tasks_id)-1]) == int(task_id)):
                print("finished execution, uploading the response to storage")
                completeWorkflow(PROCESS_ID, request)

            data = {
                'worker': {
                    'data': 'worker',
                    'workflow_exec_id': workflow_exec_id,
                    'workflow_name': workflow_name,
                    'task_name': task_name,
                    'workflow_id': workflow_id,
                    'task_id': task_id,
                    'tasks_id': tasks_id,
                    'request': request,
                    'worker_id': worker_id
                }
            }
            worker_json = json.dumps(data)
            r.rpush('scheduler_queue', worker_json)
            print('Task completed:', task_name, task_id)
        except Exception as e:
            logging.error(f"Error processing task: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Registering Worker with Controller...")

    node_ip = os.environ.get('NODE_IP', '0.0.0.0')
    container_name = os.environ.get('CONTAINER_NAME', 'worker')
    container_ip = os.environ.get('CONTAINER_IP', '0.0.0.0')
    container_namespace = os.environ.get('CONTAINER_NAMESPACE', 'default')
    rpc_host = os.environ.get('RPC_HOST', '0.0.0.0')
    rpc_port = os.environ.get('RPC_PORT', '5003')
    queue = os.environ.get('QUEUE', 'worker-queue')
    uid = os.getuid()
    gid = os.getgid()

    # Serve the RPC service
    async_serve_rpc = threading.Thread(target=serve_rpc, args=(rpc_host, rpc_port), kwargs={})
    async_serve_rpc.start()

    register_with_controller(process_id=PROCESS_ID, name=container_name, namespace=container_namespace,
                             node_ip=node_ip, host=container_ip, port=rpc_port, controller_host=controller_host,
                             controller_port=controller_port, uid=uid, gid=gid)
    logging.info(f"Successfully registered process {PROCESS_ID} with Controller!")
    register_worker(rpc_host, rpc_port, queue, 10, PROCESS_ID)

    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        for _ in range(10):
            executor.submit(process_tasks, queue)

    # Ensure the main thread waits for all tasks to complete
    async_serve_rpc.join()
