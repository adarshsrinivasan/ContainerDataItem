import os
import grpc
import logging
import threading
import base64
import redis
import json
from concurrent import futures
from rpc_api.controller_client_api_handlers import register_with_controller, ControllerClient
from srvs.common.rpc_api import process_api_pb2_grpc as pb2_grpc, process_api_pb2 as pb2
from library.common.cdi_config_model import Config
from library.common.cdi_config_model import populate_config_from_parent_config, CDI
import cv2
import numpy as np
import requests

# Set up logging
logging.basicConfig(level=logging.INFO)

# CDI key and process ID
CDI_KEY = int(os.environ.get('CDI_KEY', "50067"))
uid = 998
guid = 22
PROCESS_ID = os.environ.get('PROCESS_ID', 'worker')
controller_host = os.environ.get('CONTROLLER_HOST', '0.0.0.0')
controller_port = os.environ.get('CONTROLLER_PORT', '50000')
controller_client = ControllerClient(host=controller_host, port=controller_port)
cdi_id = os.environ.get('CDI_ID', '880519e2-7221-4387-aa1a-49707fd70812')

redis_host = 'redis-service'
redis_port = 6379
password = "supersecurepassword"
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True,password=password )
try:
    r.ping()
    print("Connected to Redis successfully!")
except redis.exceptions.ConnectionError as e:
    print("Error: Failed to connect to Redis", e)

headers = {
    "Host": "cdi"
}
SERVER_URL = os.environ.get('WORKFLOW_CONTROLLER_URL', 'http://workflow-controller-service:5002/api/v1')


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
        logging.info(f"NotifyCDIsAccess: Received access for the : {request}")
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

def process_image(cdi_id, task_name):
    response = controller_client.GetCDIsByProcessID(process_id=PROCESS_ID)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    logging.info(f"Writing image data to CDI {cdi_id} for process {PROCESS_ID}")
    for id, cdi in cdi_config.cdis.items():
        if id == cdi_id:
            data = cdi.read_data()
            print(f'writing data in cdi {cdi_id} for process {PROCESS_ID} and task {task_name}')
            cdi.write_data(data)

def process_tasks(queue):
    while True:
        try:
            task_object = r.blpop(queue, timeout=0)
            if not task_object:
                continue
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
            print('Received task', task_name, task_id, 'from workflow', workflow_name, workflow_id, workflow_exec_id)
            process_image(request, task_name)

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

    register_with_controller(process_id=PROCESS_ID, name=container_name, namespace=container_namespace, node_ip=node_ip, host=container_ip, port=rpc_port, controller_host=controller_host, controller_port=controller_port, uid=uid, gid=gid)
    logging.info("Successfully registered with Controller!")
    register_worker(rpc_host, rpc_port, queue, 2, PROCESS_ID)
    logging.info("Successfully registered with workflow Controller!")
    process_tasks(queue)
