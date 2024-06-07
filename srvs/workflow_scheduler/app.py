import os
import sys
import redis
import psycopg2
import json
import base64
import time
import grpc
import random
import threading
from concurrent import futures
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
import uuid
import logging
import boto3
from rpc_api.controller_client_api_handlers import register_with_controller, ControllerClient
from srvs.common.rpc_api import process_api_pb2_grpc as pb2_grpc, process_api_pb2 as pb2
from library.common.cdi_config_model import Config
from library.common.cdi_config_model import populate_config_from_parent_config, CDI
from PIL import Image
from io import BytesIO

# Global variable to store the last push time
last_push_time = None
times = []

redis_host = 'redis-service'
redis_port = 6379
password = "supersecurepassword"
r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True,password=password )
try:
    r.ping()
    print("Connected to Redis successfully!")
except redis.exceptions.ConnectionError as e:
    print("Error: Failed to connect to Redis", e)

conn = psycopg2.connect(database="cdi",
                                user="admin",
                                host="postgres",
                                password="admin",
                                port=5432)

s3 = boto3.client('s3',
                        aws_access_key_id='AKIAUTA4UV6WXMYCSA74',
                        aws_secret_access_key='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',
                        region_name='ap-south-1')


node_ip = os.environ.get('NODE_IP', '0.0.0.0')
container_name = os.environ.get('CONTAINER_NAME', 'worker')
container_ip = os.environ.get('CONTAINER_IP', '0.0.0.0')
container_namespace = os.environ.get('CONTAINER_NAMESPACE', 'default')
rpc_host = os.environ.get('RPC_HOST', '0.0.0.0')
rpc_port = os.environ.get('RPC_PORT', '5001')
uid = os.getuid()
gid = os.getgid()
process_id = os.environ.get('PROCESS_ID', 'scheduler')
controller_host = os.environ.get('CONTROLLER_HOST', '0.0.0.0')
controller_port = os.environ.get('CONTROLLER_PORT', '50000')
controller_client = ControllerClient(host=controller_host, port=controller_port)
best_case = os.environ.get('BEST_CASE', 'True') == 'True' 

def create_cdi(cdi_key, cdi_id):
    cdi_config = Config()
    cdi_config.cdis = {}
    cdi = CDI(cdi_id=cdi_id, cdi_key=cdi_key, cdi_size_bytes=10000000, cdi_access_mode=666, uid=uid, gid=gid)
    cdi_config.cdis[cdi_key] = cdi
    cdi_config.process_id = process_id
    cdi_config.process_name = "scheduler_process"
    cdi_config.destroy_if_no_new_data = False
    cdi_config.create = True
    cdi_config.transfer_mode = 666
    cdi_config.app_id = "0a0a2026-798f-4948-8037-93d87c66ee17"
    cdi_config.app_name = "video_process"
    print("create cdi request ", cdi_config)
    try:
        response = controller_client.CreateCDIs(cdi_config)
        print("create cdi response", response)
        if response.err:
            logging.error(f"Error creating CDI: {response.err}")
        else:
            logging.info("CDI created successfully")
        return cdi_config
    except Exception as e:
        logging.error(f"Exception in create_cdi: {e}", exc_info=True)
        return None

def write_image_to_cdi(cdi_key, cdi_id, image_data):
    response = controller_client.GetCDIsByProcessID(process_id=process_id)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    for id, cdi in cdi_config.cdis.items():
        if id == cdi_id:
            cdi.clear_data()
            data= image_data
            cdi.write_data(data)
    print("updated cdi config for process", cdi_config, process_id)
    return cdi_config

def get_request_cdi_transfer(previous_id,transfer_id):
    response = controller_client.GetCDIsByProcessID(process_id=previous_id)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    cdi_config.transfer_id = transfer_id
    logging.info(f"Requesting CDI transfer to process {transfer_id} from {process_id} and config {cdi_config}")
    try:
        response = controller_client.TransferCDIs(cdi_config)
        if response.err:
            print(f"Error transfering cdi: {response.err}")
        else:
            print("transfer cdi response", response)
    except Exception as e:
        logging.error(f"Exception in create_cdi: {e}", exc_info=True)

def request_cdi_transfer(transfer_id, cdi_key, cdi_config):
    cdi_config.transfer_id = transfer_id
    logging.info(f"Requesting CDI transfer for {cdi_key} to process {transfer_id} from {process_id} and config {cdi_config}")
    try:
        response = controller_client.TransferCDIs(cdi_config)
        if response.err:
            print(f"Error transfering cdi: {response.err}")
        else:
            print("transfer cdi response", response)
    except Exception as e:
        logging.error(f"Exception in create_cdi: {e}", exc_info=True)
        return None

def saveWorkflowOutput(cdi_config, cdi_id):
    temp_folder = '/tmp'
    output_file = os.path.join(temp_folder, f'{cdi_id}.jpeg')
    for id, cdi in cdi_config.cdis.items():
        if id == cdi_id:
            data = cdi.read_data()
            image_data = base64.b64decode(data)
            image = Image.open(BytesIO(image_data))
            image.save(output_file, format="JPEG")
            with open(output_file, 'rb') as file_data:
                s3.put_object(Bucket='orkes-image-data', Key=f'cdi_{cdi_id}.jpeg', Body=file_data)
                print(f'uploaded file cdi_{cdi_id}.jpeg to s3')
            break

def completeWorkflow(pId, cdi_id):
    get_request_cdi_transfer(pId, process_id)
    print('successfully transfered permission to the scheduler')
    response = controller_client.GetCDIsByProcessID(process_id)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    cdi_config = Config()
    cdi_config.from_proto_controller_cdi_configs(response.cdi_configs)
    saveWorkflowOutput(cdi_config, cdi_id)
    response = controller_client.DeleteCDIs(process_id, cdi_config)
    if response.err != "":
        raise Exception(f"extractor: exception while fetching cdi config: {response.err}")
    else:
        print(f'Deleted CDI for the process {pId}')

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



# Function to continuously process tasks from Redis
def process_tasks():
    global last_push_time 
    while True:
        task_object = r.blpop('scheduler_queue', timeout=0)
        key, task_json = task_object
        # Parse the JSON string back into a dictionary
        data = json.loads(task_json)
        
        # Calculate and print time difference if last_push_time is not None
        if last_push_time is not None:
            current_time = time.time()
            time_difference = current_time - last_push_time
            print("processing time is :", time_difference, "event is", data)
            times.append(time_difference)
        
        if "controller" in data:
            print("task",data["controller"])
            wf= data["controller"]
            id= wf["workflow_exe_id"]
            cur = conn.cursor()

            # Get the worker with the least work
            worker_info={}
            wi={}
            cur.execute("SELECT * FROM workers")
            workers_e = cur.fetchall()
            for worker in workers_e:
                wid=worker[0]
                queue=worker[3]
                current_load=worker[5]
                work_load=int(worker[4])-int(worker[5])
                worker_info[wid] = {'queue': queue, 'load': work_load, 'current_load':current_load, 'host': worker[1], 'port': worker[2], 'process_id': worker[6]}
                wi[wid]=work_load
            sorted_dict = dict(sorted(wi.items(), key=lambda item: item[1]))
            sorted_items = list(sorted_dict.items())
            # Get the last item, which is a tuple containing the last key-value pair
            worker_id, _ = sorted_items[-1]
            worker_info[worker_id]['current_load']=worker_info[worker_id]['current_load']+1

            cur.execute("UPDATE workers SET current_pool = %s WHERE worker_id = %s", (worker_info[worker_id]['current_load'], worker_id))
            # Commit the transaction
            conn.commit()

            cur.execute("SELECT * FROM workflow_execution where id=%s",(id,))
            workflows_e = cur.fetchone()
            id=workflows_e[0]
            workflow_id=str(workflows_e[1])
            task_id=str(workflows_e[2])
            request=workflows_e[3]['image']
            print('started executing workflow request', workflow_id)
            
            cur.execute("SELECT * FROM workflow_definition where id=%s",(workflow_id,))
            workflows_d = cur.fetchall()
            for workflow in workflows_d:
                workflow_name=workflow[1]
                tasks_id=workflow[2]
            cur.execute("SELECT * FROM task_definition where id=%s",(task_id,))
            task_d = cur.fetchall()
            for task in task_d:
                task_name=task[1]
            cur.close()
            cdi_id =str(uuid.uuid4())
            cdi_key = random.randint(10000, 99999)
            worker_process_id = worker_info[worker_id]['process_id']
            print(f'creating cdi variable for workflow {workflow_id} on process {process_id}')
            create_cdi(cdi_key, cdi_id)
            cdi_config = write_image_to_cdi(cdi_key, cdi_id, request)
            request_cdi_transfer(worker_process_id, cdi_key, cdi_config)
            last_push_time = time.time()
            work_request = {'workflow_exec_id': id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': cdi_id, 'worker_id':worker_id}
            worker_json = json.dumps(work_request)
            r.rpush(worker_info[worker_id]['queue'],  worker_json)  

        if "worker" in data:
            wf= data["worker"]
            workflow_exec_id= wf["workflow_exec_id"]
            workflow_id=wf["workflow_id"]
            workflow_name=wf["workflow_name"]
            task_id=int(wf["task_id"])
            tasks_id=wf["tasks_id"]
            request=wf["request"]
            worker_id=wf["worker_id"]
            if tasks_id[len(tasks_id)-1]==task_id:
                #This means that all the task is completed
                cur = conn.cursor()
                #doubt what is the predefined status value
                cur.execute("UPDATE workflow_execution SET status = %s WHERE id = %s", ('COMPLETED', workflow_exec_id))
                print(times)
                print(sum(times) / len(times))
                times.clear()
                last_push_time = None
                cur.execute("SELECT * FROM workers where worker_id = %s",( worker_id,))
                worker_ex = cur.fetchone()
                print(f'worker finished the task {worker_ex}')
                completeWorkflow(worker_ex[6],request)
                # Commit the transaction
                conn.commit()
                cur.close()
            else:
                cur = conn.cursor()
                worker_info={}
                wi={}
                cur.execute("SELECT * FROM workers")
                workers_e = cur.fetchall()
                for worker in workers_e:
                    id=worker[0]
                    ip=worker[1]
                    port=worker[2]
                    queue=worker[3]
                    current_load=worker[5]
                    work_load=int(worker[4])-int(worker[5])
                    worker_info[id] = {'queue': queue, 'load': work_load, 'current_load':current_load, 'ip':ip,'port':port, 'process_id': worker[6]}
                    wi[id]=work_load
                
                worker_info[worker_id]['current_load']=worker_info[worker_id]['current_load']-1
                wi[worker_id]=wi[worker_id]+1
                cur.execute("UPDATE workers SET current_pool = %s WHERE worker_id = %s", (worker_info[worker_id]['current_load'], worker_id))
                # Commit the transaction
                conn.commit()
                sorted_dict = dict(sorted(wi.items(), key=lambda item: item[1]))
                sorted_items = list(sorted_dict.items())
                # Get the last item, which is a tuple containing the last key-value pair
                print(sorted_items,best_case)
                if best_case:
                    new_worker_id = worker_id
                else :
                    new_worker_id, _ = sorted_items[-1]
                    if new_worker_id==worker_id:
                        new_worker_id,_=sorted_items[-2]
                    get_request_cdi_transfer(worker_info[worker_id]['process_id'],worker_info[new_worker_id]['process_id'])
                
                index = tasks_id.index(task_id)
                task_id=str(tasks_id[index+1])
                
                print(f'executing task {task_id} on worker {new_worker_id}')
                
                cur.execute("SELECT * FROM task_definition where id=%s",task_id)
                task_d = cur.fetchall()
                for task in task_d:
                    task_name=task[1]
                
                cur.execute("UPDATE workflow_execution SET next_task_id = %s, request = %s, status= 'INPROGRESS' WHERE id = %s", (str(task_id), json.dumps({'cdi_id': request}), workflow_exec_id))
    
                # Commit the transaction
                conn.commit()
                cur.close()
                last_push_time = time.time()
                wdata= {'workflow_exec_id': workflow_exec_id ,'workflow_name': workflow_name, 'task_name': task_name, 'workflow_id': workflow_id, 'task_id': task_id, 'tasks_id': tasks_id, 'request': request, 'worker_id':new_worker_id}
                worker_json = json.dumps(wdata)
                r.rpush(worker_info[new_worker_id]['queue'],  worker_json) 

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("Registering Scheduler with Controller...")

    async_serve_rpc = threading.Thread(target=serve_rpc, args=(rpc_host, rpc_port), kwargs={})
    async_serve_rpc.start()

    register_with_controller(process_id=process_id, name=container_name, namespace=container_namespace,
                             node_ip=node_ip, host=container_ip, port=rpc_port, controller_host=controller_host,
                             controller_port=controller_port, uid=uid, gid=gid)

    logging.info("Successfully registered with Controller!")
    process_tasks()
