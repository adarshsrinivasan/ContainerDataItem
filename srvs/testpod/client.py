import requests
import base64
import os
import boto3
import concurrent.futures

# Server URL
SERVER_URL = os.environ.get('WORKFLOW_CONTROLLER_URL', 'http://0.0.0.0:3002/orchestrator/api/v1')  # Update with your server URL

# Helper functions for image processing
def process_image(path_img):
    with open(path_img, 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8') 
        
        return image_base64

# Get image data
image = 'https://orkes-image-input-data.s3.ap-south-1.amazonaws.com/test.jpeg'

s3_bucket_name = 'orkes-image-input-data'
s3_region = 'ap-south-1'

s3_client = boto3.client('s3',
                        aws_access_key_id='AKIAUTA4UV6WXMYCSA74',
                        aws_secret_access_key='tKYwykhnoPcKM1HvTOqdRkjSxqKSlowWUIJlZm0w',
                        region_name='ap-south-1')

# Sample data for workflow and task registration
workflow_data = {
    'workflow_name': 'Image Processing Workflow',
    'task_id': [1,2,3],
    'username': 'user1'
}

task_data = {
    'task_name': 'BLUR_IMAGE',
    'username': 'user1'
}

task_data2 = {
    'task_name': 'CROP_IMAGE',
    'username': 'user1'
}

task_data3 = {
    'task_name': 'FLIP_IMAGE',
    'username': 'user1'
}

exec_data = {
    'workflow_id': 1,
    'request': {
        'image' : image
    }
}

headers = {
    "Host": "cdi"
}

# Function to register a workflow
def register_workflow():
    response = requests.post(f'{SERVER_URL}/register_workflow', json=workflow_data, headers=headers)
    print(response.json())

# Function to get all workflows
def get_workflows():
    response = requests.get(f'{SERVER_URL}/workflows', headers=headers)
    print(response.json())

# Function to register a task
def register_task():
    response = requests.post(f'{SERVER_URL}/register_task', json=task_data, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/register_task', json=task_data2, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/register_task', json=task_data3, headers=headers)
    print(response.json())

# Function to get all tasks
def get_tasks():
    response = requests.get(f'{SERVER_URL}/tasks', headers=headers)
    print(response.json())

def start_workflow(image):
    data = {
        'workflow_id': 2,
        'request': {
            'image': image
        }
    }
    response = requests.post(f'{SERVER_URL}/start_workflow', json=data, headers=headers)
    print(f'started workflow with image {image}',response.json())

def get_exec():
    response = requests.get(f'{SERVER_URL}/get_exec', headers=headers)
    print(response.json())

def register():
    response = requests.post(f'{SERVER_URL}/orchestrator/register_task', json=task_data, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/orchestrator/register_task', json=task_data2, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/orchestrator/register_task', json=task_data3, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/orchestrator/register_workflow', json=workflow_data, headers=headers)
    print(response.json())
    response = requests.post(f'{SERVER_URL}/orchestrator/start_workflow', json=exec_data, headers=headers)
    print(response.json())

def list_images_in_s3(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [f"https://{bucket_name}.s3.{s3_region}.amazonaws.com/{obj['Key']}" for obj in response['Contents']]
        else:
            return []
    except Exception as e:
        print(f"Error listing objects in S3 bucket: {e}")
        return []

def start_workflows_in_parallel(images):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(start_workflow, image_url) for image_url in images]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error starting workflow: {e}")

if __name__ == '__main__':
    # Ask the user which endpoint to hit
    print("Choose an endpoint to hit:")
    print("1. Register Workflow")
    print("2. Get Workflows")
    print("3. Register Task")
    print("4. Get Tasks")
    print("5. Start Workflow")
    print("6. Get executions")
    
    choice = input("Enter your choice: ")

    if choice == '1':
        register_workflow()
    elif choice == '2':
        get_workflows()
    elif choice == '3':
        register_task()
    elif choice == '4':
        get_tasks()
    elif choice == '5':
        images = list_images_in_s3(s3_bucket_name)
        print(images)
        new_images = []
        for i in range(0, 1):
            new_images.append(images[0])
        start_workflows_in_parallel(new_images)
    elif choice == '6':
        get_exec()
    else:
        print("Invalid choice")
