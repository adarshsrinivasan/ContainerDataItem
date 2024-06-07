import requests
import base64
import os

# Server URL
SERVER_URL = os.environ.get('WORKFLOW_CONTROLLER_URL', 'http://0.0.0.0:3002/orchestrator/api/v1')  # Update with your server URL

# Helper functions for image processing
def process_image(path_img):
    with open(path_img, 'rb') as f:
        image_data = f.read()
        image_base64 = base64.b64encode(image_data).decode('utf-8') 
        
        return image_base64

# Get image data
image = process_image(os.environ.get('IMAGE', 'test.jpeg'))

# Sample data for workflow and task registration
workflow_data = {
    'workflow_name': 'Image Workflow',
    'task_id': [1,2,3],
    'username': 'user1'
}

task_data = {
    'task_name': 'UNBLUR_IMAGE',
    'username': 'user1'
}

task_data2 = {
    'task_name': 'CROP_IMAGE',
    'username': 'user1'
}

task_data3 = {
    'task_name': 'DETECT_FACE_IMAGE',
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

def start_workflow():
    response = requests.post(f'{SERVER_URL}/start_workflow', json=exec_data, headers=headers)
    print(response.json())

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
        start_workflow()
    elif choice == '6':
        get_exec()
    else:
        print("Invalid choice")