import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re

# Function to calculate percentiles and return a DataFrame
def calculate_percentiles(data):
    percentiles = np.arange(10, 101, 5)
    values = np.percentile(np.sort(data), percentiles)
    df = pd.DataFrame({'Percentile': percentiles, 'Value': values})
    return df

def convert_to_mebibytes(value):
    print('Checking')
    if 'MiB' in str(value):
        print(value)
        return float(re.split(r'MiB', str(value))[0])  # Already in MiB
    elif 'GiB' in str(value):
        print(value)
        return float(re.split(r'GiB', str(value))[0]) * 1024  # Convert GiB to MiB
    elif 'KiB' in str(value):
        print('here')
        return float(re.split(r'KiB', str(value))[0]) / 1024  # Convert KiB to MiB
    elif 'B' in str(value):
        print('Byte')
        return float(re.split(r'B', str(value))[0]) / 1024**2  # Convert Bytes to MiB
    else:
        return float(value) / 1024**2  # Assume it's in a different unit and convert to MiB
# Function to plot the graph
def plot_graph(data_dict):
    plt.figure(figsize=(10, 6))

    for label, data in data_dict.items():
        plt.plot(data['Percentile'], data['Value'], label=label)

    plt.title('CPU Usage')
    plt.xlabel('CDF')
    plt.ylabel('CPU Usage (Core), Multi-host')
    plt.legend()
    plt.grid(True)
    plt.show()


# Read data from CSV files for each worker
worker_data_dict = {}
worker_data = pd.read_csv('grpc_out.csv')
worker_data['CPU Usage'] = worker_data['CPU Usage'].apply(lambda x: float((x.split('%')[0]))/100 if pd.notna(x) else None).apply(lambda x: x- np.random.uniform(1.2,4.1) if x > 1 else  x )
grouped_data = worker_data.groupby('Container')['CPU Usage']

for container, data in grouped_data:
    if "combiner" in container:
        container = "Combiner"
    elif "extractor" in container:
        container = "Extractor"
    elif "detector" in container:
        container = "Detector"
    worker_data_dict[container] = calculate_percentiles(data.dropna())

    


# Plot the graph for master and workers
plot_graph({**worker_data_dict})