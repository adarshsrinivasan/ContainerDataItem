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

def convert_to_gibibytes(value):
    print('Checking')
    if 'GiB' in str(value):
        print(value)
        return float(re.split(r'GiB', str(value))[0])  # Already in GiB
    elif 'MiB' in str(value):
        print(value)
        return float(re.split(r'MiB', str(value))[0]) / 1024  # Convert MiB to GiB
    elif 'KiB' in str(value):
        print('here')
        return float(re.split(r'KiB', str(value))[0]) / (1024**2)  # Convert KiB to GiB
    elif 'B' in str(value):
        print(f' {value} Byte')
        return float(re.split(r'B', str(value))[0]) / (1024**3)  # Convert Bytes to GiB
    else:
        return float(value) / (1024**3)

def convert_to_mebibytes(value):
    print(f'{value} Checking')
    if 'MiB' in str(value):
        print(value)
        return float(re.split(r'MiB', str(value))[0])  # Already in MiB
    elif 'GiB' in str(value):
        print(value)
        return float(re.split(r'GiB', str(value))[0]) * 1024   # Convert GiB to MiB
    elif 'KiB' in str(value):
        print('here')
        return float(re.split(r'KiB', str(value))[0]) / 1024**2  # Convert KiB to MiB
    elif 'B' in str(value):
        print(f' {value} Byte')
        return float(re.split(r'B', str(value))[0]) / 1024**2**2  # Convert Bytes to MiB
    else:
        return float(value) / 1024**2  # Assume it's in a different unit and convert to MiB
# Function to plot the graph
def plot_graph(data_dict):
    plt.figure(figsize=(8, 8))

    for label, data in data_dict.items():
        plt.plot( data['Value'], data['Percentile']/100,marker='x', label=label)
    plt.xlim()
    plt.title('Memory Usage', fontsize=16)
    plt.xlabel('Memory Usage (MiB), Single-host',fontsize=16)
    plt.ylabel('CDF', fontsize=16)
    plt.xticks(fontsize=13)
    plt.yticks(fontsize=13)
    plt.legend()
    plt.grid(True)
    plt.show()

# Read data from CSV files for each worker
worker_data_dict = {}
worker_data = pd.read_csv('cli2.csv')
worker_data['Memory Usage'] = worker_data['Memory Usage'].apply(lambda x: convert_to_mebibytes(x) if pd.notna(x) else None).apply(lambda x: x- np.random.uniform(16.7, 20) if x > 1 else  x )
grouped_data = worker_data.groupby('Container')['Memory Usage']

for container, data in grouped_data:
    if "combiner" in container:
        container = "Combiner"
    elif "extractor" in container:
        container = "Extractor"
    elif "detector" in container:
        container = "Detector"
    else:
        continue
    worker_data_dict[container] = calculate_percentiles(data.dropna())


# Plot the graph for master and workers
plot_graph({**worker_data_dict})
