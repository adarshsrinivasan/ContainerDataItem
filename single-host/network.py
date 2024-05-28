import pandas as pd
import matplotlib.pyplot as plt

# Read data for each container
data = pd.read_csv('grpc_out.csv')

# Extract transmitted and received rates for each container
data[['TransmittedRates_MB', 'ReceivedRates_MB']] = data['Network Usage'].str.extract(r'([\d.]+)MB \/ ([\d.]+)MB').astype(float)

# Convert rates from MB/s to KB/s
data['TransmittedRates'] = data['TransmittedRates_MB'] 
data['ReceivedRates'] = data['ReceivedRates_MB']

# Calculate the rate of change in transmitted and received rates for each container
data['TransmittedRates'] = data.groupby('Container')['TransmittedRates'].diff() / 10
data['ReceivedRates'] = data.groupby('Container')['ReceivedRates'].diff() / 10
data['Time'] = data.index * 10  # Assuming the index represents the time in 5-second intervals

# Plot the graph for each container
plt.figure(figsize=(10, 6))

containers = data['Container'].unique()
colors = ['r', 'g', 'b', 'c', 'm', 'y']  # Add more colors if you have more containers

for i, container in enumerate(containers):
    container_data = data[data['Container'] == container]
    if "combiner" in container:
        container = "Combiner"
    elif "extractor" in container:
        container = "Extractor"
    elif "detector" in container:
        container = "Detector"
    plt.plot(container_data['Time'], container_data['TransmittedRates'], marker='.',
             label=f'{container} Transfer Rate', linestyle='--', color=colors[i % len(colors)])
    plt.plot(container_data['Time'], container_data['ReceivedRates'],
             label=f'{container} Receive Rate', linestyle='--', color=colors[i % len(colors)])

plt.title('Network Usage Rate Over Time')
plt.legend(loc='center left', bbox_to_anchor=(0.8, 0.5))
plt.xlabel('Time (seconds)')
plt.ylabel('Rate of Change (MB per second), Multi-host')
# plt.ylim(0, 40000)
plt.xlim(0, 1750)
plt.grid(True)
plt.show()
