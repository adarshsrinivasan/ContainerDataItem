import csv

# Total memory limit in GiB
total_memory_gib = 62.74

# Convert GiB to MiB
total_memory_mib = total_memory_gib * 1024

# Function to convert memory usage percentage to actual memory usage in MiB
def convert_memory_usage(memory_usage_str):
    memory_percentage = float(memory_usage_str.strip('%')) / 100
    return total_memory_mib * memory_percentage

# Open input and output CSV files
input_file = 'multihost_grpc.csv'
output_file = 'multihost_grpc_out.csv'

with open(input_file, 'r') as csv_in, open(output_file, 'w', newline='') as csv_out:
    reader = csv.reader(csv_in)
    writer = csv.writer(csv_out)

    # Write header
    writer.writerow(next(reader))

    # Process rows
    for row in reader:
        # Convert memory usage percentage to actual memory usage in MiB
        memory_usage_mib = convert_memory_usage(row[2])
        # Update the row with the converted memory usage
        row[2] = '{:.2f}MiB'.format(memory_usage_mib)
        # Write the updated row to the output CSV file
        writer.writerow(row)

print("Conversion completed. Output saved to", output_file)
