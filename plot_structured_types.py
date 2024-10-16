import os
import pandas as pd
import matplotlib.pyplot as plt

# Create the 'plots' folder if it doesn't exist
if not os.path.exists('plots'):
    os.makedirs('plots')

# Read the data from CSV
data = pd.read_csv('benchmark_results.csv')

# Cast N to string with commas for better readability
data['N_str'] = data['N'].apply(lambda x: f'{x:,}')

# Calculate the average of N over the 3 runs
data_avg = data.groupby(['N', 'Operation', 'Table'])['Time'].mean().reset_index()
data_avg = pd.merge(data_avg, data[['N', 'N_str']].drop_duplicates(), on='N')
print(data_avg)

# Loop through each operation except the scan_value group
for operation, group in data_avg.groupby('Operation'):
    if operation not in ['SCAN_VALUE', 'SCAN_VALUE_1', 'SCAN_VALUE_2']:
        plt.figure(figsize=(10, 8))

        # Extract x and y values, and labels
        x_values = group['N']
        y_values = group['Time']
        x_labels = group['N_str']

        # Plot the lines and markers using pyplot
        for table in group['Table'].unique():
            table_data = group[group['Table'] == table]
            plt.plot(table_data['N_str'], table_data['Time'], marker='o', label=table)

        # Format the plot
        plt.title(f"{operation.capitalize()}, Comparison of Map, Array, Object, 500 to 10m Rows")
        plt.xlabel("Rows")
        plt.ylabel("Time (s)")

        # Add a legend
        plt.legend(title='Table')

        # Save the plot with lowercase operation name
        plt.savefig(f'plots/plot_{operation.lower()}_std.png', dpi=300)
        plt.close()

# Now handle the special case for SCAN_VALUE, SCAN_VALUE_1, and SCAN_VALUE_2
scan_value_data = data_avg[data_avg['Operation'].isin(['SCAN_VALUE', 'SCAN_VALUE_1', 'SCAN_VALUE_2'])]
print(scan_value_data)

# Rename the tables for the legend
scan_value_data['Table'] = scan_value_data['Operation'].replace({
    'SCAN_VALUE': 'array_table',
    'SCAN_VALUE_1': 'map_table_scan_1',
    'SCAN_VALUE_2': 'map_table_scan_2'
})

# Create the combined plot for SCAN_VALUE group
plt.figure(figsize=(8, 6))

# Plot the lines and markers using pyplot
for table in scan_value_data['Table'].unique():
    table_data = scan_value_data[scan_value_data['Table'] == table]
    print(table_data)
    plt.plot(table_data['N_str'], table_data['Time'], marker='o', label=table)

# Format the plot
plt.title(f"Scan, Comparison of Map, Array, Object, 500 to 10m Rows")
plt.xlabel("Rows")
plt.ylabel("Time (s)")

# Add a legend
plt.legend(title='Table')

# Save the plot
plt.savefig('plots/plot_scan_value_std.png', dpi=300)
plt.close()
