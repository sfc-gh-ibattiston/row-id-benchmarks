import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Load data from CSV
df = pd.read_csv('benchmark_results_row_id_final.csv')

# Cast N to string with commas for better readability
df['N_str'] = df['N'].apply(lambda x: f'{x:,}')

# Group by relevant columns and calculate average time
df_avg = df.groupby(['N', 'Operation', 'Table'])['Time'].mean().reset_index()

# Merge formatted N string into the grouped data
df_avg = pd.merge(df_avg, df[['N', 'N_str']].drop_duplicates(), on='N')

# Rename operations for better clarity
label_map = {
    'QUERY': 'Original Query Computation',
    'INSERT': 'Original Query Computation and Storage',
    'ROW_ID': 'Row ID Computation',
    'WRITE_ROW_ID': 'Row ID Computation and Storage',
    'REFRESH': 'Dynamic Table Initial Refresh'
}
df_avg['Operation'] = df_avg['Operation'].map(label_map)

# Unique tables and operations
tables = df_avg['Table'].unique()
operations = df_avg['Operation'].unique()

# Create plots directory if it doesn't exist
if not os.path.exists('plots'):
    os.makedirs('plots')

# Plot for each table
for table in tables:
    plt.figure(figsize=(10, 8))  # Increase height to make the plot taller

    # Filter data for the current table
    df_table = df_avg[df_avg['Table'] == table]

    # Calculate the maximum Y value across all operations for this table
    max_y = df_table['Time'].max()

    # Plot each operation for the current table
    for operation in operations:
        # Filter data for the current table and operation
        df_filtered = df_table[df_table['Operation'] == operation]

        # Plot line using the N_str (string representation of N)
        plt.plot(df_filtered['N_str'], df_filtered['Time'], marker='o', label=operation)

    # Set title and labels
    plt.title(f'Runtime of {table} Row ID Computation, 500 to 10m Rows', fontsize=14)
    plt.xlabel('Number of Tuples (N)', fontsize=12)
    plt.ylabel('Average Time (seconds)', fontsize=12)

    # Set Y-axis ticks every 10 units, based on the overall max for this table
    plt.yticks(range(0, int(max_y) + 10, 10))  # Ticks every 10 units

    plt.legend(title='Operation')

    # Save the plot to a file with higher resolution
    plt.savefig(f'plots/{table.lower()}_performance.png', dpi=300)
    plt.close()

# Create a single plot for Row ID Computation
plt.figure(figsize=(10, 8))

# Filter data for Row ID Computation for all tables
df_row_id = df_avg[df_avg['Operation'] == 'Row ID Computation']

# Plot each table's Row ID Computation
for table in df_row_id['Table'].unique():
    df_filtered = df_row_id[df_row_id['Table'] == table]
    plt.plot(df_filtered['N_str'], df_filtered['Time'], marker='o', label=table)

# Set title and labels
plt.title('Row ID Computation, 500 to 10m Rows', fontsize=14)
plt.xlabel('Number of Tuples (N)', fontsize=12)
plt.ylabel('Average Time (seconds)', fontsize=12)

# Adjust y-axis ticks
plt.yticks(range(0, int(df_row_id['Time'].max()) + 10, 10))

plt.legend(title='Table')

# Save the plot to a file with higher resolution
plt.savefig('plots/row_id_computation_performance.png', dpi=300)
plt.close()

# Create a single plot for Row ID Computation and Storage
plt.figure(figsize=(10, 8))

# Filter data for Row ID Computation and Storage for all tables
df_row_id_storage = df_avg[df_avg['Operation'] == 'Row ID Computation and Storage']

# Plot each table's Row ID Computation and Storage
for table in df_row_id_storage['Table'].unique():
    df_filtered = df_row_id_storage[df_row_id_storage['Table'] == table]
    plt.plot(df_filtered['N_str'], df_filtered['Time'], marker='o', label=table)

# Set title and labels
plt.title('Row ID Computation and Storage, 500 to 10m Rows', fontsize=14)
plt.xlabel('Number of Tuples (N)', fontsize=12)
plt.ylabel('Average Time (seconds)', fontsize=12)

# Adjust y-axis ticks
plt.yticks(range(0, int(df_row_id_storage['Time'].max()) + 10, 10))

plt.legend(title='Table')

# Save the plot to a file with higher resolution
plt.savefig('plots/row_id_computation_and_storage_performance.png', dpi=300)
plt.close()
