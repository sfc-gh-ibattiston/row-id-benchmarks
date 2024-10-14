import pandas as pd
import matplotlib.pyplot as plt
import os

# Load the data
data = pd.read_csv('benchmark_results_row_id_size.csv')

# Filter out "update" operation
filtered_data = data[data['Operation'].isin(['initial_refresh', 'insert', 'insert_2'])]

# Average the runs
averaged_data = filtered_data.groupby(['Compression', 'Table', 'Operation', 'N']).agg({'Size': 'mean'}).reset_index()

# Create a directory for plots
plots_dir = 'plots'
os.makedirs(plots_dir, exist_ok=True)

# Define custom labels for the legend and table names
def format_legend_label(operation):
    labels = {
        'initial_refresh': 'Initial Refresh',
        'insert': 'Insert (1)',
        'insert_2': 'Insert (2)'
    }
    return labels.get(operation, operation.capitalize().replace('_', ' '))

def format_table_name(table_name):
    parts = table_name.split('_')
    if parts[0] == 'dynamic' and parts[1] == 'table':
        return f'DT - {" ".join(part.capitalize() for part in parts[2:])}'
    elif len(parts) > 1:
        return f'{parts[0].capitalize()} - {" ".join(part.capitalize() for part in parts[1:])}'
    return table_name.capitalize()


# Plot for each table
tables = averaged_data['Table'].unique()
operations = averaged_data['Operation'].unique()

for table in tables:
    pretty_table_name = format_table_name(table)

    plt.figure(figsize=(12, 8))

    for operation in operations:
        for compression in ['COMPRESSED', 'DECOMPRESSED']:
            subset = averaged_data[(averaged_data['Table'] == table) &
                                   (averaged_data['Operation'] == operation) &
                                   (averaged_data['Compression'] == compression)]
            plt.plot(subset['N'], subset['Size'], marker='o', label=f'{format_legend_label(operation)} {compression.capitalize()}')

    plt.title(f'{pretty_table_name} - Compressed vs Decompressed')
    plt.xlabel('Row Size (N)')
    plt.ylabel('Size')
    plt.legend()
    plt.grid(True)

    # Format y-axis with thousands separator
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))

    # Save the plot
    plot_filename = f'{plots_dir}/{table}_compressed_vs_decompressed.png'
    plt.savefig(plot_filename)
    plt.close()  # Close the plot to free up memory

# Bar plot for the largest N, one figure with 5 subplots
largest_n = averaged_data['N'].max()
largest_n_data = averaged_data[(averaged_data['N'] == largest_n) & (averaged_data['Operation'] == 'initial_refresh')]

tables = largest_n_data['Table'].unique()

# Create a figure with 5 subplots (1 row, 5 columns)
fig, axes = plt.subplots(1, 5, figsize=(14, 6), sharey=True)
fig.suptitle(f'{largest_n} Rows - Initial Refresh: Compressed vs Decompressed Row ID', fontsize=20)

for i, table in enumerate(tables):
    pretty_table_name = format_table_name(table)
    subset = largest_n_data[largest_n_data['Table'] == table]

    # Create bar plot for each table in a separate subplot
    axes[i].bar(subset['Compression'], subset['Size'], color=['blue', 'orange'], width=0.7)
    axes[i].set_title(f'{pretty_table_name}', fontsize=14)
    # axes[i].set_xlabel('Compression')

    # Format y-axis with thousands separator
    axes[i].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
    axes[i].tick_params(axis='both', which='major', labelsize=10)

# Set common y-axis label
fig.text(0.01, 0.5, 'Size (bytes)', va='center', rotation='vertical', fontsize=14)

# Adjust layout
plt.tight_layout(rect=[0.02, 0, 1, 1])

# Save the combined figure
combined_plot_filename = f'{plots_dir}/largest_N_initial_refresh_combined.png'
plt.savefig(combined_plot_filename)
plt.close()
