import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Function to ensure the plots directory exists
def create_plots_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Load the CSV data into a DataFrame
def load_data(file_path):
    return pd.read_csv(file_path)

# Filter the data and calculate averages
def process_data(df):
    # Filter out 'flatten' queries and keep only 'initial_refresh' operations
    filtered_df = df[(df['Operation'] == 'initial_refresh')]

    # Convert Size to numeric, forcing errors to NaN
    filtered_df['Size'] = pd.to_numeric(filtered_df['Size'], errors='coerce')

    # Drop rows with NaN values in Size after conversion
    filtered_df = filtered_df.dropna(subset=['Size'])

    # Calculate the average size grouped by Table, Compression, and Query
    average_sizes = filtered_df.groupby(['Table', 'Compression', 'Query', 'N']).agg({'Size': 'mean'}).reset_index()

    # Convert Size to kilobytes
    average_sizes['Size'] = average_sizes['Size'] / 1000  # Convert bytes to kilobytes

    return average_sizes

# Plot the data
def plot_data(df, output_directory):
    # Create plots for each query
    for query in df['Query'].unique():
        query_data = df[df['Query'] == query]

        # Separate compressed and decompressed data
        compressed_data = query_data[query_data['Compression'] == 'COMPRESSED']
        decompressed_data = query_data[query_data['Compression'] == 'DECOMPRESSED']

        # Create a subplot with 2 columns (one for compressed, one for decompressed)
        fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=True)

        # Plot compressed data
        ax_compressed = axes[0]
        for table, group_data in compressed_data.groupby('Table'):
            x_values = group_data['N'].astype(str)
            y_values = group_data['Size']
            label = f"{table}_compressed"
            ax_compressed.plot(x_values, y_values, marker='o', label=label)

        ax_compressed.set_xlabel('Rows (as string)')
        ax_compressed.set_ylabel('Average Size (KB)')
        ax_compressed.set_title(f'Compressed: {query}')
        ax_compressed.legend(title='Table', loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=2)

        # Plot decompressed data
        ax_decompressed = axes[1]
        for table, group_data in decompressed_data.groupby('Table'):
            x_values = group_data['N'].astype(str)
            y_values = group_data['Size']
            label = f"{table}_decompressed"
            ax_decompressed.plot(x_values, y_values, marker='o', label=label)

        ax_decompressed.set_xlabel('Rows (as string)')
        ax_decompressed.set_ylabel('Average Size (KB)')  # Ensure Y-axis label is also on the second plot
        ax_decompressed.set_title(f'Decompressed: {query}')
        ax_decompressed.legend(title='Table', loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=2)

        # Ensure both plots have Y-axis labels
        ax_decompressed.tick_params(axis='y', labelright=True)  # Enable Y-axis ticks and labels on the right plot

        # Adjust layout to reduce white space and avoid overlap
        plt.tight_layout(rect=[0, 0, 1, 1])
        plt.subplots_adjust(bottom=0.2)  # Adjust the bottom to make room for the legend

        # Save the plot
        plt.savefig(os.path.join(output_directory, f'{query}_compressed_vs_decompressed.png'))
        plt.close()

def main():
    # Path to the CSV file
    csv_file_path = 'benchmark_results_row_id_size.csv'  # Replace with your CSV file path
    output_directory = 'plots'

    # Create the output directory if it doesn't exist
    create_plots_directory(output_directory)

    # Load and process the data
    df = load_data(csv_file_path)
    df = process_data(df)

    # Plot the data
    plot_data(df, output_directory)

if __name__ == "__main__":
    main()
