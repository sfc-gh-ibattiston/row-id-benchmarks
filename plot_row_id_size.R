# Load required libraries
library(ggplot2)
library(dplyr)
library(readr)
library(scales)  # For formatting axis labels

# Function to ensure the plots directory exists
create_plots_directory <- function(directory) {
  if (!dir.exists(directory)) {
    dir.create(directory)
  }
}

# Load the CSV data into a DataFrame
load_data <- function(file_path) {
  read_csv(file_path)
}

# Calculate averages and process the data
process_data <- function(df) {
  # Convert Size to numeric
  df <- df %>%
    mutate(Size = as.numeric(Size))

  # Replace the table names with new labels
  df <- df %>%
    mutate(Table = case_when(
      Table == 'array_table' ~ 'ARRAY',
      Table == 'dynamic_table' ~ 'DYNAMIC TABLE',
      Table == 'object_table' ~ 'OBJECT',
      Table == 'map_table' ~ 'MAP',
      TRUE ~ Table  # Keep any other table names unchanged
    )) %>%
    # Create a new column for modified query names
    mutate(Query = case_when(
      Query == 'scan' ~ 'SEQUENTIAL SCAN (SELECT *)',
      Query == 'flatten' ~ 'LATERAL FLATTEN',
      Query == 'group_by' ~ 'GROUP BY (1 KEY, INTEGER)',
      Query == 'group_by_strings' ~ 'GROUP BY (5 KEYS, STRING)',
      Query == 'join' ~ 'INNER JOIN',
      TRUE ~ Query  # Keep any other query names unchanged
    ))

  # Calculate the average size grouped by Table, Compression, Query, and N
  average_sizes <- df %>%
    group_by(Table, Compression, Storage, Query, N) %>%
    summarise(Size = mean(Size, na.rm = TRUE)) %>%
    ungroup()

  # Convert Size to kilobytes
  average_sizes <- average_sizes %>%
    mutate(Size = Size / 1000)  # Convert bytes to kilobytes

  return(average_sizes)
}

# Plot the data
plot_data <- function(df, output_directory) {
  unique_queries <- unique(df$Query)
  storage_types <- unique(df$Storage)

    for (storage in storage_types) {
      # Filter data by compression type
      compression_data <- df %>% filter(Storage == storage)

      for (query in unique_queries) {
        query_data <- compression_data %>% filter(Query == query)

        # Separate compressed and decompressed data
        compressed_data <- query_data %>% filter(Compression == 'COMPRESSED')
        decompressed_data <- query_data %>% filter(Compression == 'DECOMPRESSED')

        # Create compressed plot
        p_compressed <- ggplot(compressed_data, aes(x = as.factor(N), y = Size, color = Table)) +
          geom_line(aes(group = Table), size = 1) +
          geom_point(size = 2) +
          labs(x = "Rows", y = "Average Size (KB)", title = paste0("Compressed: ", query, ", ", storage)) +
          scale_y_continuous(labels = comma) +  # Format y-axis with thousands separator
          theme_bw() +  # White background theme
          theme(legend.position = "bottom", legend.title = element_text(size = 10))

        # Create decompressed plot
        p_decompressed <- ggplot(decompressed_data, aes(x = as.factor(N), y = Size, color = Table)) +
          geom_line(aes(group = Table), size = 1) +
          geom_point(size = 2) +
          labs(x = "Rows", y = "Average Size (KB)", title = paste0("Decompressed: ", query, ", ", storage)) +
          scale_y_continuous(labels = comma) +  # Format y-axis with thousands separator
          theme_bw() +  # White background theme
          theme(legend.position = "bottom", legend.title = element_text(size = 10))

        # Arrange both plots side by side
        combined_plot <- cowplot::plot_grid(p_compressed, p_decompressed, ncol = 2)

        # Save the plot with compression type in the filename
        ggsave(filename = file.path(output_directory, paste0(query, " - ", storage, " - COMPRESSED VS DECOMPRESSED.png")),
               plot = combined_plot, width = 16, height = 6)
     }
  }
}

main <- function() {
  # Path to the CSV file
  csv_file_path <- 'benchmark_results_row_id_size.csv'  # Replace with your CSV file path
  output_directory <- 'plots'

  # Create the output directory if it doesn't exist
  create_plots_directory(output_directory)

  # Load and process the data
  df <- load_data(csv_file_path)
  df <- process_data(df)

  # Plot the data
  plot_data(df, output_directory)
}

# Run the main function
main()
