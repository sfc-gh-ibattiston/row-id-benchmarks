# Load necessary libraries
library(dplyr)
library(ggplot2)

# Read in the datasets
row_id_data <- read.csv('benchmark_results_row_id.csv')
results_data <- read.csv('benchmark_results.csv')

# Function to create plots
create_plot <- function(df, x_column, y_column, title, filename) {
  p <- ggplot(df, aes_string(x = x_column, y = "avg_time", color = "Table")) +
    geom_line() +
    geom_point() +
    labs(title = title, x = "N", y = "Average Time (s)") +
    theme_bw()

  # Save the plot
  ggsave(filename, plot = p, dpi = 300)
}

# Create the 'plots' folder if it doesn't exist
if (!dir.exists('plots')) {
  dir.create('plots')
}

row_id_data <- row_id_data %>%
  mutate(Time = as.numeric(Time))
results_data <- results_data %>%
  mutate(Time = as.numeric(Time))

# 1. Join SCAN_KEY_VALUE (results) and READ_ROW_ID_DT (row_id)

# Filter data for SCAN_KEY and READ_ROW_ID_DT
scan_key_value_data <- results_data %>%
  filter(Operation == "SCAN_KEY_VALUE")

read_row_id_dt_data <- row_id_data %>%
  filter(Operation == "READ_ROW_ID_DT")

# Merge data on N and Table
scan_read_merged <- rbind(scan_key_value_data, read_row_id_dt_data)

print(scan_read_merged)

# Calculate average time for each N and Table
scan_read_avg <- scan_read_merged %>%
  group_by(N, Table) %>%
  summarise(avg_time = mean(Time))

# Plot SCAN_KEY_VALUE vs READ_ROW_ID_DT
create_plot(scan_read_avg, "N", "avg_time", "Row ID Scan", "plots/ROW ID SCAN - COMBINED.png")


# 2. Join ROW_ID (row_id) and CONSTRUCT (results)

# Filter data for ROW_ID and CONSTRUCT
row_id_data_filtered <- row_id_data %>%
  filter(Operation == "ROW_ID")

construct_data <- results_data %>%
  filter(Operation == "CONSTRUCT")

# Merge data on N and Table
row_id_construct_merged <- rbind(row_id_data_filtered, construct_data)

# Calculate average time for each N and Table
row_id_construct_avg <- row_id_construct_merged %>%
  group_by(N, Table) %>%
  summarise(avg_time = mean(Time))

# Plot ROW_ID vs CONSTRUCT
create_plot(row_id_construct_avg, "N", "avg_time", "Construction of row ID VS construction of SDT", "plots/ROW ID CONSTRUCT - COMBINED.png")


# 3. Join INSERT (results) and WRITE_ROW_ID (row_id)

# Filter data for INSERT and WRITE_ROW_ID
insert_data <- results_data %>%
  filter(Operation == "INSERT")

write_row_id_data <- row_id_data %>%
  filter(Operation == "WRITE_ROW_ID")

# Merge data on N and Table
insert_write_merged <- rbind(insert_data, write_row_id_data)

# Calculate average time for each N and Table
insert_write_avg <- insert_write_merged %>%
  group_by(N, Table) %>%
  summarise(avg_time = mean(Time))

# Plot INSERT vs WRITE_ROW_ID
create_plot(insert_write_avg, "N", "avg_time", "CONSTRUCTION + INSERT of row ID VS SDT", "plots/INSERT WRITE - COMBINED.png")
