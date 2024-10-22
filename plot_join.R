# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org/"))

# Install required packages if they are not installed
packages <- c("dplyr", "ggplot2", "readr", "scales")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if (length(new_packages)) {
  install.packages(new_packages)
}

# Load required libraries
library(dplyr)
library(ggplot2)
library(readr)
library(scales)

# Load data from CSV
df <- read_csv('benchmark_results_row_id_join.csv')

df <- df %>%
  mutate(Time = as.numeric(Time))

# Average time between runs for each combination of N, Operation, and Table
df_avg <- df %>%
  group_by(N, Operation, Table) %>%
  summarise(Average_Time = mean(Time), .groups = 'drop') %>%
  mutate(N_str = format(N, big.mark = ","))  # Add formatted N after averaging

# Create plots directory if it doesn't exist
if (!dir.exists('plots')) {
  dir.create('plots')
}

# Unique operations
operations <- unique(df_avg$Operation)

# For each operation, generate a plot
for (operation in operations) {
  # Filter data for the current operation
  plot_data <- df_avg %>% filter(Operation == operation)
  print(plot_data)

  # Remove underscores from operation for title and file name
  operation_title <- gsub("_", " ", operation)
  file_name <- gsub("_", " ", operation)

  # Create the plot with title including "ROW ID VS STD"
  p <- ggplot(plot_data, aes(x = N, y = Average_Time, color = Table)) +
    geom_line(size = 1) +  # Line for each table
    geom_point(size = 2) +  # Points for each table
    scale_x_continuous(labels = comma) +  # Format x-axis labels with commas
    labs(title = paste0(operation_title, " - ROW ID VS STRUCTURED DATA TYPES"),
         x = 'Number of Tuples (N)',
         y = 'Average Time (seconds)') +
    theme_bw() +  # White background
    theme(legend.title = element_text(size = 10))

  # Save the plot as an image file in the 'plots' directory
  ggsave(filename = file.path('plots', paste0(file_name, ' ROW ID VS SDT.png')), plot = p, dpi = 300)
}
