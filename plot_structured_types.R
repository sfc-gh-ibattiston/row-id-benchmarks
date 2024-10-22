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

# Create the 'plots' folder if it doesn't exist
if (!dir.exists('plots')) {
  dir.create('plots')
}

# Read the data from CSV
data <- read_csv('benchmark_results.csv')

data <- data %>%
  mutate(Time = as.numeric(Time))

# Calculate the average of N over the 3 runs
data_avg <- data %>%
  group_by(N, Operation, Table) %>%
  summarise(Time = mean(Time), .groups = 'drop')

# Loop through each operation except the SCAN_VALUE group
unique_operations <- unique(data_avg$Operation)

print(data_avg)

for (operation in unique_operations) {
  if (!operation %in% c('SCAN_VALUE', 'SCAN_VALUE_1', 'SCAN_VALUE_2')) {
    # Filter data for the current operation
    group <- data_avg %>% filter(Operation == operation)

    # Create a ggplot for this operation
    p <- ggplot(group, aes(x = N, y = Time, color = Table, group = Table)) +
      geom_line(size = 1) +
      geom_point(size = 2) +
      labs(
        title = paste0(operation, ", Comparison of Map, Array, Object, 500 to 10M Rows"),
        x = "Rows", y = "Time (s)"
      ) +
      theme_bw() +
      theme(
        axis.text.x = element_text(angle = 45, hjust = 1),
        legend.title = element_text(size = 10)
      )

    # Save the plot as PNG file
    ggsave(filename = paste0("plots/plot_", tolower(operation), "_std.png"), plot = p, dpi = 300)
  }
}

# Now handle the special case for SCAN_VALUE, SCAN_VALUE_1, and SCAN_VALUE_2
scan_value_data <- data_avg %>%
  filter(Operation %in% c('SCAN_VALUE', 'SCAN_VALUE_1', 'SCAN_VALUE_2'))

# Rename the tables for the legend
scan_value_data <- scan_value_data %>%
  mutate(Table = recode(Operation,
                        'SCAN_VALUE' = 'array_table',
                        'SCAN_VALUE_1' = 'map_table_scan_1',
                        'SCAN_VALUE_2' = 'map_table_scan_2'))

# Create the combined plot for SCAN_VALUE group
p_scan <- ggplot(scan_value_data, aes(x = N, y = Time, color = Table, group = Table)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  labs(
    title = "Scan, Comparison of Map, Array, Object, 500 to 10M Rows",
    x = "Rows", y = "Time (s)"
  ) +
  theme_bw() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.title = element_text(size = 10)
  )

# Save the combined scan plot
ggsave(filename = 'plots/plot_scan_value_std.png', plot = p_scan, dpi = 300)
