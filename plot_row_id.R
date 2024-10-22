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
df <- read_csv('benchmark_results_row_id_final.csv')

# Cast N to string with commas for better readability
df <- df %>%
  mutate(N_str = format(N, big.mark = ","))

df <- df %>%
  mutate(Time = as.numeric(Time))

# Group by relevant columns and calculate average time
df_avg <- df %>%
  group_by(N, Operation, Table) %>%
  summarise(Time = mean(Time), .groups = 'drop') %>%
  left_join(df %>% select(N, N_str) %>% distinct(), by = "N")

# Rename operations for better clarity
label_map <- c(
  'QUERY' = 'Original Query Computation',
  'INSERT' = 'Original Query Computation and Storage',
  'ROW_ID' = 'Row ID Computation',
  'WRITE_ROW_ID' = 'Row ID Computation and Storage',
  'REFRESH' = 'Dynamic Table Initial Refresh'
)
df_avg$Operation <- recode(df_avg$Operation, !!!label_map)

# Unique tables and operations
tables <- unique(df_avg$Table)
operations <- unique(df_avg$Operation)

# Create plots directory if it doesn't exist
if (!dir.exists('plots')) {
  dir.create('plots')
}

# Plot for each table
for (table in tables) {
  plot_data <- df_avg %>% filter(Table == table)

  # Create plot
  p <- ggplot(plot_data, aes(x = N_str, y = Time, color = Operation)) +
    geom_line(size = 1) +  # Line for each operation
    geom_point(size = 2) +  # Points for each operation
    labs(title = paste('Runtime of', table, 'Row ID Computation, 500 to 10m Rows'),
         x = 'Number of Tuples (N)', y = 'Average Time (seconds)') +
    scale_y_continuous(breaks = seq(0, ceiling(max(plot_data$Time, na.rm = TRUE)), by = 10)) +
    theme_bw() +  # White background theme
    theme(legend.title = element_text(size = 10))

  # Save the plot to a file with higher resolution
  ggsave(filename = file.path('plots', paste0(tolower(table), '_performance.png')), plot = p, dpi = 300)
}

# Create a single plot for Row ID Computation
p_row_id <- ggplot(df_avg %>% filter(Operation == 'Row ID Computation'), aes(x = N_str, y = Time, color = Table)) +
  geom_line(size = 1) +  # Line for each table
  geom_point(size = 2) +  # Points for each table
  labs(title = 'Row ID Computation, 500 to 10m Rows',
       x = 'Number of Tuples (N)', y = 'Average Time (seconds)') +
  scale_y_continuous(breaks = seq(0, ceiling(max(df_avg$Time[df_avg$Operation == 'Row ID Computation'], na.rm = TRUE)), by = 10)) +
  theme_bw() +  # White background theme
  theme(legend.title = element_text(size = 10))

# Save the plot to a file with higher resolution
ggsave(filename = 'plots/row_id_computation_performance.png', plot = p_row_id, dpi = 300)

# Create a single plot for Row ID Computation and Storage
p_row_id_storage <- ggplot(df_avg %>% filter(Operation == 'Row ID Computation and Storage'), aes(x = N_str, y = Time, color = Table)) +
  geom_line(size = 1) +  # Line for each table
  geom_point(size = 2) +  # Points for each table
  labs(title = 'Row ID Computation and Storage, 500 to 10m Rows',
       x = 'Number of Tuples (N)', y = 'Average Time (seconds)') +
  scale_y_continuous(breaks = seq(0, ceiling(max(df_avg$Time[df_avg$Operation == 'Row ID Computation and Storage'], na.rm = TRUE)), by = 10)) +
  theme_bw() +  # White background theme
  theme(legend.title = element_text(size = 10))

# Save the plot to a file with higher resolution
ggsave(filename = 'plots/row_id_computation_and_storage_performance.png', plot = p_row_id_storage, dpi = 300)
