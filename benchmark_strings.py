import snowflake.connector
import time
import os

# Snowflake connection configuration
gs_host = os.getenv("SF_REGRESS_GLOBAL_SERVICES_IP", "snowflake.reg.local")
gs_port = os.getenv("SF_REGRESS_GLOBAL_SERVICES_PORT", "8082")
account = os.getenv("SF_ACCOUNT", "testaccount")

DB_CONFIG_REG_ADMIN = {
    "user": "admin",
    "password": "test",
    "host": gs_host,
    "port": gs_port,
    "account": "snowflake",
    "timezone": "UTC",
    "protocol": "http",
    "warehouse": "bench",
    "database": "test",
    "schema": "public",
}

size = 100
runs = 3
N = 1000000

# File to save results
results_file = "benchmark_results_strings.csv"

# Write the header to the file
with open(results_file, "w") as file:
    file.write("Size,Operation,Run,Time\n")

# Connect to Snowflake
conn = snowflake.connector.connect(**DB_CONFIG_REG_ADMIN)
cur = conn.cursor()

# Iterate from 1 to size
for S in range(1, size + 1):

    # Create temp_data table with random data
    cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_strings AS
        SELECT
            RANDSTR({S}, RANDOM()) AS key_string
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

    for run in range(1, runs + 1):

        start_time = time.time()
        cur.execute("SELECT key_string FROM temp_data")
        end_time = time.time()
        read_time = end_time - start_time

        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_strings AS SELECT key_string FROM temp_data")
        end_time = time.time()
        read_write_time = end_time - start_time

        with open(results_file, "a") as file:
            file.write(f"{S},READ,{run},{read_time:.2f}\n")

        with open(results_file, "a") as file:
            file.write(f"{S},WRITE,{run},{read_write_time - read_time:.2f}\n")

        with open(results_file, "a") as file:
            file.write(f"{S},READ + WRITE,{run},{read_write_time:.2f}\n")

        print(f"Benchmark completed for N = {N}, S = {S}, Run = {run}...")

# Close cursor and connection
cur.close()
conn.close()

print("Benchmark executed successfully!")




