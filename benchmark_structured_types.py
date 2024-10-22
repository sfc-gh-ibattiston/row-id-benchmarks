import snowflake.connector
import time
import os

# Snowflake connection configuration
gs_host = os.getenv("SF_REGRESS_GLOBAL_SERVICES_IP", "snowflake.awsuswest2preprod6.external-zone.snowflakecomputing.com")
gs_port = os.getenv("SF_REGRESS_GLOBAL_SERVICES_PORT", "8085")

DB_CONFIG_REG_ADMIN = {
    "accountname": "snowflake",
    "user": "sfc-gh-ibattiston",
    # "password": "test",
    "host": gs_host,
    "port": gs_port,
    "account": "snowflake",
    "timezone": "UTC",
    "protocol": "https",
    "authenticator": "https://snowflake.okta.com",
    "warehouse": "xxl",
    "database": "scratch",
    "schema": "ila",
}

microbenchmark = False
S = 20
runs = 5

# Number of rows to insert
if microbenchmark:
    row_counts = [25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500]
else:
    row_counts = [500000, 1000000, 1500000, 2000000, 2500000, 3000000, 3500000, 4000000, 4500000, 5000000, 5500000, 6000000, 6500000, 7000000, 7500000, 8000000, 8500000, 9000000, 9500000, 10000000]


# File to save results
if microbenchmark:
    results_file = "benchmark_results_micro.csv"
else:
    results_file = "benchmark_results.csv"

# Write the header to the file
with open(results_file, "w") as file:
    file.write("N,Operation,Table,Run,Time\n")

# Connect to Snowflake
conn = snowflake.connector.connect(**DB_CONFIG_REG_ADMIN)
cur = conn.cursor()

for N in row_counts:
    for run in range(1, runs + 1):

        if microbenchmark:
            cur.execute("alter session set local_dop = 1")

        # Create temp_data table with random data
        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_join AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_row_id AS
        SELECT
            RANDSTR(45, RANDOM()) AS key_string
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_row_id_join AS
        SELECT
            RANDSTR(45, RANDOM()) AS key_string_join
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE string_int_table (
            key_string STRING,
            key_int NUMERIC(9, 0)
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE array_table (
            key_array ARRAY
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE array_table_join (
            key_array ARRAY
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE object_table (
            key_array VARIANT
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE object_table_join (
            key_array VARIANT
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE map_table (
            key_array MAP(VARCHAR, NUMERIC(9, 0))
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE map_table_join (
            key_array MAP(VARCHAR, NUMERIC(9, 0))
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE string_map_table (
            key_string STRING,
            key_array MAP(VARCHAR, NUMERIC(9, 0))
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE string_object_table (
            key_string STRING,
            key_array VARIANT
        )
        """)

        print(f"Benchmark initialized for N = {N}, Run = {run}...")

        # Measure and insert data into string_int_table (STRING, INTEGER)
        start_time = time.time()
        cur.execute("INSERT INTO string_int_table SELECT key_string, key_int FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,string_int_table,{run},{insert_time:.2f}\n")

        # Measure and insert data into array_table (ARRAY of VARIANT with STRING and INT)
        start_time = time.time()
        cur.execute("INSERT INTO array_table SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,array_table,{run},{insert_time:.2f}\n")

        start_time = time.time()
        cur.execute("INSERT INTO object_table SELECT OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,object_table,{run},{insert_time:.2f}\n")

        # Measure and insert data into map_table (MAP with STRING and INT)
        start_time = time.time()
        cur.execute("INSERT INTO map_table SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,map_table,{run},{insert_time:.2f}\n")

        # Measure the construction in array_table (ARRAY of VARIANT with STRING and INT)
        start_time = time.time()
        cur.execute("SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},CONSTRUCT,array_table,{run},{insert_time:.2f}\n")

        # Measure the construction in object_table (OBJECT of VARIANT with STRING and INT)
        start_time = time.time()
        cur.execute("SELECT OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},CONSTRUCT,object_table,{run},{insert_time:.2f}\n")

        # Measure the construction in map_table (MAP with STRING and INT)
        start_time = time.time()
        cur.execute("SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},CONSTRUCT,map_table,{run},{insert_time:.2f}\n")

        cur.execute("INSERT INTO map_table_join SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data_join")
        cur.execute("INSERT INTO array_table_join SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data_join")
        cur.execute("INSERT INTO object_table_join SELECT OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data_join")

        # Also insert another string (for search purposes)
        cur.execute("INSERT INTO string_map_table SELECT key_string, OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")
        cur.execute("INSERT INTO string_object_table SELECT key_string, OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data")

        # Scan of the whole column
        start_time = time.time()
        cur.execute("SELECT * FROM array_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN,array_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT * FROM map_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN,map_table,{run},{scan_time:.2f}\n")

        cur.execute("SELECT * FROM object_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN,object_table,{run},{scan_time:.2f}\n")

        # Scan of the value
        start_time = time.time()
        cur.execute("SELECT key_array[1] FROM array_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_VALUE,array_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT key_array[key_string] FROM string_map_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_VALUE_1,map_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT GET(key_array, map_keys(key_array)[0]) FROM map_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_VALUE_2,map_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT key_array[key_string] FROM string_object_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_VALUE,object_table,{run},{scan_time:.2f}\n")

        # Scan of the key
        start_time = time.time()
        cur.execute("SELECT key_array[0] FROM array_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_KEY,array_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT map_keys(key_array)[0] FROM map_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_KEY,map_table,{run},{scan_time:.2f}\n")

        # todo object

        # Scan of the key and the value
        start_time = time.time()
        cur.execute("SELECT key_array[0], key_array[1] FROM array_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_KEY_VALUE,array_table,{run},{scan_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT map_keys(key_array)[0], GET(key_array, map_keys(key_array)[0]) FROM map_table")
        end_time = time.time()
        scan_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},SCAN_KEY_VALUE,map_table,{run},{scan_time:.2f}\n")

        # todo object

        # Append a new element
        start_time = time.time()
        cur.execute("UPDATE array_table SET key_array = ARRAY_INSERT(key_array, 2, 123456789)")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},APPEND,array_table,{run},{end_time - start_time:.2f}\n")

        start_time = time.time()
        cur.execute("UPDATE map_table SET key_array = MAP_INSERT(key_array, 'some_string', 123456789)")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},APPEND,map_table,{run},{end_time - start_time:.2f}\n")

        start_time = time.time()
        cur.execute("UPDATE object_table SET key_array = OBJECT_INSERT(key_array, 'some_string', 123456789)")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},APPEND,object_table,{run},{end_time - start_time:.2f}\n")


# Close cursor and connection
cur.close()
conn.close()

print("Benchmark executed successfully!")
