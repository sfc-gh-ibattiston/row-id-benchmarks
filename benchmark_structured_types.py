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

# Number of rows to insert
row_counts = [500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000]
S = 20
runs = 5

# File to save results
results_file = "benchmark_results.csv"

# Write the header to the file
with open(results_file, "w") as file:
    file.write("N,Operation,Table,Run,Time\n")

# Connect to Snowflake
conn = snowflake.connector.connect(**DB_CONFIG_REG_ADMIN)
cur = conn.cursor()

for N in row_counts:
    for run in range(1, runs + 1):
        # Create temp_data table with random data
        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string,
            UNIFORM(1, 100, RANDOM()) AS key_tiny
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_join AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string,
            UNIFORM(1, 100, RANDOM()) AS key_tiny
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
            key_object VARIANT
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

        print(f"Benchmark initialized for N = {N}, Run = {run}...")

        # Measure and insert data into string_int_table (STRING, INTEGER)
        start_time = time.time()
        cur.execute("INSERT INTO string_int_table SELECT key_string, key_int FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,string_int_table,{run},{insert_time:.2f}\n")

        # todo - should we also measure scan time?

        # Measure and insert data into array_table (ARRAY of VARIANT with STRING and INT)
        start_time = time.time()
        cur.execute("INSERT INTO array_table SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,array_table,{run},{insert_time:.2f}\n")

        # Measure the construction in array_table (ARRAY of VARIANT with STRING and INT)
        start_time = time.time()
        cur.execute("SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},CONSTRUCT,array_table,{run},{insert_time:.2f}\n")

        cur.execute("INSERT INTO array_table_join SELECT ARRAY_CONSTRUCT(key_string, key_int) FROM temp_data_join")

        # Only run the object_table insert when N <= 10000
        if N <= 100000:
            start_time = time.time()
            cur.execute("INSERT INTO object_table SELECT OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data")
            end_time = time.time()
            insert_time = end_time - start_time
            with open(results_file, "a") as file:
                file.write(f"{N},INSERT,object_table,{run},{insert_time:.2f}\n")

            # Measure the construction in object_table (OBJECT of VARIANT with STRING and INT)
            start_time = time.time()
            cur.execute("SELECT OBJECT_CONSTRUCT(key_string, key_int) FROM temp_data")
            end_time = time.time()
            insert_time = end_time - start_time
            with open(results_file, "a") as file:
                file.write(f"{N},CONSTRUCT,object_table,{run},{insert_time:.2f}\n")

        # Measure and insert data into map_table (MAP with STRING and INT)
        start_time = time.time()
        cur.execute("INSERT INTO map_table SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,map_table,{run},{insert_time:.2f}\n")

        # Measure the construction in map_table (MAP with STRING and INT)
        start_time = time.time()
        cur.execute("SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")
        end_time = time.time()
        insert_time = end_time - start_time
        with open(results_file, "a") as file:
            file.write(f"{N},CONSTRUCT,map_table,{run},{insert_time:.2f}\n")

        cur.execute("INSERT INTO map_table_join SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data_join")

        # Also insert another string (for search purposes)
        cur.execute("INSERT INTO string_map_table SELECT key_string, OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")

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

        if N <= 100000:
            cur.execute("SELECT * FROM object_table")
            end_time = time.time()
            scan_time = end_time - start_time
            with open(results_file, "a") as file:
                file.write(f"{N},SCAN,map_table,{run},{scan_time:.2f}\n")

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

        # Join
        start_time = time.time()
        cur.execute("SELECT * FROM array_table FULL OUTER JOIN array_table_join ON array_table.key_array = array_table_join.key_array")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},JOIN,array_table,{run},{end_time - start_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT * FROM map_table FULL OUTER JOIN map_table_join ON map_table.key_array = map_table_join.key_array")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},JOIN,map_table,{run},{end_time - start_time:.2f}\n")

        # Now outer join the "row ID" as baseline
        start_time = time.time()
        cur.execute("SELECT * FROM temp_data_row_id FULL OUTER JOIN temp_data_row_id_join ON temp_data_row_id.key_string = temp_data_row_id_join.key_string_join")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},JOIN,row_id,{run},{end_time - start_time:.2f}\n")

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
            file.write(f"{N},APPEND,array_table,{run},{end_time - start_time:.2f}\n")

        # Join with the new element
        start_time = time.time()
        cur.execute("SELECT * FROM array_table a1 FULL OUTER JOIN array_table a2 ON a1.key_array = a2.key_array")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},JOIN_APPEND,array_table,{run},{end_time - start_time:.2f}\n")

        start_time = time.time()
        cur.execute("SELECT * FROM map_table m1 FULL OUTER JOIN map_table m2 ON m1.key_array = m2.key_array")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},JOIN_APPEND,map_table,{run},{end_time - start_time:.2f}\n")

# Close cursor and connection
cur.close()
conn.close()

print("Benchmark executed successfully!")
