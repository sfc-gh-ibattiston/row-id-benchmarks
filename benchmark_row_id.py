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

microbenchmark = True

# Number of rows to insert
if microbenchmark:
    row_counts = [25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500]
else:
    row_counts = [500000, 1000000, 1500000, 2000000, 2500000, 3000000, 3500000, 4000000, 4500000, 5000000, 5500000, 6000000, 6500000, 7000000, 7500000, 8000000, 8500000, 9000000, 9500000, 10000000]

S = 20
runs = 5

# File to save results
if microbenchmark:
    results_file = "microbenchmark_results_row_id_micro.csv"
else:
    results_file = "benchmark_results_row_id.csv"

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
        CREATE OR REPLACE TABLE temp_data_strings AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string1,
            RANDSTR({S}, RANDOM()) AS key_string2,
            RANDSTR({S}, RANDOM()) AS key_string3,
            RANDSTR({S}, RANDOM()) AS key_string4,
            RANDSTR({S}, RANDOM()) AS key_string5
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_shuffle AS
        SELECT
            *
        FROM
            temp_data
        ORDER BY RANDOM()
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE temp_data_join AS
        SELECT
            UNIFORM(1, 1000000, RANDOM()) AS key_int,
            RANDSTR({S}, RANDOM()) AS key_string,
            UNIFORM(1, 1000000, RANDOM()) AS key_int_greatest,
            RANDSTR({S}, RANDOM()) AS key_string_greatest
        FROM
            TABLE(GENERATOR(ROWCOUNT => {N}))
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE map_table (
            key_array MAP(VARCHAR, NUMERIC(9, 0)),
            key_string VARCHAR,
            key_int NUMERIC(9, 0)
        )
        """)

        cur.execute("""
        INSERT INTO map_table 
        SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0)),
        key_string,
        key_int
        FROM temp_data
        """)

        cur.execute("""
        CREATE OR REPLACE DYNAMIC TABLE dynamic_table_scan 
        INITIALIZE = on_schedule
        TARGET_LAG = '1 hour'
        WAREHOUSE = bench
        AS SELECT key_int 
        FROM temp_data
        """)

        cur.execute("""
        CREATE OR REPLACE DYNAMIC TABLE dynamic_table_group_by 
        INITIALIZE = on_schedule
        TARGET_LAG = '1 hour'
        WAREHOUSE = bench
        AS SELECT key_string, COUNT(key_int) AS c
        FROM temp_data
        GROUP BY key_string
        """)

        cur.execute("""
        CREATE OR REPLACE DYNAMIC TABLE dynamic_table_group_by_strings 
        INITIALIZE = on_schedule
        TARGET_LAG = '1 hour'
        WAREHOUSE = bench
        AS SELECT key_string1, key_string2, key_string3, key_string4, key_string5, COUNT(key_int) AS c
        FROM temp_data_strings
        GROUP BY key_string1, key_string2, key_string3, key_string4, key_string5
        """)

        cur.execute("""
        CREATE OR REPLACE DYNAMIC TABLE dynamic_table_join
        INITIALIZE = on_schedule
        TARGET_LAG = '1 hour'
        WAREHOUSE = bench
        AS SELECT temp_data_shuffle.key_int, temp_data_shuffle.key_string
        FROM temp_data
        INNER JOIN temp_data_shuffle
        ON temp_data.key_int = temp_data_shuffle.key_int
        """)

        cur.execute("""
        CREATE OR REPLACE DYNAMIC TABLE dynamic_table_flatten
        INITIALIZE = on_schedule
        TARGET_LAG = '1 hour'
        WAREHOUSE = bench
        AS SELECT f.value 
        FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f;
        """)

        print(f"Benchmark initialized for N = {N}, Run = {run}...")

        # Sequential scan
        # Insert
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_scan AS SELECT key_int FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,scan,{run},{end_time - start_time:.2f}\n")

        # Query
        start_time = time.time()
        cur.execute("SELECT key_int FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},QUERY,scan,{run},{end_time - start_time:.2f}\n")

        # Storage of the row ID
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_row_id_scan AS SELECT CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))) AS c FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},WRITE_ROW_ID,scan,{run},{end_time - start_time:.2f}\n")

        # Calculation of the row ID
        start_time = time.time()
        cur.execute("SELECT CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))) FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},ROW_ID,scan,{run},{end_time - start_time:.2f}\n")

        # Refresh
        start_time = time.time()
        cur.execute("ALTER DYNAMIC TABLE dynamic_table_scan REFRESH")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},REFRESH,scan,{run},{end_time - start_time:.2f}\n")

        # Read the row ID from the dynamic table
        start_time = time.time()
        cur.execute("SELECT METADATA$MT_ROW_ID FROM dynamic_table_scan")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},READ_ROW_ID_DT,scan,{run},{end_time - start_time:.2f}\n")

        # GROUP BY - 1 key, string
        # Insert
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_group_by AS SELECT key_string, COUNT(key_int) AS c FROM temp_data GROUP BY key_string")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,group_by,{run},{end_time - start_time:.2f}\n")

        # Query
        start_time = time.time()
        cur.execute("SELECT key_string, COUNT(key_int) AS c FROM temp_data GROUP BY key_string")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},QUERY,group_by,{run},{end_time - start_time:.2f}\n")

        # Storage of the row ID
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_row_id_group_by AS SELECT CONCAT('~', SUBSTR(TO_JSON(ARRAY_CONSTRUCT(key_string)), 0, 20), ':', BASE64_ENCODE(SHA1_BINARY(TO_JSON(ARRAY_CONSTRUCT(key_string))))) AS c FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},WRITE_ROW_ID,group_by,{run},{end_time - start_time:.2f}\n")

        # Calculation of the row ID
        start_time = time.time()
        cur.execute("SELECT CONCAT('~', SUBSTR(TO_JSON(ARRAY_CONSTRUCT(key_int)), 0, 20), ':', BASE64_ENCODE(SHA1_BINARY(TO_JSON(ARRAY_CONSTRUCT(key_int))))) FROM temp_data")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},ROW_ID,group_by,{run},{end_time - start_time:.2f}\n")

        # Refresh
        start_time = time.time()
        cur.execute("ALTER DYNAMIC TABLE dynamic_table_group_by REFRESH")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},REFRESH,group_by,{run},{end_time - start_time:.2f}\n")

        # Read the row ID from the dynamic table
        start_time = time.time()
        cur.execute("SELECT METADATA$MT_ROW_ID FROM dynamic_table_group_by")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},READ_ROW_ID_DT,group_by,{run},{end_time - start_time:.2f}\n")

        # GROUP BY - 5 keys, strings
        # Insert
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_group_by_strings AS SELECT key_string1, key_string2, key_string3, key_string4, key_string5, COUNT(key_int) AS c FROM temp_data_strings GROUP BY key_string1, key_string2, key_string3, key_string4, key_string5")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Query
        start_time = time.time()
        cur.execute("SELECT key_string1, key_string2, key_string3, key_string4, key_string5, COUNT(key_int) AS c FROM temp_data_strings GROUP BY key_string1, key_string2, key_string3, key_string4, key_string5")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},QUERY,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Storage of the row ID
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_row_id_group_by_strings AS SELECT CONCAT('~', SUBSTR(TO_JSON(ARRAY_CONSTRUCT(key_string1, key_string2, key_string3, key_string4, key_string5)), 0, 20), ':', BASE64_ENCODE(SHA1_BINARY(TO_JSON(ARRAY_CONSTRUCT(key_string1, key_string2, key_string3, key_string4, key_string5))))) AS r FROM temp_data_strings")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},WRITE_ROW_ID,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Calculation of the row ID
        start_time = time.time()
        cur.execute("SELECT CONCAT('~', SUBSTR(TO_JSON(ARRAY_CONSTRUCT(key_string1, key_string2, key_string3, key_string4, key_string5)), 0, 20), ':', BASE64_ENCODE(SHA1_BINARY(TO_JSON(ARRAY_CONSTRUCT(key_string1, key_string2, key_string3, key_string4, key_string5))))) FROM temp_data_strings")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},ROW_ID,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Refresh
        start_time = time.time()
        cur.execute("ALTER DYNAMIC TABLE dynamic_table_group_by_strings REFRESH")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},REFRESH,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Read the row ID from the dynamic table
        start_time = time.time()
        cur.execute("SELECT METADATA$MT_ROW_ID FROM dynamic_table_group_by_strings")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},READ_ROW_ID_DT,group_by_strings,{run},{end_time - start_time:.2f}\n")

        # Inner join
        join_query = """
        SELECT
          CONCAT(
            GREATEST(
              SPLIT_PART(CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))), ':', 0),
              SPLIT_PART(CONCAT(key_string_greatest, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string_greatest, BITSHIFTRIGHT(key_int_greatest, 1))))), ':', 0)
            ),
            ':',
            BASE64_ENCODE(
              SHA1_BINARY(
                CONCAT(
                  SPLIT_PART(CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))), ':', -1),
                  '$',
                  SPLIT_PART(CONCAT(key_string_greatest, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string_greatest, BITSHIFTRIGHT(key_int_greatest, 1))))), ':', -1)
                )
              )
            )
          ) AS r
        FROM
          temp_data_join
        """

        # Insert
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_join AS SELECT temp_data_join.key_int, temp_data_join.key_string FROM temp_data INNER JOIN temp_data_join ON temp_data.key_int = temp_data_join.key_int")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,join,{run},{end_time - start_time:.2f}\n")

        # Query
        start_time = time.time()
        cur.execute("SELECT temp_data_join.key_int, temp_data_join.key_string FROM temp_data INNER JOIN temp_data_join ON temp_data.key_int = temp_data_join.key_int")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},QUERY,join,{run},{end_time - start_time:.2f}\n")

        # Storage of the row ID
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_row_id_join AS " + join_query)
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},WRITE_ROW_ID,join,{run},{end_time - start_time:.2f}\n")

        # Calculation of the row ID
        start_time = time.time()
        cur.execute(join_query)
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},ROW_ID,join,{run},{end_time - start_time:.2f}\n")

        # Refresh
        start_time = time.time()
        cur.execute("ALTER DYNAMIC TABLE dynamic_table_join REFRESH")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},REFRESH,join,{run},{end_time - start_time:.2f}\n")

        # Read the row ID from the dynamic table
        start_time = time.time()
        cur.execute("SELECT METADATA$MT_ROW_ID FROM dynamic_table_join")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},READ_ROW_ID_DT,join,{run},{end_time - start_time:.2f}\n")

        # Flatten
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_flatten AS SELECT key_string, key_int, f.path AS path FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},INSERT,flatten,{run},{end_time - start_time:.2f}\n")

        # Query
        start_time = time.time()
        cur.execute("SELECT key_string, key_int, f.path AS path FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},QUERY,flatten,{run},{end_time - start_time:.2f}\n")

        # Storage of the row ID
        flatten_query = """
        SELECT
          CONCAT(
            SPLIT_PART(CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))), ':', 0),
            ':',
            BASE64_ENCODE(
              SHA1_BINARY(
                CONCAT(SPLIT_PART(CONCAT(key_string, ':', BASE64_ENCODE(SHA1_BINARY(CONCAT(key_string, BITSHIFTRIGHT(key_int, 1))))), ':', -1), '$', path)
              )
            )
          ) AS d
        FROM
          tmp_flatten
        """
        start_time = time.time()
        cur.execute("CREATE OR REPLACE TABLE tmp_row_id_flatten AS " + flatten_query)
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},WRITE_ROW_ID,flatten,{run},{end_time - start_time:.2f}\n")

        # Calculation of the row ID
        start_time = time.time()
        cur.execute(flatten_query)
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},ROW_ID,flatten,{run},{end_time - start_time:.2f}\n")

        # Refresh
        start_time = time.time()
        cur.execute("ALTER DYNAMIC TABLE dynamic_table_flatten REFRESH")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},REFRESH,flatten,{run},{end_time - start_time:.2f}\n")

        # Read the row ID from the dynamic table
        start_time = time.time()
        cur.execute("SELECT METADATA$MT_ROW_ID FROM dynamic_table_flatten")
        end_time = time.time()
        with open(results_file, "a") as file:
            file.write(f"{N},READ_ROW_ID_DT,flatten,{run},{end_time - start_time:.2f}\n")

        print(f"Benchmark completed for N = {N}, Run = {run}...")

# Close cursor and connection
cur.close()
conn.close()

print("Benchmark executed successfully!")




