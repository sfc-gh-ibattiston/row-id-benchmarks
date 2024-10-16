import snowflake.connector
import time
import os

# SELECT TABLE_CATALOG, TABLE_NAME, ACTIVE_BYTES AS STORAGE_USAGE
# FROM "INFORMATION_SCHEMA".TABLE_STORAGE_METRICS
# --WHERE TABLE_NAME IN ('array_table_scan');

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

# File to save results
results_file = "benchmark_results_row_id_size.csv"

def write_size(cur, results_file, query_first_part, query_second_part, tables, operation, run, N):
    for table in tables:
        cur.execute(query_first_part + table + query_second_part)
        row = cur.fetchone()
        total_size_compressed = row[0]
        total_size_decompressed = row[1]

        # Split the table name based on the second occurrence of '_'
        parts = table.split('_', 2)  # Split into at most 3 parts
        if len(parts) > 2:
            first_part = parts[0] + '_' + parts[1]  # Join the first two parts
            second_part = parts[2]  # The rest is the second part
        else:
            first_part = table  # If there are fewer than two '_', keep the whole name
            second_part = ''  # Set second part as empty

        # Remove "_iceberg" from both parts if present
        first_part = first_part.replace("_iceberg", "")
        second_part = second_part.replace("_iceberg", "")

        with open(results_file, "a") as file:
            if "iceberg" in table:
                file.write(f"{N},COMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")
            else:
                file.write(f"{N},COMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")


def write_size_sdt(cur, results_file, query_first_part, query_second_part, tables, operation, run, N):
    for table in tables:
        cur.execute(query_first_part + table + query_second_part)
        row = cur.fetchone()
        total_size_compressed = row[0]
        total_size_decompressed = row[1]

        # Split the table name based on the second occurrence of '_'
        parts = table.split('_', 2)  # Split into at most 3 parts
        if len(parts) > 2:
            first_part = parts[0] + '_' + parts[1]  # Join the first two parts
            second_part = parts[2]  # The rest is the second part
        else:
            first_part = table  # If there are fewer than two '_', keep the whole name
            second_part = ''  # Set second part as empty

        # Remove "_iceberg" from both parts if present
        first_part = first_part.replace("_iceberg", "")
        second_part = second_part.replace("_iceberg", "")

        with open(results_file, "a") as file:
            if "iceberg" in table:
                file.write(f"{N},COMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")
            else:
                file.write(f"{N},COMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")

def refresh_tables(cur, dynamic_tables):
    for table in dynamic_tables:
        cur.execute(f"ALTER DYNAMIC TABLE {table} REFRESH")

def update_tables(cur, S):
    cur.execute("UPDATE temp_data SET key_int = key_int + 1 WHERE key_int % 20 = 0")
    cur.execute("UPDATE temp_data_strings SET key_string1 = key_string1 || 'a' WHERE key_int % 20 = 0")
    cur.execute("UPDATE temp_data_join SET key_int = key_int + 1 WHERE key_int % 20 = 0")
    cur.execute(f"""
        UPDATE map_table 
        SET key_array = OBJECT_CONSTRUCT(RANDSTR({S}, RANDOM()), UNIFORM(1, 1000000, RANDOM()))::MAP(VARCHAR, NUMERIC(9, 0)) 
        WHERE UNIFORM(1, 100, RANDOM()) <= 5
    """)

    #cur.execute("UPDATE temp_data_iceberg SET key_int = key_int + 1 WHERE key_int % 20 = 0")
    #cur.execute("UPDATE temp_data_strings_iceberg SET key_string1 = key_string1 || 'a' WHERE key_int % 20 = 0")
    #cur.execute("UPDATE temp_data_join_iceberg SET key_int = key_int + 1 WHERE key_int % 20 = 0")
    #cur.execute(f"""
    #    UPDATE map_table_iceberg
    #    SET key_array = OBJECT_CONSTRUCT(RANDSTR({S}, RANDOM()), UNIFORM(1, 1000000, RANDOM()))::MAP(VARCHAR, NUMERIC(9, 0))
    #    WHERE UNIFORM(1, 100, RANDOM()) <= 5
    #""")


def insert_tables(cur, N):
    cur.execute("INSERT INTO temp_data SELECT * FROM temp_data_5")
    cur.execute(f"""
                INSERT INTO temp_data_strings 
                SELECT UNIFORM(1, 1000000, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()) 
                FROM TABLE(GENERATOR(ROWCOUNT => 0.05 * {N}))
                """)
    cur.execute("INSERT INTO temp_data_join SELECT * FROM temp_data_5")
    cur.execute("INSERT INTO map_table SELECT OBJECT_CONSTRUCT(RANDSTR(20, RANDOM()), UNIFORM(1, 1000000, RANDOM()))::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data")

    # cur.execute("INSERT INTO temp_data_iceberg SELECT * FROM temp_data_5_iceberg")
    # cur.execute(f"""
    #             INSERT INTO temp_data_strings_iceberg
    #             SELECT UNIFORM(1, 1000000, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM()), RANDSTR(20, RANDOM())
    #             FROM TABLE(GENERATOR(ROWCOUNT => 0.05 * {N}))
    #             """)
    # cur.execute("INSERT INTO temp_data_join_iceberg SELECT * FROM temp_data_5_iceberg")
    # cur.execute("INSERT INTO map_table_iceberg SELECT OBJECT_CONSTRUCT(RANDSTR(20, RANDOM()), UNIFORM(1, 1000000, RANDOM()))::MAP(VARCHAR, NUMERIC(9, 0)) FROM temp_data_iceberg")


def run_benchmark():
    query_first_part = """
    WITH json_data AS (
        SELECT DISTINCT METADATA$PARTITION_NAME, METADATA$PARTITION_METADATA_INFORMATION:columns AS columns 
        FROM """
    query_second_part = """
    ), json_data_row_id AS (
        SELECT value FROM json_data, LATERAL FLATTEN(input => columns) AS comp_block 
        WHERE value:columnName = 'METADATA$MT_ROW_ID'
    ) 
    SELECT SUM(CAST(comp_block.value:textData.sizeCompressed AS NUMBER)) AS total_size_compressed, 
           SUM(CAST(comp_block.value:textData.sizeDecompressed AS NUMBER)) AS total_size_decompressed 
    FROM json_data_row_id, LATERAL FLATTEN(input => value:compBlocks) AS comp_block 
    WHERE comp_block.value:textData IS NOT NULL"""

    query_first_part_sdt = """
    WITH json_data AS (
        SELECT DISTINCT METADATA$PARTITION_NAME, METADATA$PARTITION_METADATA_INFORMATION:columns AS columns
    FROM """
    query_second_part_sdt = """
    ), json_data_row_id AS (
        SELECT value FROM json_data, LATERAL FLATTEN(input => columns) AS comp_block
    )
    SELECT
    SUM(
        COALESCE(CAST(comp_block.value:textData.sizeCompressed AS NUMBER),
    COALESCE(CAST(comp_block.value:textData.sizeDecompressed AS NUMBER), 0))
    )
    + SUM(
        COALESCE(CAST(comp_block.value:numericData.sizeCompressed AS NUMBER),
    COALESCE(CAST(comp_block.value:numericData.sizeDecompressed AS NUMBER), 0))
    )
    + SUM(
        COALESCE(CAST(comp_block.value:indexData.sizeCompressed AS NUMBER),
    COALESCE(CAST(comp_block.value:indexData.sizeDecompressed AS NUMBER), 0))  
    ) AS total_size_compressed,
    SUM(
        COALESCE(CAST(comp_block.value:textData.sizeDecompressed AS NUMBER), 0)
    )
    + SUM(
        COALESCE(CAST(comp_block.value:numericData.sizeDecompressed AS NUMBER), 0)
    )  
    + SUM(
        COALESCE(CAST(comp_block.value:indexData.sizeDecompressed AS NUMBER), 0)  
    ) AS total_size_decompressed
    FROM json_data_row_id, LATERAL FLATTEN(input => value:compBlocks) AS comp_block;"""

    row_counts = [500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000]
    #row_counts = [500]
    S = 20
    runs = 1

    dynamic_tables = [
        "dynamic_table_scan",
        "dynamic_table_group_by",
        "dynamic_table_group_by_strings",
        "dynamic_table_join",
        "dynamic_table_flatten",
        # "dynamic_table_scan_iceberg",
        # "dynamic_table_group_by_iceberg",
        # "dynamic_table_group_by_strings_iceberg",
        # "dynamic_table_join_iceberg",
        # "dynamic_table_flatten_iceberg"
    ]

    tables = [
        "map_table_scan",
        "map_table_group_by",
        "map_table_group_by_strings",
        "map_table_join",
        "map_table_flatten",
        # "map_table_scan_iceberg",
        # "map_table_group_by_iceberg",
        # "map_table_group_by_strings_iceberg",
        # "map_table_join_iceberg",
        # "map_table_flatten_iceberg",
        "array_table_scan",
        "array_table_group_by",
        "array_table_group_by_strings",
        "array_table_join",
        "array_table_flatten"
    ]

    # Write the header to the file
    with open(results_file, "w") as file:
        file.write("N,Compression,Storage,Table,Query,Operation,Run,Size\n")

    # Connect to Snowflake
    conn = snowflake.connector.connect(**DB_CONFIG_REG_ADMIN)
    cur = conn.cursor()

    cur.execute("""
        select system$creds_add_credentials_to_pool(
        'COMMON_AWS_INTEGRATION',
        'AWS',
        '[{
        "AWS_KEY_ID": "xxx",
        "AWS_SECRET_KEY": "xxx",
        "CREDENTIAL_NAME": "s3-external_stage_integrations",
        "CREDENTIAL_TYPE": "AWS_IAM_USER"}
        ]',
         '{"PROVIDER_SPECIFIC_NAME":"AWS-specific data"}')
    """)

    cur.execute("""
        alter session set
        QA_EXTERNAL_VOLUME_DEPLOYMENT_REGION='us-west-2', \
        QA_EXTERNAL_VOLUME_DEPLOYMENT_TYPE='AWS'
    """)
    cur.execute("""
        create external volume if not exists ibattiston_iceberg_volume
        STORAGE_LOCATIONS = (
            (
        NAME = 'iceberg_default_volume'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://datalake-storage-team/iceberg_writes/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::631484165566:role/datalake-storage-integration-role'
    ))""")

    cur.execute("""
    create or replace stage ibattiston_external_stage
    url = 's3://datalake-storage-team/iceberg_writes/'
    credentials = (AWS_KEY_ID = 'xxx', AWS_SECRET_KEY = 'xxx')
    FILE_FORMAT = (TYPE = 'JSON')
    """)

    for N in row_counts:
        for run in range(1, runs + 1):
            # Create temp_data and temp_data_strings tables with random data
            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data AS
            SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
            FROM TABLE(GENERATOR(ROWCOUNT => {N}))
            """)

            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data_strings
            AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, 
                   RANDSTR(4, RANDOM() % {N * 0.01}) AS key_string1, 
                   RANDSTR(4, RANDOM() % {N * 0.01 + 1}) AS key_string2, 
                   RANDSTR(4, RANDOM() % {N * 0.01 + 2}) AS key_string3, 
                   RANDSTR(4, RANDOM() % {N * 0.01 + 3}) AS key_string4, 
                   RANDSTR(4, RANDOM()) AS key_string5
            FROM TABLE(GENERATOR(ROWCOUNT => {N}))
            ORDER BY 2, 3, 4, 5, 6
            """)

            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data_5 AS
            SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
            FROM TABLE(GENERATOR(ROWCOUNT => {N * 0.05}))
            """)

            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data_join AS
            SELECT key_int, key_string FROM temp_data
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE map_table (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            INSERT INTO map_table
            SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0))
            FROM temp_data
            """)

            # cur.execute(f"""
            # CREATE OR REPLACE ICEBERG TABLE temp_data_iceberg
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
            # FROM TABLE(GENERATOR(ROWCOUNT => {N}))
            # """)
            #
            # cur.execute(f"""
            # CREATE OR REPLACE ICEBERG TABLE temp_data_strings_iceberg
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int,
            #        RANDSTR(4, RANDOM() % {N * 0.01}) AS key_string1,
            #        RANDSTR(4, RANDOM() % {N * 0.01 + 1}) AS key_string2,
            #        RANDSTR(4, RANDOM() % {N * 0.01 + 2}) AS key_string3,
            #        RANDSTR(4, RANDOM() % {N * 0.01 + 3}) AS key_string4,
            #        RANDSTR(4, RANDOM()) AS key_string5
            # FROM TABLE(GENERATOR(ROWCOUNT => {N}))
            # ORDER BY 2, 3, 4, 5, 6
            # """)
            #
            # cur.execute(f"""
            # CREATE OR REPLACE ICEBERG TABLE temp_data_5_iceberg
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
            # FROM TABLE(GENERATOR(ROWCOUNT => {N * 0.05}))
            # """)
            #
            # cur.execute(f"""
            # CREATE OR REPLACE ICEBERG TABLE temp_data_join_iceberg
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT key_int, key_string FROM temp_data
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_iceberg (
            #     key_array MAP(VARCHAR, NUMERIC(9, 0))
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)
            #
            # cur.execute("""
            # INSERT INTO map_table_iceberg
            # SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0))
            # FROM temp_data_iceberg
            # """)

            # Create dynamic tables
            cur.execute("""
            CREATE OR REPLACE DYNAMIC TABLE dynamic_table_scan 
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            AS SELECT key_int FROM temp_data
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
            AS SELECT temp_data_join.key_int, temp_data_join.key_string
            FROM temp_data
            INNER JOIN temp_data_join
            ON temp_data.key_int = temp_data_join.key_int
            """)

            cur.execute("""
            CREATE OR REPLACE DYNAMIC TABLE dynamic_table_flatten
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            AS SELECT f.key 
            FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f;
            """)

            # cur.execute("""
            # CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_scan_iceberg
            # INITIALIZE = on_schedule
            # TARGET_LAG = '1 hour'
            # WAREHOUSE = bench
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT key_int FROM temp_data
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_group_by_iceberg
            # INITIALIZE = on_schedule
            # TARGET_LAG = '1 hour'
            # WAREHOUSE = bench
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT key_string, COUNT(key_int) AS c
            # FROM temp_data
            # GROUP BY key_string
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_group_by_strings_iceberg
            # INITIALIZE = on_schedule
            # TARGET_LAG = '1 hour'
            # WAREHOUSE = bench
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT key_string1, key_string2, key_string3, key_string4, key_string5, COUNT(key_int) AS c
            # FROM temp_data_strings
            # GROUP BY key_string1, key_string2, key_string3, key_string4, key_string5
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_join_iceberg
            # INITIALIZE = on_schedule
            # TARGET_LAG = '1 hour'
            # WAREHOUSE = bench
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT temp_data_join.key_int, temp_data_join.key_string
            # FROM temp_data
            # INNER JOIN temp_data_join
            # ON temp_data.key_int = temp_data_join.key_int
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_flatten_iceberg
            # INITIALIZE = on_schedule
            # TARGET_LAG = '1 hour'
            # WAREHOUSE = bench
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # AS SELECT f.key
            # FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f;
            # """)

            # Now create map and array tables for each DT
            cur.execute("""
            CREATE OR REPLACE TABLE array_table_scan (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_scan (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE array_table_join (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_join (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE array_table_group_by (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_group_by (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE array_table_group_by_strings (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_group_by_strings (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE array_table_flatten (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_flatten (
                key_array MAP(VARCHAR, VARCHAR)
            )
            """)

            # Iceberg

            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_scan_iceberg (
            #     key_array MAP(VARCHAR, NUMERIC(9, 0))
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_join_iceberg (
            #     key_array MAP(VARCHAR, NUMERIC(9, 0))
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_group_by_iceberg (
            #     key_array MAP(VARCHAR, NUMERIC(9, 0))
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_group_by_strings_iceberg (
            #     key_array MAP(VARCHAR, NUMERIC(9, 0))
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)
            #
            # cur.execute("""
            # CREATE OR REPLACE ICEBERG TABLE map_table_flatten_iceberg (
            #     key_array MAP(VARCHAR, VARCHAR)
            # )
            # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            # CATALOG = 'SNOWFLAKE'
            # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            # """)

            # Refresh tables
            refresh_tables(cur, dynamic_tables)

            cur.execute("INSERT INTO array_table_scan SELECT ARRAY_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_scan")
            cur.execute("INSERT INTO map_table_scan SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_scan")

            cur.execute("INSERT INTO array_table_join SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_join")
            cur.execute("INSERT INTO map_table_join SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_join")

            cur.execute("INSERT INTO array_table_group_by SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by")
            cur.execute("INSERT INTO map_table_group_by SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by")

            cur.execute("INSERT INTO array_table_group_by_strings SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by_strings")
            cur.execute("INSERT INTO map_table_group_by_strings SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_strings")

            cur.execute("INSERT INTO array_table_flatten SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), key) FROM dynamic_table_flatten")
            cur.execute("INSERT INTO map_table_flatten SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), key)::MAP(VARCHAR, VARCHAR) FROM dynamic_table_flatten")

            # cur.execute("INSERT INTO map_table_scan_iceberg SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_scan_iceberg")
            # cur.execute("INSERT INTO map_table_join_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_join_iceberg")
            # cur.execute("INSERT INTO map_table_group_by_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_iceberg")
            # cur.execute("INSERT INTO map_table_group_by_strings_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_strings_iceberg")

            print(f"Benchmark initialized for N = {N}, Run = {run}...")

            # Measure sizes
            write_size(cur, results_file, query_first_part, query_second_part, dynamic_tables, "initial_refresh", run, N)
            write_size_sdt(cur, results_file, query_first_part_sdt, query_second_part_sdt, tables, "initial_refresh", run, N)

            if N <= 1000000:
                # Update tables
                update_tables(cur, N)

                # Refresh tables after update
                refresh_tables(cur, dynamic_tables)

                # Measure sizes after update
                write_size(cur, results_file, query_first_part, query_second_part, dynamic_tables, "update", run, N)

                # Insert 5% more rows
                insert_tables(cur, N)

                # Refresh tables after insert
                refresh_tables(cur, dynamic_tables)

                # Measure sizes after insert
                write_size(cur, results_file, query_first_part, query_second_part, dynamic_tables, "insert", run, N)

                # Insert 5% more rows again
                cur.execute(f"""
                CREATE OR REPLACE TABLE temp_data_5 AS
                SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
                FROM TABLE(GENERATOR(ROWCOUNT => {N * 0.05}))
                """)
                # cur.execute(f"""
                # CREATE OR REPLACE ICEBERG TABLE temp_data_5_iceberg
                # EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
                # CATALOG = 'SNOWFLAKE'
                # BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
                # AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
                # FROM TABLE(GENERATOR(ROWCOUNT => {N * 0.05}))
                # """)

                insert_tables(cur, S)

                # Refresh tables after second insert
                refresh_tables(cur, dynamic_tables)

                # Measure sizes after second insert
                write_size(cur, results_file, query_first_part, query_second_part, dynamic_tables, "insert_2", run, N)

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_benchmark()
