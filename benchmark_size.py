import conda.exports
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

# File to save results
results_file = "benchmark_results_row_id_size.csv"

import json

def get_compressed_size(cur, table):
    query = "SELECT METADATA$PARTITION_METADATA_INFORMATION FROM " + table
    cur.execute(query)
    results = cur.fetchall()

    total_compressed_size = 0
    total_decompressed_size = 0

    # Iterate over the results
    for result in results:
        # The result is a single column, which contains the JSON string
        json_data = result[0]

        # Parse the JSON data
        data = json.loads(json_data)

        # Extract the compressed and uncompressed sizes of METADATA$MT_ROW_ID
        for column in data['rowGroup']['columns']:
            if column['name'] == 'METADATA$MT_ROW_ID':
                compressed_size = column['pageIndex']['metadata'][0]['compressedSize']
                uncompressed_size = column.get('uncompressedSize', 0)  # Use 0 if uncompressed size is not found

                total_compressed_size += compressed_size
                total_decompressed_size += uncompressed_size

    return total_compressed_size, total_decompressed_size


def get_compressed_size_sdt(cur, table):
    query = "SELECT METADATA$PARTITION_METADATA_INFORMATION FROM " + table
    cur.execute(query)
    results = cur.fetchall()

    total_compressed_size = 0
    total_uncompressed_size = 0

    # Iterate through all the rows in the results
    for result in results:
        # Assuming the JSON data is in the first column of each result row
        json_data = result[0]

        # Parse the JSON string
        data = json.loads(json_data)

        # Extract and accumulate the totalCompressedSize and totalUncompressedSize
        total_compressed_size += data.get('rowGroup', {}).get('totalCompressedSize', 0)
        total_uncompressed_size += data.get('rowGroup', {}).get('totalUncompressedSize', 0)

    return total_compressed_size, total_uncompressed_size


def write_size(cur, results_file, query_first_part, query_second_part, tables, operation, run, N):
    for table in tables:

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
                pass
                # setting parameters
                # cur.execute("alter session set ENABLE_MOD_GUARD = true")
                # cur.execute("alter session set obfuscation_level = 5")
                # total_size_compressed, total_size_decompressed = get_compressed_size(cur, table)
                # file.write(f"{N},COMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                # file.write(f"{N},DECOMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")
                # cur.execute("alter session set obfuscation_level = 0")
                # cur.execute("alter session set ENABLE_MOD_GUARD = false")ƒ
            else:
                cur.execute(query_first_part + table + query_second_part)
                row = cur.fetchone()
                total_size_compressed = row[0]
                total_size_decompressed = row[1]
                file.write(f"{N},COMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")


def write_size_sdt(cur, results_file, query_first_part, query_second_part, tables, operation, run, N):
    for table in tables:

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
                pass
                # setting parameters
                # cur.execute("alter session set ENABLE_MOD_GUARD = true")
                # cur.execute("alter session set obfuscation_level = 5")
                # total_size_compressed, total_size_decompressed = get_compressed_size_sdt(cur, table)
                # file.write(f"{N},COMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                # file.write(f"{N},DECOMPRESSED,ICEBERG,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")
                # cur.execute("alter session set ENABLE_MOD_GUARD = false")
                # cur.execute("alter session set obfuscation_level = 0")
            else:
                cur.execute(query_first_part + table + query_second_part)
                row = cur.fetchone()
                total_size_compressed = row[0]
                total_size_decompressed = row[1]
                file.write(f"{N},COMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_compressed}\n")
                file.write(f"{N},DECOMPRESSED,FDN,{first_part},{second_part},{operation},{run},{total_size_decompressed}\n")


def refresh_tables(cur, dynamic_tables):
    for table in dynamic_tables:
        cur.execute(f"ALTER DYNAMIC TABLE {table} REFRESH")


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
    runs = 3

    dynamic_tables = [
        "dynamic_table_scan",
        "dynamic_table_group_by",
        "dynamic_table_group_by_strings",
        "dynamic_table_join",
        "dynamic_table_flatten",
        "dynamic_table_scan_iceberg",
        "dynamic_table_group_by_iceberg",
        "dynamic_table_group_by_strings_iceberg",
        "dynamic_table_join_iceberg",
        "dynamic_table_flatten_iceberg"
    ]

    tables = [
        "map_table_scan",
        "map_table_group_by",
        "map_table_group_by_strings",
        "map_table_join",
        "map_table_flatten",
        "map_table_scan_iceberg",
        "map_table_group_by_iceberg",
        "map_table_group_by_strings_iceberg",
        "map_table_join_iceberg",
        "map_table_flatten_iceberg",
        "array_table_scan",
        "array_table_group_by",
        "array_table_group_by_strings",
        "array_table_join",
        "array_table_flatten",
        "object_table_scan",
        "object_table_group_by",
        "object_table_group_by_strings",
        "object_table_join",
        "object_table_flatten"
    ]

    # Write the header to the file
    with open(results_file, "w") as file:
        file.write("N,Compression,Storage,Table,Query,Operation,Run,Size\n")

    for N in row_counts:
        for run in range(1, runs + 1):

            # Connect to Snowflake
            conn = snowflake.connector.connect(**DB_CONFIG_REG_ADMIN)
            cur = conn.cursor()

            cur.execute("""
                select system$creds_add_credentials_to_pool(
                'COMMON_AWS_INTEGRATION',
                'AWS',
                '[{
                "AWS_KEY_ID": "xxxx",
                "AWS_SECRET_KEY": "xxxx",
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
            credentials = (AWS_KEY_ID = 'xxxx', AWS_SECRET_KEY = 'xxxx')
            FILE_FORMAT = (TYPE = 'JSON')
            """)

            cur.execute("alter session set ENABLE_MOD_GUARD = false")
            cur.execute("alter session set enable_fix_1622597_pq_write_correct_distinctcount = Disable")
            cur.execute("alter session set ENABLE_PARQUET_FOOTER_RETRIEVAL = true")
            cur.execute("alter session set UNSAFE_ENABLE_RETRIEVE_PARQUET_FOOTER_OPTIMIZATION = true")
            cur.execute("alter session set ENABLE_PARQUET_MIN_MAX_METADATA_RETRIEVAL = true;")

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
            CREATE OR REPLACE TABLE temp_data_join AS
            SELECT key_int, RANDSTR({S}, RANDOM()) AS key_string FROM temp_data
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

            cur.execute(f"""
            CREATE OR REPLACE ICEBERG TABLE temp_data_iceberg
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT UNIFORM(1, 1000000, RANDOM()) AS key_int, RANDSTR({S}, RANDOM()) AS key_string
            FROM TABLE(GENERATOR(ROWCOUNT => {N}))
            """)

            cur.execute(f"""
            CREATE OR REPLACE ICEBERG TABLE temp_data_strings_iceberg
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
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
            CREATE OR REPLACE ICEBERG TABLE temp_data_join_iceberg
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT key_int, RANDSTR({S}, RANDOM()) AS key_string FROM temp_data
            """)

            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_iceberg (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)

            cur.execute("""
            INSERT INTO map_table_iceberg
            SELECT OBJECT_CONSTRUCT(key_string, key_int)::MAP(VARCHAR, NUMERIC(9, 0))
            FROM temp_data_iceberg
            """)

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

            cur.execute("""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_scan_iceberg
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT key_int FROM temp_data
            """)

            cur.execute("""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_group_by_iceberg
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT key_string, COUNT(key_int) AS c
            FROM temp_data
            GROUP BY key_string
            """)

            cur.execute("""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_group_by_strings_iceberg
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT key_string1, key_string2, key_string3, key_string4, key_string5, COUNT(key_int) AS c
            FROM temp_data_strings
            GROUP BY key_string1, key_string2, key_string3, key_string4, key_string5
            """)

            cur.execute("""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_join_iceberg
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT temp_data_join.key_int, temp_data_join.key_string
            FROM temp_data
            INNER JOIN temp_data_join
            ON temp_data.key_int = temp_data_join.key_int
            """)

            cur.execute("""
            CREATE OR REPLACE DYNAMIC ICEBERG TABLE dynamic_table_flatten_iceberg
            INITIALIZE = on_schedule
            TARGET_LAG = '1 hour'
            WAREHOUSE = bench
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            AS SELECT f.key
            FROM map_table, LATERAL FLATTEN(input => map_table.key_array) f;
            """)

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
            CREATE OR REPLACE TABLE object_table_scan (
                key_array VARIANT
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
            CREATE OR REPLACE TABLE object_table_join (
                key_array VARIANT
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
            CREATE OR REPLACE TABLE object_table_group_by (
                key_array VARIANT
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
            CREATE OR REPLACE TABLE object_table_group_by_strings (
                key_array VARIANT
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
            cur.execute("""
            CREATE OR REPLACE TABLE object_table_flatten (
                key_array VARIANT
            )
            """)

            # Join tables
            cur.execute("""
            CREATE OR REPLACE TABLE array_table_scan_join (
                key_array ARRAY
            )
            """)
            cur.execute("""
            CREATE OR REPLACE TABLE map_table_scan_join (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            """)

            cur.execute("""
            CREATE OR REPLACE TABLE object_table_scan_join (
                key_array VARIANT
            )
            """)

            # Iceberg
            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_scan_iceberg (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)
            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_join_iceberg (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)
            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_group_by_iceberg (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)
            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_group_by_strings_iceberg (
                key_array MAP(VARCHAR, NUMERIC(9, 0))
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)
            cur.execute("""
            CREATE OR REPLACE ICEBERG TABLE map_table_flatten_iceberg (
                key_array MAP(VARCHAR, VARCHAR)
            )
            EXTERNAL_VOLUME = 'ibattiston_iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 's3://datalake-storage-team/iceberg_writes/'
            """)

            # Refresh tables
            refresh_tables(cur, dynamic_tables)

            cur.execute("INSERT INTO array_table_scan SELECT ARRAY_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_scan")
            cur.execute("INSERT INTO map_table_scan SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_scan")
            cur.execute("INSERT INTO object_table_scan SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_scan")

            cur.execute("INSERT INTO array_table_join SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_join")
            cur.execute("INSERT INTO map_table_join SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_join")
            cur.execute("INSERT INTO object_table_join SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_join")

            cur.execute("INSERT INTO array_table_group_by SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by")
            cur.execute("INSERT INTO map_table_group_by SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by")
            cur.execute("INSERT INTO object_table_group_by SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by")

            cur.execute("INSERT INTO array_table_group_by_strings SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by_strings")
            cur.execute("INSERT INTO map_table_group_by_strings SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_strings")
            cur.execute("INSERT INTO object_table_group_by_strings SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_group_by_strings")

            cur.execute("INSERT INTO array_table_flatten SELECT ARRAY_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), key) FROM dynamic_table_flatten")
            cur.execute("INSERT INTO map_table_flatten SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), key)::MAP(VARCHAR, VARCHAR) FROM dynamic_table_flatten")
            cur.execute("INSERT INTO object_table_flatten SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), key) FROM dynamic_table_flatten")

            cur.execute("INSERT INTO map_table_scan_iceberg SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_scan_iceberg")
            cur.execute("INSERT INTO map_table_join_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_join_iceberg")
            cur.execute("INSERT INTO map_table_group_by_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_iceberg")
            cur.execute("INSERT INTO map_table_group_by_strings_iceberg SELECT OBJECT_CONSTRUCT(SPLIT_PART(METADATA$MT_ROW_ID, ':', 0), METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_group_by_strings_iceberg")

            print(f"Benchmark (size) initialized for N = {N}, Run = {run}...")

            # Measure sizes
            write_size(cur, results_file, query_first_part, query_second_part, dynamic_tables, "initial_refresh", run, N)
            write_size_sdt(cur, results_file, query_first_part_sdt, query_second_part_sdt, tables, "initial_refresh", run, N)

            # Now measuring join time of structured data types
            cur.execute("INSERT INTO array_table_scan_join SELECT ARRAY_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_scan ORDER BY RANDOM()")
            cur.execute("INSERT INTO map_table_scan_join SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER)::MAP(VARCHAR, NUMERIC(9, 0)) FROM dynamic_table_scan ORDER BY RANDOM()")
            cur.execute("INSERT INTO object_table_scan_join SELECT OBJECT_CONSTRUCT(METADATA$PARTITION_NAME, METADATA$PARTITION_ROW_NUMBER) FROM dynamic_table_scan ORDER BY RANDOM()")

            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data_row_id AS
            SELECT METADATA$MT_ROW_ID AS key_string
            FROM dynamic_table_scan
            """)
            cur.execute(f"""
            CREATE OR REPLACE TABLE temp_data_row_id_join AS
            SELECT METADATA$MT_ROW_ID AS key_string_join 
            FROM dynamic_table_scan
            ORDER BY RANDOM()
            """)

            results_file_join = "benchmark_results_row_id_join.csv"
            with open(results_file_join, "w") as file:
                file.write("N,Operation,Table,Run,Time\n")

            # INNER JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan INNER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan INNER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan INNER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN,OBJECT,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM temp_data_row_id INNER JOIN temp_data_row_id_join ON temp_data_row_id.key_string = temp_data_row_id_join.key_string_join")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN,ROW_ID,{run},{end_time - start_time}\n")

            # LEFT JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan LEFT OUTER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan LEFT OUTER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan LEFT OUTER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN,OBJECT,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM temp_data_row_id LEFT OUTER JOIN temp_data_row_id_join ON temp_data_row_id.key_string = temp_data_row_id_join.key_string_join")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN,ROW_ID,{run},{end_time - start_time}\n")

            # FULL JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan FULL OUTER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan FULL OUTER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan FULL OUTER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN,OBJECT,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM temp_data_row_id FULL OUTER JOIN temp_data_row_id_join ON temp_data_row_id.key_string = temp_data_row_id_join.key_string_join")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN,ROW_ID,{run},{end_time - start_time}\n")

            cur.execute("UPDATE array_table_scan SET key_array = ARRAY_INSERT(key_array, 2, 123456789)")
            cur.execute("UPDATE map_table_scan SET key_array = MAP_INSERT(key_array, 'some_string', 123456789)")
            cur.execute("UPDATE object_table_scan SET key_array = OBJECT_INSERT(key_array, 'some_string', 123456789)")
            cur.execute("UPDATE array_table_scan_join SET key_array = ARRAY_INSERT(key_array, 2, 123456789)")
            cur.execute("UPDATE map_table_scan_join SET key_array = MAP_INSERT(key_array, 'some_string', 123456789)")
            cur.execute("UPDATE object_table_scan_join SET key_array = OBJECT_INSERT(key_array, 'some_string', 123456789)")

            # INNER JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan INNER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN_APPEND,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan INNER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN_APPEND,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan INNER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},INNER_JOIN_APPEND,OBJECT,{run},{end_time - start_time}\n")

            # LEFT JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan LEFT OUTER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN_APPEND,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan LEFT OUTER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN_APPEND,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan LEFT OUTER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},LEFT_JOIN_APPEND,OBJECT,{run},{end_time - start_time}\n")

            # FULL JOIN
            start_time = time.time()
            cur.execute("SELECT * FROM array_table_scan FULL OUTER JOIN array_table_scan_join ON array_table_scan.key_array = array_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN_APPEND,ARRAY,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM map_table_scan FULL OUTER JOIN map_table_scan_join ON map_table_scan.key_array = map_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN_APPEND,MAP,{run},{end_time - start_time}\n")

            start_time = time.time()
            cur.execute("SELECT * FROM object_table_scan FULL OUTER JOIN object_table_scan_join ON object_table_scan.key_array = object_table_scan_join.key_array")
            end_time = time.time()
            with open(results_file_join, "a") as file:
                file.write(f"{N},FULL_JOIN_APPEND,OBJECT,{run},{end_time - start_time}\n")



    cur.close()
    conn.close()

if __name__ == "__main__":
    run_benchmark()
