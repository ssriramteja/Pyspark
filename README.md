# 📌 Hive Table Metadata Extractor for 3000+ tables with Excel Input

## 🔹 Overview
This **PySpark script** extracts metadata from **Hive tables**, reads table names from an **Excel file**, and saves the results in both **CSV** and **Excel formats**. The processed files are stored **locally** and uploaded to **HDFS** for further access.

---

## 🔹 What This Script Does
✔ **Loads table names** from an Excel file (`tables_input.xlsx`).  
✔ **Extracts metadata** for each Hive table, including:
   - 📍 **Table Location**
   - 👤 **Owner**
   - ⏳ **Creation Time**
   - 🕒 **Last Access Time**
   - 🔢 **Row Count**
✔ **Processes tables in parallel** for fast execution.  
✔ **Saves the results** as:
   - 📄 **CSV** (`hive_metadata.csv`)
   - 📊 **Excel** (`hive_metadata.xlsx`)
✔ **Uploads output to HDFS** for easy access.  
✔ **Handles missing or non-existent tables gracefully.**  

---

## 🔹 Technologies Used
- 🐍 **Python**
- 🔥 **PySpark**
- 📊 **Pandas**
- 🗄 **HDFS**
- 💻 **Hue Portal**
- ⚡ **ThreadPoolExecutor** (for parallel processing)
- 📑 **OpenPyXL** (for Excel handling)

---






**Code**

## 1. Imports and Spark Session Initialization

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HiveTableMetadata") \
    .enableHiveSupport() \
    .config("spark.executor.memory", '10g') \
    .config("spark.network.timeout", '800') \
    .config("spark.executor.cores", '3') \
    .config("spark.driver.memory", '8g') \
    .config("spark.sql.warehouse.dir", 'enter the path') \
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
    .getOrCreate()
```

- **Libraries**: Imports necessary libraries for Spark, threading, data handling, and interacting with HDFS.
- **SparkSession**: Initializes a Spark session with Hive support enabled. This allows the code to run Spark SQL queries on Hive tables.

## 2. Loading Table Names from Excel

```python
input_excel_path = "tables_input.xlsx"  # Path to the input Excel file
try:
    df_tables = pd.read_excel(input_excel_path, engine="openpyxl")
    specific_tables = df_tables['Table_Name'].dropna().tolist()  # Ensure no NaN values
    print(f"Loaded {len(specific_tables)} tables from Excel.")
except Exception as e:
    print(f"Error reading Excel file: {e}")
    specific_tables = []
```

- **Excel File**: Loads table names from an Excel file. This allows the user to specify which Hive tables to process.
- **Pandas**: Uses `pandas` to read the Excel file and extract the `Table_Name` column as a list of table names.

## 3. Defining the Schema for Table Metadata

```python
schema_name = "Enter the schema name"

schema = StructType([
    StructField("Table_Name", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Owner", StringType(), True),
    StructField("CreateTime", StringType(), True),
    StructField("LastAccessTime", StringType(), True),
    StructField("Row_count", LongType(), True)
])
```

- **Schema**: Defines the schema for the table metadata, specifying the types of information to retrieve for each table (e.g., name, location, owner, creation time, row count).

## 4. Fetching Metadata for Each Table

```python
def get_table_details(table_name):
    """Fetch table metadata and row count for a given Hive table."""
    try:
        describe_df = spark.sql(f"DESCRIBE FORMATTED {schema_name}.{table_name}")
        table_info = describe_df.selectExpr("col_name as Column", "data_type as Value").collect()
        
        # Additional metadata fetching logic here
    except Exception as e:
        print(f"Error retrieving details for table {table_name}: {e}")
        return None
```

- **Metadata Retrieval**: Defines a function to query Hive for the table metadata using `DESCRIBE FORMATTED`. This retrieves the table's structure and metadata like column names and data types.

## 5. Processing Tables in Batches

```python
def process_tables_in_batches(tables):
    batch_size = 5
    batches = [tables[i:i + batch_size] for i in range(0, len(tables), batch_size)]
    all_table_details = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(get_table_details, table): table for batch in batches for table in batch}
        
        for future in as_completed(futures):
            try:
                table_details = future.result(timeout=60)  # Timeout to prevent stuck processes
                all_table_details.append(table_details)
            except Exception as e:
                print(f"Error in batch execution: {e}")
    
    return all_table_details
```

- **Concurrency**: Uses `ThreadPoolExecutor` to process tables concurrently in batches, which speeds up the metadata extraction process.

## 6. Saving Metadata to Local Files (CSV and Excel)

```python
# Convert to DataFrame
try:
    table_details_df = spark.createDataFrame(table_details, schema=schema)
    pandas_df = table_details_df.toPandas()  # Convert to Pandas DataFrame

    # Define local paths for saving
    local_csv_path = "define the path"
    local_excel_path = "define the path"

    # Save to CSV
    pandas_df.to_csv(local_csv_path, index=False)
    print(f"CSV file saved locally at: {local_csv_path}")

    # Save to Excel
    pandas_df.to_excel(local_excel_path, index=False, engine='openpyxl')
    print(f"Excel file saved locally at: {local_excel_path}")
```

- **Saving Files**: Converts the Spark DataFrame into a Pandas DataFrame and saves it locally in both CSV and Excel formats.

## 7. Moving Files to HDFS

```python
# Move files from Local to HDFS (overwrite if needed)
hdfs_csv_path = "/=define the path and output table name.csv"
hdfs_excel_path = "define the path and output table name.xlsx"

os.system(f"hdfs dfs -rm -skipTrash {hdfs_csv_path} {hdfs_excel_path} 2>/dev/null")
os.system(f"hdfs dfs -put {local_csv_path} {hdfs_csv_path}")
os.system(f"hdfs dfs -put {local_excel_path} {hdfs_excel_path}")

print(f"📂 Files are now available in HDFS: {hdfs_csv_path} and {hdfs_excel_path}")
```

- **HDFS Operations**: Moves the generated CSV and Excel files from the local file system to HDFS, making the data accessible for distributed processing.

## 8. Final Execution

```python
# Process tables
if specific_tables:
    table_details = process_tables_in_batches(specific_tables)

    # Handle file saving and HDFS uploading
    # Further code for file operations...
else:
    print("No tables found in the Excel file!")
```

- **Final Process**: If tables are found in the Excel file, the metadata is extracted and saved to both local and HDFS locations. If no tables are specified, it prints a message.

## Summary of Technologies and Methods Used:
- **Technologies**:
  - **PySpark**: For interacting with Hive, retrieving table metadata, and processing data in a distributed environment.
  - **Pandas**: For handling the metadata in tabular form and exporting it to CSV and Excel files.
  - **HDFS**: For storing the output data in Hadoop's distributed file system.
- **Methods**:
  - Spark SQL (`DESCRIBE FORMATTED`) for metadata extraction.
  - ThreadPoolExecutor for concurrent processing of tables.
  - Data conversion and file saving using Pandas and PySpark.
```


