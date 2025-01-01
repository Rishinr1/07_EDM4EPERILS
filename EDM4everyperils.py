#!/usr/bin/env python
# coding: utf-8

# In[290]:




# 1 ) First Create a BLANK EDM in RISK LINK and import a sample MRI file according to the peril
# 2) Give the name of sample EDM in datbase ( )
# 3)Run the script

# In[291]:


import polars as pl


# In[292]:


# Read the CSV file using Polars
df = pl.read_csv("input\THFL_SAMPLE_6.csv", separator='\t') # give the location file path here
perilno=int(input(" Enter the peril number you are going to run "))
currency="USD"
undate = "9999-12-31 00:00:00"
Idate = "12/3/2024 12:00:00 AM"
Edate = "12/2/2025 12:00:00 AM"
sample_database_name = input ("enter the name of sample database")





# In[293]:


def get_columns_to_unpivot(perilno):
    if perilno == 1:
        return ["EQCV1VAL", "EQCV2VAL", "EQCV3VAL"]
    elif perilno == 2:
        return ["WSCV1VAL", "WSCV2VAL", "WSCV3VAL"]    
    elif perilno == 3:
        return ["TOCV1VAL", "TOCV2VAL", "TOCV3VAL"]
    elif perilno==4:
        return ["FLCV1VAL", "FLCV2VAL", "FLCV3VAL"]
    elif perilno==5:
        return["FRCV1VAL", "FRCV2VAL", "FRCV3VAL"]
    elif perilno==6:
        return["TRCV1VAL", "TRCV2VAL", "TRCV3VAL"]
    else:
        raise ValueError("Unsupported perilno value")



# Get columns to unpivot based on perilno
columns_to_unpivot = get_columns_to_unpivot(perilno)

# Define id_vars
id_vars = [col for col in df.columns if col not in columns_to_unpivot]

# Unpivot the DataFrame
unpivoted_df = df.unpivot(
    index=id_vars,
    on=columns_to_unpivot,
    variable_name="losstype",
    value_name="Value"
)

# Map losstype to corresponding values
columns_to_unpivot_mapping = {
    "EQCV1VAL": 1,
    "EQCV2VAL": 2,
    "EQCV3VAL": 3,
    "WSCV1VAL": 1,
    "WSCV2VAL": 2,
    "WSCV3VAL": 3,
    "TOCV1VAL": 1,
    "TOCV2VAL": 2,
    "TOCV3VAL": 3,
    "FLCV1VAL": 1,
    "FLCV2VAL": 2,
    "FLCV3VAL": 3,
    "FRCV1VAL": 1,
    "FRCV2VAL": 2,
    "FRCV3VAL": 3,
    "TRCV1VAL": 1,
    "TRCV2VAL": 2,
    "TRCV3VAL": 3
}

unpivoted_df = unpivoted_df.with_columns([
    pl.col("losstype").map_elements(lambda x: columns_to_unpivot_mapping.get(x, None), return_dtype=pl.Int32).alias("losstype")
])

# Add COVGMODE column
unpivoted_df = unpivoted_df.with_columns([
    pl.when(pl.col("losstype") == 2).then(3).otherwise(0).alias("COVGMODE")
])

# Define the mapping from losstype to labelid
losstype_to_labelid = {
    1: 7,
    2: 8,
    3: 9
}

# Add the labelid column based on the losstype column
unpivoted_df = unpivoted_df.with_columns([
    pl.col("losstype").map_elements(lambda x: losstype_to_labelid.get(x, None), return_dtype=pl.Int32).alias("labelid")
])

# Sort the DataFrame by LOCNUM
unpivoted_df = unpivoted_df.sort("LOCNUM")

# Add the LOCCVGID column
unpivoted_df = unpivoted_df.with_columns([
    pl.arange(1, unpivoted_df.height + 1).alias("LOCCVGID")
])


# In[294]:

import pyodbc

# Define the public variable
accgrp_id = None

# Database connection parameters
server = 'localhost'
database = sample_database_name
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Connect to the database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Query the accgrp table to get the ACCGRPID of the first row
cursor.execute("SELECT ACCGRPID FROM accgrp")
row = cursor.fetchone()

# Assign the value to the public variable
if row:
    accgrp_id = row.ACCGRPID

# Close the connection
cursor.close()
conn.close()

# Print the value (for verification)
print(f"ACCGRPID: {accgrp_id}")

# In[295]:


import subprocess

# Global list to store newly created database names
new_databases = []

# Function to execute a SQL command using sqlcmd
def execute_sql_command(server, sql_query):
    try:
        subprocess.run(f"sqlcmd -S {server} -Q \"{sql_query}\"", check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"SQL command failed: {e}")
        raise

# Function to drop a database if it exists and has a specific extension
def drop_database_if_exists(server, database_name):
    try:
        if "_" in database_name:
            drop_query = f"IF EXISTS (SELECT name FROM sys.databases WHERE name = '{database_name}') DROP DATABASE [{database_name}]"
            execute_sql_command(server, drop_query)
            print(f"Database '{database_name}' dropped successfully if it existed.")
    except Exception as e:
        print(f"Error dropping database {database_name}: {e}")

# Function to copy a database
def copy_database(server, original_database_name, new_database_name, split_number):
    """
    Copies a database on the specified SQL Server instance by performing a backup and restore operation.
    """
    try:
        print(f"Copying database: {original_database_name} to {new_database_name}")

        # Drop the new database if it already exists and has an extension
        drop_database_if_exists(server, new_database_name)

        # Define paths for backup and new database files
        backup_file = f"D:\\Program Files\\Microsoft SQL Server\\MSSQLSERVER\\MSSQL13.MSSQLSERVER\\MSSQL\\Backup\\{original_database_name}_{split_number}.bak"
        data_file = f"D:\\Program Files\\Microsoft SQL Server\\MSSQLSERVER\MSSQL13.MSSQLSERVER\MSSQL\\DATA{new_database_name}.mdf"
        log_file = f"D:\\Program Files\\Microsoft SQL Server\\MSSQLSERVER\MSSQL13.MSSQLSERVER\MSSQL\\DATA{new_database_name}_log.ldf"

        # Backup the original database
        backup_query = f"BACKUP DATABASE [{original_database_name}] TO DISK = '{backup_file}'"
        execute_sql_command(server, backup_query)

        # Restore the database with a new name
        restore_query = (
            f"RESTORE DATABASE [{new_database_name}] FROM DISK = '{backup_file}' "
            f"WITH MOVE '{original_database_name}' TO '{data_file}', "
            f"MOVE '{original_database_name}_log' TO '{log_file}'"
        )
        execute_sql_command(server, restore_query)

        print(f"Database '{new_database_name}' created successfully.")
        new_databases.append(new_database_name)

    except Exception as e:
        print("Error during database copy operation:", e)
        
# Function to count rows in the DataFrame
def count_rows_in_dataframe(df):
    return len(df)

# Function to split and create databases
def split_and_create_databases(df, server, original_database_name, locations_per_split):
    """
    Creates multiple database copies based on the number of rows in the DataFrame and the user-defined split size.
    """
    try:
        total_rows = count_rows_in_dataframe(df)
        print(f"Total rows in the dataset: {total_rows}")

        # Determine the number of splits required
        num_splits = (total_rows + locations_per_split - 1) // locations_per_split
        print(f"Number of splits required: {num_splits}")

        # Create the necessary database copies
        for i in range(1, num_splits + 1):
            new_database_name = f"{original_database_name}_{i}"
            copy_database(server, original_database_name, new_database_name, i)

        return new_databases

    except Exception as e:
        print("An error occurred while splitting and creating databases:", e)
        return []

# Main execution
if __name__ == "__main__":
    # User inputs and initial setup
    try:
        server = "localhost"
        database_name = sample_database_name
        locations_per_split = int(input("Enter the number of locations wanted in one split: "))
        # Create database copies
        created_databases = split_and_create_databases(df, server, database_name, locations_per_split)

        # Print the results
        print("Created Databases:", created_databases)

    except ValueError:
        print("Invalid input. Please enter numeric values for the split size.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# In[296]:


## adding old codes here



# In[297]:


import pyodbc
import polars as pl

# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            #print(f"Executing SQL Command: {sql_command}")
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

# Server details
server = "localhost"

# Define table mappings (DataFrame column to database table columns)
table_mappings = {
    "Address": {
        "CountryScheme": "CNTRYSCHEME", "CountryCode": "CNTRYCODE",
        "CountryRMSCode": "CNTRYCODE", "Latitude": "LATITUDE", "Longitude": "LONGITUDE"
    }
}

# Define behavior for unspecified columns
unspecified_column_behavior = {
    "Address": {
        "default": "' '",  # Default value for unspecified columns
        "null_columns": ['ParcelNumber', 'Zone3', 'GeoProductVersion', 'GeoDateTime'],
        "blank_columns": [],
        "zero_columns": ['AddressTypeID', 'CountryGeoID', 'Admin1GeoID', 'Admin2GeoID', 'Admin3GeoID', 'CityGeoID', 'PostalCodeGeoID', 'AreaID', 'Zone1GeoID', 'GeoResolutionCode', 'GeoResolutionConfidence', 'GeoAccuracyBuffer', 'GeoDataSourceID', 'GeoDataSourceVersionID', 'Admin4GeoID', 'Admin5GeoID', 'LocationCodeGeoID', 'Zone2GeoID', 'Zone3GeoID', 'Zone4GeoID', 'Zone5GeoID']
    }
}

# Counter for AddressID
address_id_counter = 4

# Assume df and created_databases are defined elsewhere
# Split the DataFrame into chunks
chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

# Open the SQL connection once
sql_conn = SQLConnection(server, created_databases[0])
sql_conn.open()

# Populate each chunk into the corresponding database
for i, chunk in enumerate(chunks):
    if i < len(created_databases):
        database = created_databases[i]
        print(f"Populating database: {database}")

        # Switch database if necessary
        if sql_conn.database != database:
            sql_conn.close()
            sql_conn = SQLConnection(server, database)
            sql_conn.open()

        # Delete existing rows from the Address table
        sql_conn.execute("DELETE FROM Address")
        print(f"All rows deleted from Address table in database {database}.")

        for row in chunk.iter_rows(named=True):
            for table_name, column_mapping in table_mappings.items():
                all_columns = get_table_columns(sql_conn.cursor, table_name)
                foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                mapped_columns = []
                mapped_values = []

                # Add mapped columns from the DataFrame
                for table_col in all_columns:
                    if table_col == 'AddressID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{address_id_counter}")
                    elif table_col in column_mapping:
                        df_col = column_mapping[table_col]
                        if df_col in row:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{row[df_col]}'")
                    elif table_col not in foreign_key_columns:
                        default_behavior = unspecified_column_behavior.get(table_name, {"default": "' '"})
                        null_columns = default_behavior.get("null_columns", [])
                        blank_columns = default_behavior.get("blank_columns", [])
                        zero_columns = default_behavior.get("zero_columns", [])

                        if table_col in null_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("NULL")
                        elif table_col in blank_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("' '")
                        elif table_col in zero_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("0")
                        else:
                            mapped_columns.append(table_col)
                            mapped_values.append(default_behavior.get("default", "0"))

                # Increment the AddressID counter
                if 'AddressID' in mapped_columns:
                    address_id_counter += 1

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")

# Close the SQL connection after everything is completed
sql_conn.close()

print("Data population completed in Address table.")


# In[298]:


#############################################################


# In[299]:


# names = [f"_{i}" for i in range(1, 96)]
# print(names)
# created_databases=names
# locations_per_split=10000


# In[300]:


import pyodbc
import polars as pl

# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            #print(f"Executing SQL Command: {sql_command}")
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

# Server details
server = "localhost"

table_mappings = {
    "property": {"LOCNUM":"LOCNUM","LOCNAME":"LOCNUM",
                 "BLDGSCHEME":"BLDGSCHEME","OCCSCHEME":"OCCSCHEME","BLDGSCHEME" :"BLDGSCHEME",
                 "OCCTYPE":"OCCTYPE",
                 "OCCTYPE":"OCCTYPE","NUMBLDGS":"NUMBLDGS","NUMSTORIES":"NUMSTORIES"},
    
}

# Define behavior for unspecified columns
unspecified_column_behavior = {
    "property": {
        "default": "0",  # General default value for unspecified columns
        "null_columns": [],  # Columns where value should be NULL
        "blank_columns": ['USERID1', 'USERID2', 'USERTXT1', 'USERTXT2', 'SITENAME', 'FLOOROCCUPANCY','HUZONE'],  # Columns where value should be a blank space
        "zero_columns": [],  # Columns where value should be 0
        "specific_defaults": {  # Columns with specific default values
            "YEARBUILT": undate,
            "INCEPTDATE": Idate,
            "EXPIREDATE": Edate,
            "FLOODDEFENSEELEVATION": "-999",
            "AREAUNIT": "2",
            "HEIGHTUNIT": "2",
            "PRIMARYBLDG": "1",
            "FLOODDEFENSEELEVATIONUNIT": "2",
            "FLOODDEFHTABOVEGRND": "-999",
            "USERGROUNDELEV": "-999",
            "USERBFE": "-999",
            "CREATEDATETIME": undate,
            "UPDATEDATETIME": undate,
            "ACCGRPID": accgrp_id,
            'OTHERZONE':currency,
            
        },
    },
}

# Counter for AddressID, LOCID, and PRIMARYLOCID
address_id_counter = 4
loc_id_counter = 4  # Initialize LOCID counter
primary_id_counter = 4  # Initialize PRIMARYLOCID counter

# Assume df and created_databases are defined elsewhere
# Split the DataFrame into chunks
chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

# Open the SQL connection once
sql_conn = SQLConnection(server, created_databases[0])
sql_conn.open()

# Populate each chunk into the corresponding database
for i, chunk in enumerate(chunks):
    if i < len(created_databases):
        database = created_databases[i]
        print(f"Populating database: {database}")

        # Switch database if necessary
        if sql_conn.database != database:
            sql_conn.close()
            sql_conn = SQLConnection(server, database)
            sql_conn.open()

        # Delete existing rows from the Property table
        sql_conn.execute("DELETE FROM Property ")
        print(f"All rows deleted from Property table in database {database}.")

        for row in chunk.iter_rows(named=True):
            for table_name, column_mapping in table_mappings.items():
                all_columns = get_table_columns(sql_conn.cursor, table_name)
                foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                mapped_columns = []
                mapped_values = []

                # Add mapped columns from the DataFrame
                for table_col in all_columns:
                    if table_col == 'AddressID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{address_id_counter}")
                    elif table_col == 'LOCID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{loc_id_counter}")
                    elif table_col == 'PRIMARYLOCID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{primary_id_counter}")
                    elif table_col in column_mapping:
                        df_col = column_mapping[table_col]
                        if df_col in row:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{row[df_col]}'")
                    elif table_col not in foreign_key_columns:
                        default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                        null_columns = default_behavior.get("null_columns", [])
                        blank_columns = default_behavior.get("blank_columns", [])
                        zero_columns = default_behavior.get("zero_columns", [])
                        specific_defaults = default_behavior.get("specific_defaults", {})

                        if table_col in null_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("NULL")
                        elif table_col in blank_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("' '")
                        elif table_col in zero_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("0")
                        elif table_col in specific_defaults:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{specific_defaults[table_col]}'")
                        else:
                            mapped_columns.append(table_col)
                            mapped_values.append(default_behavior.get("default", "0"))

                # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                if 'AddressID' in mapped_columns:
                    address_id_counter += 1
                if 'LOCID' in mapped_columns:
                    loc_id_counter += 1
                if 'PRIMARYLOCID' in mapped_columns:
                    primary_id_counter += 1

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")

# Close the SQL connection after everything is completed
sql_conn.close()

print("Data population completed in Property table.")


# In[301]:


#########################################################################################################3


# In[302]:


#now loccvg


# In[303]:


import pyodbc
import polars as pl

# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

# Server details
server = "localhost"

table_mappings = {
    "loccvg": {"VALUEAMT":"Value","LOSSTYPE":"losstype","LABELID":"labelid","LOCID":"LOCNUM"},
    

}

# Define behavior for unspecified columns
unspecified_column_behavior = {
    "loccvg": {
        "default": "0",  # General default value for unspecified columns
        "null_columns": [],  # Columns where value should be NULL
        "blank_columns": [], # Columns where value should be a blank space
        "zero_columns": [],  # Columns where value should be 0
        "specific_defaults":  {
        "ISVALID": "1",
        "PERIL": perilno,
        "LIMITCUR": currency,
        "DEDUCTCUR": currency,
        "VALUECUR": currency,
        "NONRANKINGDEDUCTCUR": currency,
        "BIPOI":12
    }
    },
}

# Counter for AddressID, LOCID, and PRIMARYLOCID
loccvg_id_counter = 4
#loc_id_counter_counter = 4


# Assume df and created_databases are defined elsewhere
# Split the DataFrame into chunks
chunks = [unpivoted_df[i:i + (locations_per_split*3)] for i in range(0, len(unpivoted_df), (locations_per_split*3))]

# Open the SQL connection once
sql_conn = SQLConnection(server, created_databases[0])
sql_conn.open()

# Populate each chunk into the corresponding database
for i, chunk in enumerate(chunks):
    if i < len(created_databases):
        database = created_databases[i]
        print(f"Populating database: {database}")

        # Switch database if necessary
        if sql_conn.database != database:
            sql_conn.close()
            sql_conn = SQLConnection(server, database)
            sql_conn.open()

        # Delete existing rows from the Property table
        sql_conn.execute("DELETE FROM loccvg ")
        print(f"All rows deleted from loccvg table in database {database}.")

        for row in chunk.iter_rows(named=True):
            for table_name, column_mapping in table_mappings.items():
                all_columns = get_table_columns(sql_conn.cursor, table_name)
                foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                mapped_columns = []
                mapped_values = []

                # Add mapped columns from the DataFrame
                for table_col in all_columns:
                    if table_col == 'LOCCVGID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{loccvg_id_counter}")
                    # elif table_col == 'LOCID':
                    #     mapped_columns.append(table_col)
                    #     mapped_values.append(f"{loc_id_counter_counter}")
                    
                    elif table_col in column_mapping:
                        df_col = column_mapping[table_col]
                        if df_col in row:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{row[df_col]}'")
                    elif table_col not in foreign_key_columns:
                        default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                        null_columns = default_behavior.get("null_columns", [])
                        blank_columns = default_behavior.get("blank_columns", [])
                        zero_columns = default_behavior.get("zero_columns", [])
                        specific_defaults = default_behavior.get("specific_defaults", {})

                        if table_col in null_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("NULL")
                        elif table_col in blank_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("' '")
                        elif table_col in zero_columns:
                            mapped_columns.append(table_col)
                            mapped_values.append("0")
                        elif table_col in specific_defaults:
                            mapped_columns.append(table_col)
                            mapped_values.append(f"'{specific_defaults[table_col]}'")
                        else:
                            mapped_columns.append(table_col)
                            mapped_values.append(default_behavior.get("default", "0"))

                # Increment the loccvg id counter counters
                if 'LOCCVGID' in mapped_columns:
                    loccvg_id_counter += 1
                # if 'LOCID' in mapped_columns:
                #     loc_id_counter_counter += 1
                
                

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")

# Close the SQL connection after everything is completed
sql_conn.close()

print("Data population completed in loccvg table.")


# In[304]:


sql_command


# In[305]:


#################################################


# In[314]:


import pyodbc
import polars as pl

# Class to manage SQL connection
class SQLConnection:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None
        self.cursor = None

    def open(self):
        # Open a persistent connection
        self.connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.cursor = self.connection.cursor()

    def execute(self, sql_command):
        try:
            self.cursor.execute(sql_command)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing command: {sql_command}\nException: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Function to get table columns
def get_table_columns(cursor, table_name):
    try:
        sql_command = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []

# Function to get foreign key columns
def get_foreign_key_columns(cursor, table_name):
    try:
        sql_command = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = '{table_name}' AND CONSTRAINT_NAME IN (
            SELECT CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        )
        """
        cursor.execute(sql_command)
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error fetching foreign key columns for table {table_name}: {e}")
        return []

# Server details
server = "localhost"



if perilno==1:

    table_mappings = {
        "eqdet": {"EQDETID":"LOCNUM","LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "eqdet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": [],  # Columns where value should be NULL
            "blank_columns": ['MMI_VERSION', "RMSCLASS", "ISOCLASS", "ATCCLASS", "FIRECLASS", "USERCLASS",
                            "ATCOCC", "SICOCC", "ISOOCC", "IBCOCC", "USEROCC"],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "PCNTCOMPLT": "100", "NONRANKINGSITEDEDCUR": currency, "NONRANKINGCOMBINEDDEDCUR": currency, "SITEDEDCUR": currency, "ISVALID": "1", "DI": "-1", "STARTDATE": undate, "YEARUPGRAD": undate, "YEARSPNKLR": undate, "COMPDATE": undate,
                "COMBINEDLIMCUR": currency, "COMBINEDDEDCUR": currency, "SITELIMCUR": currency,
            },
        },
    }

    # Counter for AddressID, LOCID, and PRIMARYLOCID
    eqdet_counter = 4
    loc_id_counter = 4  # Initialize LOCID counter

    # Assume df and created_databases are defined elsewhere
    # Split the DataFrame into chunks
    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM eqdet ")
            print(f"All rows deleted from eqdet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'EQDETID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{eqdet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(table_col)
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'EQDETID' in mapped_columns:
                        eqdet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in eqdet table.")

elif perilno==2:

    table_mappings = {
        "hudet": {"HUDETID":"LOCNUM","LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "hudet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": ['HUFLOORTYPE'],  # Columns where value should be NULL
            "blank_columns": ['MMI_VERSION', "RMSCLASS", "ISOCLASS", "ATCCLASS", "FIRECLASS", "USERCLASS",
                            "ATCOCC", "SICOCC", "ISOOCC", "IBCOCC", "USEROCC"],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "BUILDINGELEVATION": -999, 'RMSBUILDINGELEVATION':-999,'SITELIMCUR':currency,'SITEDEDCUR':currency, 'COMBINEDLIMCUR':currency, 'COMBINEDDEDCUR':currency,
                'NFIPYEAR' :9999, 'HUZONEGROUP':-99,"PCNTCOMPLT":100, "YEARUPGRAD":undate, "STARTDATE":undate ,"COMPDATE":undate,        },
        },
    }

    hudet_counter = 4
    loc_id_counter = 4  

    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM hudet ")
            print(f"All rows deleted from eqdet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'HUDETID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{hudet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(table_col)
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'HUDETID' in mapped_columns:
                        hudet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in hudet table.")

elif perilno==3:

    table_mappings = {
        "todet": {"TODETID":"LOCNUM","LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "todet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": [],  # Columns where value should be NULL
            "blank_columns": ['RMSCLASS', 'FIRECLASS', 'USERCLASS', 'ATCOCC', 'ISOOCC', 'USEROCC'],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "PCNTCOMPLT": "100",  "SITEDEDCUR": currency, "ISVALID": "1", "YEARUPGRAD": undate, "YEARSPNKLR": undate,
                "COMBINEDLIMCUR": currency, "COMBINEDDEDCUR": currency, "SITELIMCUR": currency,
            },
        },
    }

    # Counter for AddressID, LOCID, and PRIMARYLOCID
    todet_counter = 4
    loc_id_counter = 4  # Initialize LOCID counter

    # Assume df and created_databases are defined elsewhere
    # Split the DataFrame into chunks
    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM todet ")
            print(f"All rows deleted from eqdet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'TODETID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{todet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(table_col)
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'TODETID' in mapped_columns:
                        todet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in todet table.")

    
elif perilno == 4:

    table_mappings = {
        "fldet": {"FLDETID": "LOCNUM", "LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "fldet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": [],  # Columns where value should be NULL
            "blank_columns": ['BFE', 'ADDLINFO', 'PANEL', 'COBRA', 'FLOODWAY', 'SFHA', 'COMMUNITY', 'UNDERREV', 'OTHERZONES', 'USERID1', 'USERID2', 'ANNPROB', 'BASINNAME', 'FLOODDRIVER', 'LEVEES'],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "PCNTCOMPLT": "100", "SITEDEDCUR": currency, "ISVALID": "1", "RMS100FLZONE": "-1", "RMS500FLZONE": "-1", "STARTDATE": undate, "YEARUPGRAD": undate, "COMPDATE": undate, "PANELDATE": undate,
                "COMBINEDLIMCUR": currency, "COMBINEDDEDCUR": currency, "SITELIMCUR": currency, 'FINISHEDFLOOR': "-999", 'FLFFHAG': "-999", 'FLZONEGROUP': "-99",
                '"50YRRPDEF"': "0", '"75YRRPDEF"': "0", '"100YRRPDEF"': "0", '"200YRRPDEF"': "0", '"250YRRPDEF"': "0", '"500YRRPDEF"': "0", '"1000YRRPDEF"': "0", '"10000YRRPDEF"': "0",
                '"50YRRPUNDEF"': "0", '"75YRRPUNDEF"': "0", '"100YRRPUNDEF"': "0", '"200YRRPUNDEF"': "0", '"250YRRPUNDEF"': "0", '"500YRRPUNDEF"': "0", '"1000YRRPUNDEF"': "0", '"10000YRRPUNDEF"': "0",
                '"30YRRPDEF"': "0", '"30YRRPUNDEF"': "0"
            },
        },
    }

    # Counter for AddressID, LOCID, and PRIMARYLOCID
    fldet_counter = 4
    loc_id_counter = 4  # Initialize LOCID counter

    # Split the DataFrame into chunks
    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Helper function to quote column names starting with a number
    def quote_column_name(column_name):
        if column_name[0].isdigit():
            return f'"{column_name}"'
        return column_name

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM fldet ")
            print(f"All rows deleted from fldet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'FLDETID':
                            mapped_columns.append(quote_column_name(table_col))
                            mapped_values.append(f"{fldet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(quote_column_name(table_col))
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(quote_column_name(table_col))
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'FLDETID' in mapped_columns:
                        fldet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in fldet table.")

elif perilno==5:

    table_mappings = {
        "frdet": {"FRDETID":"LOCNUM","LOCID": "LOCNUM"}
    }

    # Define behavior for unspecified columns
    unspecified_column_behavior = {
        "frdet": {
            "default": "0",  # General default value for unspecified columns
            "null_columns": [],  # Columns where value should be NULL
            "blank_columns": ['WFSURFFUEL', 'WFSPECCOND', 'WFSITEHAZVERSION'],  # Columns where value should be a blank space
            "zero_columns": [],  # Columns where value should be 0
            "specific_defaults": {  # Columns with specific default values
                "PCNTCOMPLT": "100",  "NONRANKINGCOMBINEDDEDCUR": currency, "SITEDEDCUR": currency, "ISVALID": "1", "DI": "-1", "STARTDATE": undate, "YEARUPGRAD": undate, "COMPDATE": undate,
                "COMBINEDLIMCUR": currency, "COMBINEDDEDCUR": currency, "SITELIMCUR": currency,'WFSLOPE':-999, 'WFD2V':-999, 'DISTANCETOCALFIREHIGH':-999, 'DISTANCETOCALFIREMEDIUM':-999, 'DISTANCETOCALFIREVERYHIGH':-999,'YEARLASTHISTFIRE250M':9999,

            },
        },
    }

    # Counter for AddressID, LOCID, and PRIMARYLOCID
    frdet_counter = 4
    loc_id_counter = 4  # Initialize LOCID counter

    # Assume df and created_databases are defined elsewhere
    # Split the DataFrame into chunks
    chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]

    # Open the SQL connection once
    sql_conn = SQLConnection(server, created_databases[0])
    sql_conn.open()

    # Populate each chunk into the corresponding database
    for i, chunk in enumerate(chunks):
        if i < len(created_databases):
            database = created_databases[i]
            print(f"Populating database: {database}")

            # Switch database if necessary
            if sql_conn.database != database:
                sql_conn.close()
                sql_conn = SQLConnection(server, database)
                sql_conn.open()

            # Delete existing rows from the Property table
            sql_conn.execute("DELETE FROM frdet ")
            print(f"All rows deleted from frdet table in database {database}.")

            for row in chunk.iter_rows(named=True):
                for table_name, column_mapping in table_mappings.items():
                    all_columns = get_table_columns(sql_conn.cursor, table_name)
                    foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                    mapped_columns = []
                    mapped_values = []

                    # Add mapped columns from the DataFrame
                    for table_col in all_columns:
                        if table_col == 'FRDETID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{frdet_counter}")
                        elif table_col == 'LOCID':
                            mapped_columns.append(table_col)
                            mapped_values.append(f"{loc_id_counter}")
                        
                        elif table_col in column_mapping:
                            df_col = column_mapping[table_col]
                            if df_col in row:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{row[df_col]}'")
                        elif table_col not in foreign_key_columns:
                            default_behavior = unspecified_column_behavior.get(table_name, {"default": "0"})
                            null_columns = default_behavior.get("null_columns", [])
                            blank_columns = default_behavior.get("blank_columns", [])
                            zero_columns = default_behavior.get("zero_columns", [])
                            specific_defaults = default_behavior.get("specific_defaults", {})

                            if table_col in null_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("NULL")
                            elif table_col in blank_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("' '")
                            elif table_col in zero_columns:
                                mapped_columns.append(table_col)
                                mapped_values.append("0")
                            elif table_col in specific_defaults:
                                mapped_columns.append(table_col)
                                mapped_values.append(f"'{specific_defaults[table_col]}'")
                            else:
                                mapped_columns.append(table_col)
                                mapped_values.append(default_behavior.get("default", "0"))

                    # Increment the AddressID, LOCID, and PRIMARYLOCID counters
                    if 'FRDETID' in mapped_columns:
                        frdet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter += 1
                    

                    # Ensure the number of columns matches the number of values
                    if len(mapped_columns) == len(mapped_values):
                        sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                        sql_conn.execute(sql_command)
                    else:
                        print("Mismatch in columns and values, skipping row.")

    # Close the SQL connection after everything is completed
    sql_conn.close()

    print("Data population completed in frdet table.")



else:
    print("PERIL NOT VALID")


# In[ ]:





# In[ ]:





# In[ ]:




