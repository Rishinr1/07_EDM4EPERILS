

import polars as pl
import pyodbc
import subprocess


# Read the CSV file using Polars
inputfile = input("Enter the location of the input file ")
#"input\THEQ_SAMPLE_6.csv"
df = pl.read_csv(inputfile, separator='\t') # give the location file path here
print(df)
currency="USD"
undate = "9999-12-31 00:00:00"
Idate = "12/3/2024 12:00:00 AM"
Edate = "12/2/2025 12:00:00 AM"
sample_database_name = input ("Enter the name of sample database created using RL: ")
LOCID_1=df['LOCNUM'][0]

# Define the public variable
perilno = None

# Database connection parameters
server = 'localhost'
database = sample_database_name
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Connect to the database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

cursor.execute("SELECT POLICYTYPE FROM policy")
row = cursor.fetchone()

# Assign the value to the public variable
if row:
    perilno = row.POLICYTYPE

# Close the connection
cursor.close()
conn.close()

if perilno==1:
    # Prompt the user for EQCV1VAL, EQCV2VAL, and EQCV3VAL
    EQCV1VAL = int(input("Enter the value for EQCV1VAL: "))
    EQCV2VAL = int(input("Enter the value for EQCV2VAL: "))
    EQCV3VAL = int(input("Enter the value for EQCV3VAL: "))
    # Add the values to the DataFrame for all rows
    df = df.with_columns([
    pl.lit(EQCV1VAL).alias('EQCV1VAL'),
    pl.lit(EQCV2VAL).alias('EQCV2VAL'),
    pl.lit(EQCV3VAL).alias('EQCV3VAL')
    ])
elif perilno==2:
    # Prompt the user for WSCV1VAL, WSCV2VAL, and WSCV3VAL
    WSCV1VAL = int(input("Enter the value for WSCV1VAL: "))
    WSCV2VAL = int(input("Enter the value for WSCV2VAL: "))
    WSCV3VAL = int(input("Enter the value for WSCV3VAL: "))
    # Add the values to the DataFrame for all rows
    df = df.with_columns([
    pl.lit(WSCV1VAL).alias('WSCV1VAL'),
    pl.lit(WSCV2VAL).alias('WSCV2VAL'),
    pl.lit(WSCV3VAL).alias('WSCV3VAL')
    ])
elif perilno==3:
    # Prompt the user for TOCV1VAL, TOCV2VAL, and TOCV3VAL
    TOCV1VAL = int(input("Enter the value for TOCV1VAL: "))
    TOCV2VAL = int(input("Enter the value for TOCV2VAL: "))
    TOCV3VAL = int(input("Enter the value for TOCV3VAL: "))
    # Add the values to the DataFrame for all rows
    df = df.with_columns([
    pl.lit(TOCV1VAL).alias('TOCV1VAL'),
    pl.lit(TOCV2VAL).alias('TOCV2VAL'),
    pl.lit(TOCV3VAL).alias('TOCV3VAL')
    ])

elif perilno==4:
    # Prompt the user for FLCV1VAL, FLCV2VAL, and FLCV3VAL
    FLCV1VAL = int(input("Enter the value for FLCV1VAL: "))
    FLCV2VAL = int(input("Enter the value for FLCV2VAL: "))
    FLCV3VAL = int(input("Enter the value for FLCV3VAL: "))
    # Add the values to the DataFrame for all rows
    df = df.with_columns([
    pl.lit(FLCV1VAL).alias('FLCV1VAL'),
    pl.lit(FLCV2VAL).alias('FLCV2VAL'),
    pl.lit(FLCV3VAL).alias('FLCV3VAL')
    ])
elif perilno==5:
    # Prompt the user for FRCV1VAL, FRCV2VAL, and FRCV3VAL
    FRCV1VAL = int(input("Enter the value for FRCV1VAL: "))
    FRCV2VAL = int(input("Enter the value for FRCV2VAL: "))
    FRCV3VAL = int(input("Enter the value for FRCV3VAL: "))
    # Add the values to the DataFrame for all rows
    df = df.with_columns([
    pl.lit(FRCV1VAL).alias('FRCV1VAL'),
    pl.lit(FRCV2VAL).alias('FRCV2VAL'),
    pl.lit(FRCV3VAL).alias('FRCV3VAL')
    ])
else:
    print("Unsupported perilno value")





# # Prompt the user for EQCV1VAL, EQCV2VAL, and EQCV3VAL
# EQCV1VAL = WSCV1VAL = TOCV1VAL = FLCV1VAL = FRCV1VAL = TRCV1VAL = int(input("Enter the value for EQCV1VAL: "))
# EQCV2VAL = WSCV2VAL = TOCV2VAL = FLCV2VAL = FRCV2VAL = TRCV2VAL = int(input("Enter the value for EQCV2VAL: "))
# EQCV3VAL = WSCV3VAL = TOCV3VAL = FLCV3VAL = FRCV3VAL = TRCV3VAL = int(input("Enter the value for EQCV3VAL: "))


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
        backup_file = rf"D:\Program Files\Microsoft SQL Server\MSSQLSERVER\MSSQL15.MSSQLSERVER\MSSQL\Backup\{original_database_name}_{split_number}.bak"
        data_file = rf"D:\Program Files\Microsoft SQL Server\MSSQLSERVER\MSSQL15.MSSQLSERVER\MSSQL\DATA\{new_database_name}.mdf"
        log_file = rf"D:\Program Files\Microsoft SQL Server\MSSQLSERVER\MSSQL15.MSSQLSERVER\MSSQL\Log\{new_database_name}_log.ldf"

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





# In[293]:


# Database connection parameters
server = 'localhost'
database = sample_database_name
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Connect to the database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

cursor.execute("SELECT POLICYTYPE FROM policy")
row = cursor.fetchone()

# Assign the value to the public variable
if row:
    perilno = row.POLICYTYPE

# Close the connection
cursor.close()
conn.close()

# Print the value (for verification)
print(f"POLICYTYPE: {perilno}")

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

# Unpivot the DataFrame using melt
unpivoted_df = df.melt(
    id_vars=id_vars,
    value_vars=columns_to_unpivot,
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

# Apply the mapping to losstype
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


print(unpivoted_df)

# In[294]:

import pyodbc

# Define the public variables
accgrp_id = None
ACCNTNUM = None
CNTRYCODE = None
CNTRYSCHEME = None
BLDGSCHEME = None
BLDGCLASS = None
OCCSCHEME = None
OCCTYPE = 0
NUMBLDGS = None
NUMSTORIES = None
YEARBUILT = None


# Database connection parameters
server = 'localhost'
database = sample_database_name
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Connect to the database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Query the accgrp table to get the ACCNTNUM
cursor.execute("SELECT ACCGRPNAME,ACCGRPID FROM accgrp")
row = cursor.fetchone()
if row:
    ACCNTNUM = row.ACCGRPNAME
    accgrp_id=row.ACCGRPID

# Query the property table to get the required values
cursor.execute("""
    SELECT LOCNUM,BLDGSCHEME, BLDGCLASS, YEARBUILT, OCCSCHEME, NUMSTORIES,NUMBLDGS
    FROM property
""")
row = cursor.fetchone()
if row:
    
    BLDGSCHEME = row.BLDGSCHEME
    BLDGCLASS = row.BLDGCLASS
    YEARBUILT = row.YEARBUILT
    OCCSCHEME = row.OCCSCHEME
    NUMSTORIES = row.NUMSTORIES
    NUMBLDGS=row.NUMBLDGS

# Query the Address table to get the CNTRYCODE and CNTRYSCHEME
cursor.execute("SELECT CountryCode, CountryScheme FROM Address")
row = cursor.fetchone()
if row:
    CNTRYCODE = row.CountryCode
    CNTRYSCHEME = row.CountryScheme

# Close the connection
cursor.close()
conn.close()



# Print the values (for verification)
print(f"ACCNTNUM: {ACCNTNUM}")
print(f"CNTRYCODE: {CNTRYCODE}")
print(f"CNTRYSCHEME: {CNTRYSCHEME}")
print(f"BLDGSCHEME: {BLDGSCHEME}")
print(f"BLDGCLASS: {BLDGCLASS}")
print(f"OCCSCHEME: {OCCSCHEME}")
print(f"OCCTYPE: {OCCTYPE}")
print(f"NUMSTORIES: {NUMSTORIES}")
print(f"YEARBUILT: {YEARBUILT}")
print(f"NUMBLDGS: {NUMBLDGS}")
# Print the value (for verification)
print(f"ACCGRPID: {accgrp_id}")

# Add the values to the DataFrame for all rows
df = df.with_columns([
    pl.lit(ACCNTNUM).alias('ACCNTNUM'),
    pl.lit(CNTRYCODE).alias('CNTRYCODE'),
    pl.lit(CNTRYSCHEME).alias('CNTRYSCHEME'),
    pl.lit(BLDGSCHEME).alias('BLDGSCHEME'),
    pl.lit(BLDGCLASS).alias('BLDGCLASS'),
    pl.lit(OCCSCHEME).alias('OCCSCHEME'),
    pl.lit(NUMSTORIES).alias('NUMSTORIES'),
    pl.lit(YEARBUILT).alias('YEARBUILT'),
    pl.lit(accgrp_id).alias('ACCGRPID'),
    pl.lit(NUMBLDGS).alias('NUMBLDGS'),
    pl.lit(OCCTYPE).alias('OCCTYPE')

])




address_id_counter = address_id_counter_2 = primary_id_counter_2 = loc_id_counter = loc_id_counter_2 = loc_id_counter_3 = loc_id_counter_4 = primary_id_counter = loccvg_id_counter = eqdet_counter = hudet_counter = todet_counter = fldet_counter = int(LOCID_1)


###########################555555555555555555#################5555########
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

# Table mappings and unspecified column behavior
table_mappings = {
    "Address": {
        "CountryScheme": "CNTRYSCHEME", "CountryCode": "CNTRYCODE",
        "CountryRMSCode": "CNTRYCODE", "Latitude": "LATITUDE", "Longitude": "LONGITUDE"
    },
    "Property": {"LOCNUM": "LOCNUM", "LOCNAME": "LOCNAME", "BLDGSCHEME": "BLDGSCHEME", "OCCSCHEME": "OCCSCHEME", "OCCTYPE": "OCCTYPE", "NUMBLDGS": "NUMBLDGS", "NUMSTORIES": "NUMSTORIES", 'YEARBUILT': 'YEARBUILT'},
    "loccvg": {"VALUEAMT": "Value", "LOSSTYPE": "losstype", "LABELID": "labelid", "LOCID": "LOCNUM"},
    "eqdet": {"EQDETID":"LOCNUM","LOCID": "LOCNUM"},
    "hudet": {"HUDETID":"LOCNUM","LOCID": "LOCNUM"},
    "todet": {"TODETID":"LOCNUM","LOCID": "LOCNUM"},
    "fldet": {"FLDETID": "LOCNUM", "LOCID": "LOCNUM"},


    
}

unspecified_column_behavior = {

     "Address": {
        "default": "' '",  # Default value for unspecified columns
        "null_columns": ['ParcelNumber', 'Zone3', 'GeoProductVersion', 'GeoDateTime'],
        "blank_columns": [],
        "zero_columns": ['AddressTypeID', 'CountryGeoID', 'Admin1GeoID', 'Admin2GeoID', 'Admin3GeoID', 'CityGeoID', 'PostalCodeGeoID', 'AreaID', 'Zone1GeoID', 'GeoResolutionCode', 'GeoResolutionConfidence', 'GeoAccuracyBuffer', 'GeoDataSourceID', 'GeoDataSourceVersionID', 'Admin4GeoID', 'Admin5GeoID', 'LocationCodeGeoID', 'Zone2GeoID', 'Zone3GeoID', 'Zone4GeoID', 'Zone5GeoID']
    },


    "Property": {
        "default": "0",  # General default value for unspecified columns
        "null_columns": [],  # Columns where value should be NULL
        "blank_columns": ['USERID1', 'USERID2', 'USERTXT2', 'SITENAME', 'FLOOROCCUPANCY','HUZONE'],  # Columns where value should be a blank space
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

# address_id_counter = 1
# address_id_counter_2 = 1
# primary_id_counter_2=1

# loc_id_counter = 1
# loc_id_counter_2 = 1
# loc_id_counter_3 = 1
# loc_id_counter_4=1

# primary_id_counter = 1
# loccvg_id_counter = 1
# eqdet_counter = 1
# hudet_counter = 1
# todet_counter = 1
# fldet_counter = 1


# Split the DataFrame into chunks
chunks = [df[i:i + locations_per_split] for i in range(0, len(df), locations_per_split)]
loccvg_chunks = [df[i:i + locations_per_split * 3] for i in range(0, len(df), locations_per_split * 3)]

# Process each database
for i, database in enumerate(created_databases):
    print(f"Processing database: {database}")
    sql_conn = SQLConnection(server, database)
    sql_conn.open()

    # Process Address table

    # Delete existing rows from the Address table
    sql_conn.execute("DELETE FROM Address")

    for row in chunks[i].iter_rows(named=True):
        all_columns = get_table_columns(sql_conn.cursor, "Address")
        foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, "Address")

        mapped_columns = []
        mapped_values = []

        # Add mapped columns from the DataFrame
        for table_col in all_columns:
            if table_col == 'AddressID':
                mapped_columns.append(table_col)
                mapped_values.append(f"{address_id_counter}")
            elif table_col in table_mappings["Address"]:
                df_col = table_mappings["Address"][table_col]
                if df_col in row:
                    mapped_columns.append(table_col)
                    mapped_values.append(f"'{row[df_col]}'")
            elif table_col not in foreign_key_columns:
                default_behavior = unspecified_column_behavior.get("Address", {})
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



        # Ensure the number of columns matches the number of values
        if len(mapped_columns) == len(mapped_values):
            sql_command = f"INSERT INTO Address ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
            sql_conn.execute(sql_command)

            # Increment the AddressID counter
            if 'AddressID' in mapped_columns:
                address_id_counter += 1
        else:
            print("Mismatch in columns and values, skipping row.")


    # Process Property table
    sql_conn.execute("DELETE FROM Property")
    for row in chunks[i].iter_rows(named=True):
        all_columns = get_table_columns(sql_conn.cursor, "Property")
        foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, "Property")

        mapped_columns = []
        mapped_values = []

        for table_col in all_columns:
            if table_col == 'AddressID':
                mapped_columns.append(table_col)
                mapped_values.append(f"{address_id_counter_2}")
            elif table_col == 'LOCID':
                mapped_columns.append(table_col)
                mapped_values.append(f"{loc_id_counter}")
            elif table_col == 'PRIMARYLOCID':
                mapped_columns.append(table_col)
                mapped_values.append(f"{primary_id_counter}")
            elif table_col in table_mappings["Property"]:
                df_col = table_mappings["Property"][table_col]
                if df_col in row:
                    mapped_columns.append(table_col)
                    mapped_values.append(f"'{row[df_col]}'")
            elif table_col not in foreign_key_columns:
                default_behavior = unspecified_column_behavior["Property"]
                if table_col in default_behavior["null_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("NULL")
                elif table_col in default_behavior["blank_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("' '")
                elif table_col in default_behavior["zero_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("0")
                elif table_col in default_behavior["specific_defaults"]:
                    mapped_columns.append(table_col)
                    mapped_values.append(f"'{default_behavior['specific_defaults'][table_col]}'")
                else:
                    mapped_columns.append(table_col)
                    mapped_values.append(default_behavior["default"])



        if len(mapped_columns) == len(mapped_values):
            sql_command = f"INSERT INTO Property ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
            sql_conn.execute(sql_command)
            if 'AddressID' in mapped_columns:
                address_id_counter_2 += 1
            if 'LOCID' in mapped_columns:
                loc_id_counter += 1
            if 'PRIMARYLOCID' in mapped_columns:
                primary_id_counter += 1
        else:
            print("Mismatch in columns and values, skipping row.")


    if perilno==1:
        # Delete existing rows from the eqdet table
        sql_conn.execute("DELETE FROM eqdet")

        for row in chunks[i].iter_rows(named=True):
            all_columns = get_table_columns(sql_conn.cursor, "eqdet")
            foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, "eqdet")

            mapped_columns_eq = []
            mapped_values_eq = []

            # Add mapped columns from the DataFrame
            for table_col in all_columns:
                if table_col == 'EQDETID':
                    mapped_columns_eq.append(table_col)
                    mapped_values_eq.append(f"{eqdet_counter}")
                elif table_col == 'LOCID':
                    mapped_columns_eq.append(table_col)
                    mapped_values_eq.append(f"{loc_id_counter_3}")
                elif table_col in table_mappings["eqdet"]:
                    df_col = table_mappings["eqdet"][table_col]
                    if df_col in row:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append(f"'{row[df_col]}'")
                elif table_col not in foreign_key_columns:
                    default_behavior = unspecified_column_behavior["eqdet"]
                    if table_col in default_behavior["null_columns"]:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append("NULL")
                    elif table_col in default_behavior["blank_columns"]:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append("' '")
                    elif table_col in default_behavior["zero_columns"]:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append("0")
                    elif table_col in default_behavior["specific_defaults"]:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append(f"'{default_behavior['specific_defaults'][table_col]}'")
                    else:
                        mapped_columns_eq.append(table_col)
                        mapped_values_eq.append(default_behavior["default"])

            if len(mapped_columns_eq) == len(mapped_values_eq):
                sql_command = f"INSERT INTO eqdet ({', '.join(mapped_columns_eq)}) VALUES ({', '.join(mapped_values_eq)})"
                sql_conn.execute(sql_command)

                # Increment the EQDETID and LOCID counters
                if 'EQDETID' in mapped_columns_eq:
                    eqdet_counter += 1
                if 'LOCID' in mapped_columns_eq:
                    loc_id_counter_3 += 1
            else:
                print("Mismatch in columns and values, skipping row.")

    elif perilno==2:
        #
        # Delete existing rows from the hudet table
        sql_conn.execute("DELETE FROM hudet")

        for row in chunks.iter_rows(named=True):
            all_columns = get_table_columns(sql_conn.cursor, "hudet")
            foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, "hudet")

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
                elif table_col in table_mappings["hudet"]:
                    df_col = table_mappings["hudet"][table_col]
                    if df_col in row:
                        mapped_columns.append(table_col)
                        mapped_values.append(f"'{row[df_col]}'")
                elif table_col not in foreign_key_columns:
                    default_behavior = unspecified_column_behavior["hudet"]
                    if table_col in default_behavior["null_columns"]:
                        mapped_columns.append(table_col)
                        mapped_values.append("NULL")
                    elif table_col in default_behavior["blank_columns"]:
                        mapped_columns.append(table_col)
                        mapped_values.append("' '")
                    elif table_col in default_behavior["zero_columns"]:
                        mapped_columns.append(table_col)
                        mapped_values.append("0")
                    elif table_col in default_behavior["specific_defaults"]:
                        mapped_columns.append(table_col)
                        mapped_values.append(f"'{default_behavior['specific_defaults'][table_col]}'")
                    else:
                        mapped_columns.append(table_col)
                        mapped_values.append(default_behavior["default"])



                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO hudet ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                    # Increment the HUDETID and LOCID counters
                    if 'HUDETID' in mapped_columns:
                        hudet_counter += 1
                    if 'LOCID' in mapped_columns:
                        loc_id_counter_3 += 1
                else:
                    print("Mismatch in columns and values, skipping row.")
    elif perilno==3:
        #
        # Delete existing rows from the todet table
        sql_conn.execute("DELETE FROM todet")

        for row in chunks.iter_rows(named=True):
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

                # Increment the TODETID and LOCID counters
                if 'TODETID' in mapped_columns:
                    todet_counter += 1
                if 'LOCID' in mapped_columns:
                    loc_id_counter_3 += 1

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")
    elif perilno==4:
        # Delete existing rows from the fldet table
        sql_conn.execute("DELETE FROM fldet")

        for row in chunks.iter_rows(named=True):
            for table_name, column_mapping in table_mappings.items():
                all_columns = get_table_columns(sql_conn.cursor, table_name)
                foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, table_name)

                mapped_columns = []
                mapped_values = []

                # Add mapped columns from the DataFrame
                for table_col in all_columns:
                    if table_col == 'FLDETID':
                        mapped_columns.append(table_col)
                        mapped_values.append(f"{fldet_counter}")
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

                # Increment the FLDETID and LOCID counters
                if 'FLDETID' in mapped_columns:
                    fldet_counter += 1
                if 'LOCID' in mapped_columns:
                    loc_id_counter_3 += 1

                # Ensure the number of columns matches the number of values
                if len(mapped_columns) == len(mapped_values):
                    sql_command = f"INSERT INTO {table_name} ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
                    sql_conn.execute(sql_command)
                else:
                    print("Mismatch in columns and values, skipping row.")
    else:
        print("perilno not valid")

    sql_conn.execute("DELETE FROM loccvg")

    # Use the unpivoted DataFrame for loccvg
    loccvg_chunk = unpivoted_df[i * locations_per_split * 3 : (i + 1) * locations_per_split * 3]
    for row in loccvg_chunk.iter_rows(named=True):
        all_columns = get_table_columns(sql_conn.cursor, "loccvg")
        foreign_key_columns = get_foreign_key_columns(sql_conn.cursor, "loccvg")

        mapped_columns = []
        mapped_values = []

        for table_col in all_columns:
            if table_col == 'LOCCVGID':
                mapped_columns.append(table_col)
                mapped_values.append(f"{loccvg_id_counter}")
            elif table_col in table_mappings["loccvg"]:
                df_col = table_mappings["loccvg"][table_col]
                if df_col in row:
                    mapped_columns.append(table_col)
                    mapped_values.append(f"'{row[df_col]}'")
            elif table_col not in foreign_key_columns:
                default_behavior = unspecified_column_behavior["loccvg"]
                if table_col in default_behavior["null_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("NULL")
                elif table_col in default_behavior["blank_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("' '")
                elif table_col in default_behavior["zero_columns"]:
                    mapped_columns.append(table_col)
                    mapped_values.append("0")
                elif table_col in default_behavior["specific_defaults"]:
                    mapped_columns.append(table_col)
                    mapped_values.append(f"'{default_behavior['specific_defaults'][table_col]}'")
                else:
                    mapped_columns.append(table_col)
                    mapped_values.append(default_behavior["default"])



        if len(mapped_columns) == len(mapped_values):
            sql_command = f"INSERT INTO loccvg ({', '.join(mapped_columns)}) VALUES ({', '.join(mapped_values)})"
            sql_conn.execute(sql_command)
            if 'VALUEAMT' not in mapped_columns and 'Value' in row:
                mapped_columns.append('VALUEAMT')
                mapped_values.append(f"'{row['Value']}'")

            if 'LOCCVGID' in mapped_columns:
                loccvg_id_counter += 1

        else:
            print("Mismatch in columns and values, skipping row.")
            
    print("Population completed in database:", database)



#######################555555##############




