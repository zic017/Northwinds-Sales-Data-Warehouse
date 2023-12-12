#!/usr/bin/env python
# coding: utf-8

# In[1]:


import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime


# In[ ]:


# Connect to Snowflake
connection = snowflake.connector.connect(
    user = 'zikangc',
    password = 'Prcb4401',
    account = 'gkklnry-oab14435'
)
cur = connection.cursor()


# In[2]:


def create_database_table(dataframe, primary_key, database_name, table_name):    
    
    # Check if primary_key is a column or a list in the dataframe, composite key
    
    if isinstance(primary_key, str):
        if primary_key not in dataframe.columns:
            raise ValueError("Primary key column not found in the dataframe.")
    elif isinstance(primary_key, list):
        if not all(col in dataframe.columns for col in primary_key):
            raise ValueError("One or more primary key columns not found in the dataframe.")
    else:
        raise ValueError("Invalid primary key type. Expected string or list.")

        
   # Check if primary_key column(s) is/are unique
    if isinstance(primary_key, str):
        if not dataframe[primary_key].is_unique:
            raise ValueError("Primary key column is not unique.")
    elif isinstance(primary_key, list):
        if dataframe.duplicated(subset=primary_key).any():
            raise ValueError("Composite primary key values are not unique.")
              
    #check if the database exists, Connect the database or create it
    cur.execute(f"SHOW DATABASES LIKE '{database_name}'")
    exists = cur.fetchone()

    if exists:
        # Connect it
        cur.execute(f"USE DATABASE {database_name}")
    else:
        # Create it
        cur.execute(f"CREATE DATABASE {database_name}")

        
    # map the data type
    def map_dtype(dtype):
        if dtype == "object":
            return "STRING"
        elif dtype == "int64":
            return "INTEGER"
        elif dtype == "float64":
            return "FLOAT"
        elif dtype == "bool":
            return "BOOLEAN"
        elif dtype == "datetime64[ns]":
            return "TIMESTAMP"
        else:
            return "STRING"  # Default to STRING.

    
    # Generate the SQL CREATE or REPLACE command
    columns = []
    for column, dtype in dataframe.dtypes.items():
        if isinstance(primary_key, str) and column == primary_key:
            columns.append(f"{column} {map_dtype(dtype)} PRIMARY KEY")
        elif isinstance(primary_key, list) and column in primary_key:
            columns.append(f"{column} {map_dtype}")
        else:
            columns.append(f"{column} {map_dtype(dtype)}")

    create_table = f"CREATE OR REPLACE TABLE {table_name} (\n"
    create_table += ",\n".join(columns)
    create_table += "\n)"
     
    cur.execute(create_table)
#    print(create_table)

    # Generate the SQL INSERT command
    insert_statement = f"INSERT INTO {table_name} ("
    insert_statement += f", ".join(dataframe.columns) + f") VALUES "

    # Iterate over each row in the dataframe
    connection.autocommit(False)

    for _, row in dataframe.iterrows():
        values = []
        for value in row.values:
            if pd.isna(value):
                values.append("NULL")
            elif isinstance(value, str):
                values.append(f"'{value}'")
            elif isinstance(value, pd.Timestamp):
                values.append(f"'{value}'")
            else:
                values.append(str(value))

        insert_statement = insert_statement + "(" + ", ".join(values) + "), "

    insert_statement = insert_statement[:-2]  #  Remove the final comma and space
#    print(insert_statement)
    cur.execute(insert_statement)

    connection.commit()
    connection.autocommit(True)


# In[3]:


def create_initial_surrogate_key_mapping_table(table, surrogate_key_name, start_date, end_date):
    # Create an array of consecutive integers of the size the length of the table called surrogate_key
    num_rows = table.shape[0]
    
    # Create a new DataFrame to avoid SettingWithCopyWarning
    new_table = table.copy()
    
    # Assign the surrogate_key values to a new column in the new_table as the first column
    surrogate_key = range(num_rows)
    new_table.insert(0, surrogate_key_name, surrogate_key)  # Inserting at the first position
    
    # Create surrogate key mapping table containing surrogate key and original natural key
    surrogate_key_mapping_table = new_table.iloc[:, 0:2].copy()
    surrogate_key_mapping_table['Start_Date'] = start_date
    surrogate_key_mapping_table['End_Date'] = end_date
    surrogate_key_mapping_table['Current_Flag'] = True
    
    return new_table, surrogate_key_mapping_table


# In[ ]:




