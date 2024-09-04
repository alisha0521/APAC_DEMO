import snowflake_readWrite
import snowflake.connector as sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine 
#import pandas as pd
import os
import pandas as pd
from snowflake_readWrite import read_data_Mysql,snowflake_read,write_data_Mysql,snowflake_write
from datetime import datetime

cwd = os.getcwd()
print(cwd)

def read_data():
    objfile = f"{cwd}dynamicDataIngestion\Config\objconfig.yaml"
    with open(objfile, 'r') as file:
        objconfig = yaml.safe_load(file)
        print(objconfig)
    if objconfig['source'].get('database') == 'mysql':
        tbl_name = objconfig['source'].get('tableName')
        df = snowflake_readWrite.read_data_Mysql(tbl_name)
        return df
    if objconfig['source'].get('database') == 'snowflake':
        tbl_name = objconfig['source'].get('tableName')
        df = snowflake_readWrite.snowflake_read(tbl_name)
    if objconfig['source'].get('database') == 'local':
        tbl_name = objconfig['source'].get('tableName')
        df = pd.read_csv(tbl_name)
        return df


def write_data():
    objfile = f"{cwd}dynamicDataIngestion\Config\objconfig.yaml"
    with open(objfile, 'r') as file:
        objconfig = yaml.safe_load(file)
        print(objconfig)
    if objconfig['target'].get('database') == 'mysql':
        tbl_name = tbl_name = objconfig['target'].get('tableName')
        df = read_data() 
        write_data_Mysql(df, tbl_name)
    if objconfig['target'].get('database') == 'snowflake':
        tbl_name = objconfig['target'].get('tableName')
        df = read_data()
        snowflake_write(df, tbl_name)
    if objconfig['target'].get('database') == 'local':
        tbl_name = objconfig['target'].get('tableName')
        df = read_data
        df.to_csv(f"{cwd}\{tbl_name}_{datetime.now()}", index=False)

    




# def write_data():
#     # Write data to the database
#     db_choice = input("Select the destination from the following option:\n"\
#                       "1. Local filesystem \n"\
#                       "2. Mysql \n"\
#                       "3. Snowflake \n"\
#                       )
#     if db_choice == '1':
#         pass
#     if db_choice == '2':
#         tbl_name = input("Enter the table Name")
#         df = snowflake_readWrite.snowflake_read(tbl_name)
#         snowflake_readWrite.write_data_Mysql(df, tbl_name)
#     if db_choice == '3':
#         tbl_name = input("Enter the table Name")
#         df = snowflake_readWrite.snowflake_read(tbl_name)
#         snowflake_readWrite.write_data_Snowflake(df, tbl_name)




# op = input("Enter the operation\n"\
#     "R for Read\n"\
#     "W for write\n").upper()

# if op == "R" :
#     read_data()
# elif op == "W" :
#     write_data()






    