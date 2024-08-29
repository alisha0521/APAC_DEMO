import snowflake.connector as sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine 
import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_config(filepath):
    with open(filepath, 'r') as f:
        cnf = yaml.safe_load(f)
        return cnf
    


def db_MySqlConnection(filepath):
    config = get_config(filepath)
# print(config)
    user = config['mysql'].get('user')
    password = config['mysql'].get('password').replace('@','%40')
    host = config['mysql'].get('host')
    port = config['mysql'].get('port')
    database = config['mysql'].get('database')
    # print(user, password, host, port, database)
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine,session

def read_data_Mysql(tbl_name):
    engine,session = db_MySqlConnection(filepath)
    # print(engine)
    with engine.connect() as conn:
        df = pd.DataFrame()
        result = conn.execute(f"Select * from {tbl_name}").fetchall()
        print(result)
        return df


def write_data_Mysql(tbl_name,df):
    engine,session = db_MySqlConnection(filepath)
    # print(engine)
    df.to_sql(tbl_name, engine, if_exists='replace', index=False)
    
    
    # config = get_config(filepath)
    # host = config['mysql']['host']
    # user = config['mysql']['user']
    # password = config['mysql']['password']
    # database = config['mysql']['database']
    # port = config['mysql']['port']
    # return f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'



def db_connect_sf(filepath):    
    # Extract Snowflake configuration
    config = get_config(filepath)
        # Example of accessing values from the Snowflake configuration
    print(config)
    url = URL(
            user = config['snowflake'].get('user')
            ,password = config['snowflake'].get('password')
            ,account = config['snowflake'].get('account')
            ,warehouse = config['snowflake'].get('warehouse')
            ,database = config['snowflake'].get('database')
            ,schema = config['snowflake'].get('schema')
            ,role = config['snowflake'].get('role')
            )
    engine = create_engine(url)
    connection = engine.connect()
    return connection

# def read_from_mysql(tbl_name, filepath):
#     """Read data from MySQL into a pandas DataFrame."""
#     connection = db_connect_mysql(filepath)
#     query = f"Select * from {tbl_name}"
#     if connection:
#         try:
#             df = pd.read_sql(query, connection)
#             return df
#         except mysql.connector.Error as e:
#             print(f"Error reading data: {e}")
#         finally:
#             connection.close()

# def write_to_mysql(df, table_name,if_exists='replace', truncate=False):
#     """Write a pandas DataFrame to a MySQL table."""
#     config = get_config(filepath)
#     db_config = config.get('mysql',)
#     connection = db_connect_mysql(db_config)
#     if connection:
#         cursor = connection.cursor()
#         try:
#             if truncate:
#                 cursor.execute(f"TRUNCATE TABLE {table_name}")
#                 connection.commit()
            
#             # Write DataFrame to MySQL
#             df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=False, method='multi')
#             print(f"Data written to {table_name}")
#         except mysql.connector.Error as e:
#             print(f"Error writing data: {e}")
#         finally:
#             cursor.close()
#             connection.close()


# def mysqlRead_Write(query = None,df = None,Write = False, Truncate = False, table_name = None):
#     #Connect to MYsql
#     conn = db_connect_mysql(filepath)
#     if Write:
#         # Truncate table before writing data
#         if Truncate:
#             query = f'TRUNCATE TABLE {table_name}'
#             conn.execute(query)
#             print(f'Truncated table {table_name}')
#             # Write data to table
#             if df is not None:
#                 df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
#                 print(f'Wrote data to table {table_name}')
#             else:
#                 print(f'No data to write to table {table_name}')
#     else:
#         # Read data from table
#         if query is not None:
#             print("Read table form query")
#             result = conn.execute(query)
#             df = pd.DataFrame(result.fetchall())
#             df.columns = result.keys()
#             print(f'Read data from table')
#             return df
#         else:
#             print(f'Read table using from Tbl_name')
#             query = f"select * from {table_name}"
#             result = conn.execute(query)
#             df = pd.DataFrame(result.fetchall())
#             df.columns = result.keys()
#             print(f'Read data from table {table_name}')
#             return df
        


def snowflake_write(df,table_name,Truncate=False):
    # Connect to Snowflake
    conn = db_connect_sf(filepath)
    print (conn)
    if Truncate and table_name:
        conn.execute("TRUNCATE TABLE {}".format(table_name))
    if df is not None and table_name:
        df.to_sql(table_name, con = conn,if_exists='append' ,index=False, index_label=None, chunksize=16000)
        print(f"Data has been written to {table_name}.")
        conn.close()
        return True


def snowflake_read(table_name):
    # Connect to Snowflake
    conn = db_connect_sf(filepath)
    query = f"select * from {table_name}"
    result = conn.execute(query)
    df = pd.read_sql_query(query,con = conn)
    return df
    


# ======================================================================================================================================================






        # if write:
        #     if Truncate:
        #         cur.execute(f"truncate table if exists {query}")
        #         # conn.execute(text(f"Delete from {query}"))
        #         # connection.commit()
        #     df.to_sql(query,con=connection ,if_exists='append' ,index=False, index_label=None, chunksize=16000)
        #     # success, nchunks, nrows, _ = write_pandas(conn, df, query)
        #     # print(f"success: {success}, nchunks: {nchunks}, nrows: {nrows}")
        #     conn.close()
        #     return True    
        # else:
        #     # df = cur.fetch_pandas_all()  
        #     # return df
        #     df = pd.read_sql(query, con=connection )
        #     return df
        
        # return df
    # finally:
    #     connection.close()
    #     engine.dispose()

# df.to_sql('my_table', con=engine, index=False) #make sure index is False, Snowflake doesnt accept indexes
# df.to_sql("table-name" , con,if_exists='append' ,index=False, index_label=None)


        # return sf.connect(**get_config('config.yaml')['snowflake'])
    # elif dbtype == 'postgresql':
    #     return psycopg2.connect(**get_config('config.yaml')['postgresql'])


filepath = r'.\dynamicDataIngestion\Config\dbConfig.yaml'
# config = get_config(filepath)
# # db_connect_mysql(filepath)
# tbl_name = 'DB1.SCHEMA1.SALARY'
# table_name = 'Salary1'
# df = snowflake_read(tbl_name)
# print(df)
# write_to_mysql(df, table_name,if_exists='replace', truncate=False)

# snowflake_write(df,table_name)
# db_name = 'snowflake'
# db_name = 'mysql'
# mysqlRead_Write(query = None,df = None,Write = True, Truncate = False, table_name = tbl_name)
# write_to_mysql(df, table_name = tbl_name, db_config = config.get('mysql'), if_exists='replace', truncate=False)
# db = db_connector(config,param)
# print(type(cnf))
# tbl_name = 'RAW_POS.MENU'
# query = f'select * from {tbl_name}'
# data = snowflake_read_write(query)
# print(data.columns)
# print(data)


# snowflake_read_write(query = None, df =None, write=False, Truncate=False,table_name = None)

# df = pd.read_csv(r"C:\Users\spoul01\Downloads\business-employment-data-march-2024-quarter\machine-readable-business-employment-data-mar-2024-quarter.csv")
# df = df.dropna()
# print(df)
# tbl = 'employment'

# con = db_connect_sf(filepath)
# snowflake_read_write(query = None, df =df, write=True, Truncate=False,table_name = tbl)