from snowflake_readWrite import SnowflakeReadWrite,get_config
def spark_read_Mysql(filepath):
    # Read the file
    config = get_config(filepath)
    print(config['mysql'])

filepath = r'C:\Users\spoul01\Downloads\Dynamic Data Ingestion\dynamicDataIngestion\Config\dbConfig.yaml'
config = get_config(filepath)

