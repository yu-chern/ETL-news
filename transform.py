import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

# 
# Load data into postgres database
def load_to_postgres_db(file_name,table_name):
    hook = PostgresHook(postgres_conn_id="postgres_connection")
    #hook.bulk_load(table_name, file_name) #table_name = 'news_etl_table'
    copy_query = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)"
    hook.copy_expert(copy_query, file_name)
    return True

## data transformation
def transform_data(df,output_file,table_name):
    #TODO: transform the data to csv format
    df.to_csv(output_file,index=None, header=None)
    #TODO: load the csv file to postgres database
    load_to_postgres_db(output_file,table_name)