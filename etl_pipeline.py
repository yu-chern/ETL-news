import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

from news_extractor import extract_data
#from extract import extract_data

create_table_sql_task = """CREATE TABLE IF NOT EXISTS news_etl_table(
      id INT PRIMARY KEY,
      title VARCHAR(500),
      author VARCHAR(200),
      publishedAt DATE,
      url VARCHAR(200),
      description TEXT
    );"""

args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(1),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    'time_zone': 'Europe/Berlin',
    'schedule': '@once',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=args,
    schedule_interval= '@once',
    dag_id = 'etl_news_pipeline',
    description="A simple news ETL pipeline using Python,PostgreSQL and Apache Airflow"
) as dag:
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
    )
    
    create_table = PostgresOperator(
        sql=create_table_sql_task,
        task_id='create_table',
        postgres_conn_id='postgres_connection'      
    )
  
    etl = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
    )

    ## TODO: Define task dependencies
    #start_pipeline >> create_table >> etl >> clean_table >> end_pipeline
    start_pipeline >> create_table >> etl >> end_pipeline