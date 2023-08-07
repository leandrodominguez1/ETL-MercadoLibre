from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from operators.PostgresFileOperator import PostgresFileOperator
from tmp import scraper, delete_matches
'''probanado usar scraper en tmp, que es donde deberia ir, tambien moviendo el almacenamiento de el tsv a tmp'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG (
    dag_id='ml-tec',
    start_date= datetime(2023,4,3)
) as dag_1: 
    start = EmptyOperator(
        task_id='start',
    )
    task_1= PostgresOperator(
        task_id= "Crear_tabla",
        postgres_conn_id='postgres',
        sql="""
        create table if not exists tecnica_ml (
            id varchar(30),
            site_id varchar(30),
            title varchar(250),
            price varchar(10),
            sold_quantity varchar(20),
            thumbnail varchar(100),
            created_date varchar(20),
            primary key(id,created_date)
            )
        """
    )
    task_2= PythonOperator(
    task_id='scraping_api',
    python_callable=scraper.scrape_function,
    dag=dag_1
    )
    task_3= PythonOperator(
        task_id='delete-matches',
        python_callable= delete_matches.delete_matches,
        dag=dag_1
    )
    task_4= PostgresFileOperator(
    task_id='loading_data',
    operation='write',
    config={'table_name':'tecnica_ml'},
    dag=dag_1
    )
    end = EmptyOperator(
        task_id='end',
    )

start >> task_1 >> task_2 >> task_3 >> task_4 >> end