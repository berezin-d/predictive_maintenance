from datetime import datetime
import logging
import os

from airflow.operators.python import PythonOperator
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from source.train import train_model, transform_data
from source.common import get_sensor_data_info, get_all_sensor_data_from_db, get_status_data, prepare_data_for_model
from source.pandas_over_etl_operator import PandasETLOverPostgresOperator

with DAG(
    dag_id='train_dag',
    start_date=datetime(2022, 3, 1),
    schedule_interval='@daily',
    catchup=False,
) as train_dag:
    task_read_info = PythonOperator(
        task_id='read_data_info',
        python_callable=get_sensor_data_info,
        do_xcom_push=True,
    )   

    extract_sensor_data_task = PythonOperator(
        task_id='extract_sensors_data',
        python_callable=get_all_sensor_data_from_db,
        op_kwargs={'task_ids':['read_data_info']},
        do_xcom_push=True,
    )

    extract_status_data_task = PythonOperator(
        task_id='extract_status_data',
        python_callable=get_status_data,
        do_xcom_push=True,
    )

    prepare_task = PythonOperator(
        task_id='prepare_data_for_model', 
        python_callable=prepare_data_for_model,
        op_kwargs={
            'extract_sensor_data_task_id': 'extract_sensors_data',
            'extract_status_data_task_id': 'extract_status_data',
        },
        do_xcom_push=True,
    )





    # task_pandas_over_etl = PandasETLOverPostgresOperator(
    #     task_id='etl_over_postgres_task',
    #     connection_id=os.getenv("RAW_DATA_DB_CONN"),
    #     sql_query='SELECT datetime, value FROM ambient_temp ORDER BY datetime;',
    #     etl_function=transform_data,
    #     do_xcom_push=True,
    # )
    
    task_read_info >> [extract_sensor_data_task, extract_status_data_task] >> prepare_task

    # task_train = PythonOperator(
    #     task_id='train_model_task',
    #     python_callable=train_model,
    #     do_xcom_push=True,
    # )
    
    # task_train
