import os
from datetime import datetime
from time import sleep

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def create_tables(ti):
    dtype_str_map = {
        'int64': 'INTEGER',
        'float64': 'NUMERIC',
        'object': 'TEXT'
    }

    sensors_info = ti.xcom_pull(task_ids=['task_sensor_data_info'])
    table_name_map = sensors_info[0]['Column_valid']
    value_dtypes_map = sensors_info[0]['Dtype']
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id=os.getenv("RAW_DATA_DB_CONN"))
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        for key, value in table_name_map.items():
            if key == 'DateTime':
                continue
            query = f"""
                DROP TABLE IF EXISTS {value};
                CREATE TABLE {value} (
                    "datetime" TIMESTAMP PRIMARY KEY,
                    "value" {dtype_str_map[value_dtypes_map[key]]}
                );"""
            cursor.execute(query=query)
            
        conn.commit()
        return 0
    
    except:
        raise ConnectionError('Error when creating tables')


def stream_sensor_data(ti):
    import pandas as pd

    sensors_info = ti.xcom_pull(task_ids=['task_sensor_data_info'])
    table_name_map = sensors_info[0]['Column_valid']
    df = pd.read_csv(f'{os.getenv("INPUT_DATA_DIR")}/Wind Turbine/scada_data.csv')
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%m/%d/%Y %H:%M')
    prev_time = None
    time_to_sleep = 0

    prediction_start_date = pd.to_datetime(os.getenv('PREDICTION_START_DATE'), format='%Y.%m.%d')
    speed_coeff = float(os.getenv('SPEED_COEFF'))

    try:
        postgres_hook = PostgresHook(postgres_conn_id=os.getenv("RAW_DATA_DB_CONN"))
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        for i, row in df.iterrows():
            if row['DateTime'] > prediction_start_date:
                sleep(time_to_sleep)

            if prev_time is not None:
                for col in df.columns[1:]:
                    query = f"""
                        INSERT INTO {table_name_map[col]} (datetime, value)
                        VALUES (TIMESTAMP '{prev_time}',{row[col]})
                        ON CONFLICT (datetime) DO UPDATE
                        SET datetime = excluded.datetime
                        ;
                    """
                    cursor.execute(query)
                
                time_to_sleep = (row['DateTime'] - prev_time).total_seconds() / speed_coeff

                conn.commit()
            prev_time = row['DateTime']
    except:
        raise ConnectionError(f'Error when inserting values')


with DAG(
    dag_id='scada_sensor_',
    start_date=datetime(2022, 3, 1),
    schedule_interval='@once',
    catchup=False,
) as scada_dag:
    from source.common import get_sensor_data_info
    task_read = PythonOperator(
        task_id='task_sensor_data_info',
        python_callable=get_sensor_data_info,
        do_xcom_push=True,
    )

    create_task = PythonOperator(
        task_id=f'create_tables_task',
        python_callable=create_tables,
    )

    stream_task = PythonOperator(
        task_id=f'stream_task',
        python_callable=stream_sensor_data,
    )

    
    task_read >> create_task >> stream_task
