from datetime import datetime, timedelta
from typing import Optional

from psycopg2.extensions import connection

from airflow.models import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_prediction_period_wrapper(ti):
    from source.predict import get_prediction_period
    return get_prediction_period()


def create_proba_table(ti):
    import os
    from source.predict import get_connection
    
    conn = get_connection()
    if not conn:
        return
    
    query = """
        CREATE TABLE IF NOT EXISTS proba (
            "datetime" TIMESTAMP PRIMARY KEY,
            "proba" NUMERIC
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
        return 1


def predict_wrapper(prediction_period):
    from source.predict import predict
    return predict(prediction_period=prediction_period)
    

with DAG(
    dag_id='predict_proba_',
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as predict_dag:
    
    get_period_task = PythonOperator(
        task_id='get_prediction_period',
        python_callable=get_prediction_period_wrapper,
        do_xcom_push=True,
    )

    create_table_task = PythonOperator(
        task_id='create_proba_table',
        python_callable=create_proba_table,
    )

    predict_task = PythonVirtualenvOperator(
        task_id='predict_proba_rf',
        python_callable=predict_wrapper,
        op_kwargs={'prediction_period': "{{ ti.xcom_pull(task_ids=['get_prediction_period'])[0] }}" },
        requirements=['scikit-learn'],
        system_site_packages=False,
    )

    get_period_task >> create_table_task >> predict_task
