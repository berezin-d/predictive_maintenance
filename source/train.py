import os

from airflow.providers.postgres.operators.postgres import PostgresHook

def train_model():
    postgres_hook = PostgresHook(postgres_conn_id=os.getenv("RAW_DATA_DB_CONN"))
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        'SELECT datetime, value FROM ambient_temp ORDER BY datetime;'
    )

    query_res = cursor.fetchall()

    return query_res

def transform_data(df):
    return df