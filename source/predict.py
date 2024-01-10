from datetime import datetime, timedelta
from pathlib import Path
import pickle
from typing import Optional
import os

from psycopg2.extensions import connection
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresHook


class FeaturePreprarer:
    @classmethod
    def prepare_has_missing(cls, start_time, end_time, conn, *args, **kwargs):
        with conn.cursor() as cursor:
            query = 'SELECT datetime, value FROM time WHERE datetime BETWEEN %s AND %s;'
            cursor.execute(query, (start_time, end_time))
            query_res = cursor.fetchall()
        return pd.DataFrame.from_records(query_res, columns=['datetime', 'HasMissing'])


def get_connection() -> Optional[connection]:
    try:
        postgres_hook = PostgresHook(postgres_conn_id=os.getenv("RAW_DATA_DB_CONN"))
        return postgres_hook.get_conn()
    except:
        ConnectionError('Connection error')


def get_prediction_period():
    conn = get_connection()
    if not conn:
        return
    
    with conn.cursor() as cursor:
        q1 = "SELECT MAX(datetime) FROM proba"
        cursor.execute(q1)
        q1_res = cursor.fetchall()

        q2 = "SELECT MAX(datetime) FROM time"
        cursor.execute(q2)
        q2_res = cursor.fetchall()

    start_time = q1_res[0][0]
    end_time = q2_res[0][0]
    if start_time is None:
        start_time = datetime(2000, 1, 1)

    return {
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
    }


def predict(prediction_period):
    from sklearn.ensemble import RandomForestClassifier

    extra_feature_preparers = {
        'HasMissing': FeaturePreprarer.prepare_has_missing,
    }
    
    print(prediction_period)
    prediction_period = eval(prediction_period)

    start_time = datetime.strptime(prediction_period['start_time'], '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(prediction_period['end_time'], '%Y-%m-%d %H:%M:%S')
    if start_time >= end_time:
        return 0

    with open(Path(os.getenv('MODELS_DIR')) / 'rf_model.pickle', 'rb') as file:
        model = pickle.load(file)
        print(model.feature_names_in_)
    
    conn = get_connection()
    if not conn:
        return 0
    
    prediction_df = pd.DataFrame()
    with conn.cursor() as cursor:
        for feature in model.feature_names_in_:
            if feature in extra_feature_preparers:
                func = extra_feature_preparers[feature]
                feature_df = func(start_time, end_time, conn)
            else:
                query = f'SELECT datetime, value FROM {feature} WHERE datetime BETWEEN %s AND %s;'
                cursor.execute(query, (start_time, end_time))
                query_res = cursor.fetchall()
                feature_df = pd.DataFrame.from_records(query_res, columns=['datetime', feature])

            if prediction_df.empty:
                prediction_df = feature_df.copy()
            else:
                prediction_df = prediction_df.merge(feature_df, on='datetime', how='left')

    proba = model.predict_proba(prediction_df[model.feature_names_in_])[:,1]

    with conn.cursor() as cursor:
        args_str = ','.join(cursor.mogrify("(%s,%s)", tpl).decode("utf-8") for tpl in zip(prediction_df['datetime'], proba))
        cursor.execute(
            "INSERT INTO proba (datetime, proba) VALUES " + args_str + "ON CONFLICT (datetime) DO UPDATE SET datetime = excluded.datetime",
        )
        conn.commit()

    return 1
    