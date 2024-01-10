from datetime import datetime
import os
import logging

from numpy import int16, int64, int8
import numpy as np
import pandas as pd
from airflow.models import TaskInstance
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_sensor_data_info(ti):
    try:
        df = pd.read_csv(f'{os.getenv("INPUT_DATA_DIR")}/Wind Turbine/scada_data.csv')
    except:
        raise FileNotFoundError('File not found')
    
    df_info = pd.DataFrame(index=df.dtypes.index, data={'Column': df.dtypes.index,'Dtype': df.dtypes.astype(str)})
    df_info['Column_valid'] = (df_info['Column'].str.replace('[.:]', '', regex=True)).str.replace(' ', '_').str.lower()
    return df_info[['Column', 'Column_valid', 'Dtype']].to_dict()


def get_status_data(ti):
    try:
        status_df = pd.read_csv(f'{os.getenv("INPUT_DATA_DIR")}/Wind Turbine/status_data.csv')
    except:
        raise FileNotFoundError('File not found')
    
    status_df['Time'] = pd.to_datetime(status_df['Time'], format='%d/%m/%Y %H:%M:%S')
    status_df = status_df.rename(columns={'Time': 'DateTime'})
    status_df['DateTime5s'] = status_df['DateTime'].dt.round(freq='5s')
    status_df['StatusRecordID'] = status_df.index
    status_df['StateDurationMinutes'] = (status_df['DateTime'].shift(-1) - status_df['DateTime']).dt.total_seconds() / 60

    date_range = pd.Series(pd.date_range(start=status_df['DateTime5s'].min(), end=status_df['DateTime5s'].max(), freq='5s'), name='DateTime5s')
    status_df_ts = status_df[['DateTime5s', 'Main Status', 'Sub Status', 'StatusRecordID']]\
        .drop_duplicates(subset=['DateTime5s'], keep='last')\
        .merge(date_range, how='outer', on='DateTime5s')\
        .sort_values('DateTime5s')\
        .ffill()\
        .reset_index(drop=True)
    
    status_df_ts['DateTime5s'] = (status_df_ts['DateTime5s'].view(int64))
    status_df_ts['Main Status'] = status_df_ts['Main Status'].astype(int8)
    status_df_ts['Sub Status'] = status_df_ts['Sub Status'].astype(int8)
    status_df_ts['StatusRecordID'] = status_df_ts['StatusRecordID'].astype(int8)

    df_dir = f'{os.getenv("TEMP_DATA_DIR")}/status_df_ts.csv'
    status_df_ts.to_csv(df_dir, index=False)

    return df_dir


def get_all_sensor_data_from_db(ti:TaskInstance, **kwargs):
    data_info = ti.xcom_pull(task_ids=kwargs['task_ids'])

    data_tables = list(data_info[0]['Column_valid'].values())
    all_data = pd.DataFrame()

    for table_name in data_tables:
        if table_name == 'datetime':
            continue

        postgres_instance = PostgresHook(postgres_conn_id=os.getenv('RAW_DATA_DB_CONN'))
        temp_df = postgres_instance.get_pandas_df(f'SELECT datetime, value FROM {table_name} ORDER BY datetime;')
        temp_df = temp_df.rename(columns={'datetime': 'DateTime', 'value': table_name})
        if all_data.empty:
            all_data = temp_df
        else:
            all_data = all_data.merge(temp_df, on='DateTime', how='outer')

    df_dir = f'{os.getenv("TEMP_DATA_DIR")}/all_sensors_data.csv'
    all_data.sort_values('DateTime').to_csv(df_dir, index=False)

    return df_dir


def prepare_data_for_model(ti: TaskInstance, **kwargs):
    dfs_dirs = ti.xcom_pull(task_ids=[kwargs['extract_sensor_data_task_id'], kwargs['extract_status_data_task_id']])

    # sensors data
    sensors_data = pd.read_csv(dfs_dirs[0])
    sensors_data['DateTime'] = pd.to_datetime(sensors_data['DateTime'])
    sensors_data['DateTimeR'] = sensors_data['DateTime'].dt.round(freq='10min')
    date_range = pd.Series(pd.date_range(start=sensors_data['DateTimeR'].min(), end=sensors_data['DateTimeR'].max(), freq='10min'), name='DateTimeR')

    scada_df_gr = sensors_data.merge(date_range, how='outer', on='DateTimeR').sort_values('DateTimeR')
    scada_df_gr['HasMissing'] = (scada_df_gr[sensors_data.columns[1]].isna()).astype(int)

    date_range_5s = pd.Series(pd.date_range(start=scada_df_gr['DateTimeR'].min(), end=scada_df_gr['DateTimeR'].max(), freq='5s'), name='DateTime5s')
    scada_df_gr = scada_df_gr.merge(date_range_5s, how='outer', left_on='DateTimeR', right_on='DateTime5s').sort_values('DateTime5s').ffill().reset_index(drop=True)
    scada_df_gr.loc[scada_df_gr['HasMissing'] == 1, sensors_data.columns] = np.nan

    # status data
    status_df_ts = pd.read_csv(dfs_dirs[1])
    status_df_ts['DateTime5s'] = pd.to_datetime(status_df_ts['DateTime5s'])

    scada_status_df = scada_df_gr.merge(status_df_ts, on='DateTime5s', how='outer').sort_values('DateTime5s')

    logging.info(sensors_data.head())
    logging.info(status_df_ts.head())
    
    df_dir = f'{os.getenv("TEMP_DATA_DIR")}/scada_status_df.csv'
    scada_status_df.to_csv(df_dir, index=False)

    return df_dir
