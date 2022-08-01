from time import sleep

import pandas as pd


class DataFeeder:
    def __init__(self, path_to_file: str, speed_coef: float, date_col_name: str, value_col_name: str) -> None:
        self._path_to_file = path_to_file
        self._speed_coeff = speed_coef
        self._date_col_name = date_col_name
        self._value_col_name = value_col_name


    def start_feed_data(self):
        data = pd.read_csv(self._path_to_file)
        data[self._date_col_name] = pd.to_datetime(data[self._date_col_name], format='%m/%d/%Y %H:%M')
        data['time_diff'] = data[self._date_col_name].shift(-1) - data[self._date_col_name]

        for i, row in data.iterrows():
            print(row[self._date_col_name], row[self._value_col_name])
            # write to db
            
            time_to_sleep = row['time_diff']
            if pd.isnull(time_to_sleep):
                break

            sleep(time_to_sleep.total_seconds() / self._speed_coeff)
        
        return 1


            

