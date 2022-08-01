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

        for i, row in data.iterrows():
            

