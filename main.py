from pathlib import Path 

from components.data_feeder.data_feeder import DataFeeder


data_feeder = DataFeeder(
    Path('/home/dmitryberezin/Projects/PredictiveMaintenance/Data/Wind Turbine/')  / 'scada_data.csv',
    120,
    'DateTime',
    'WEC: ava. windspeed',
)

data_feeder.start_feed_data()
