from pathlib import Path 

from components.data_feeder.data_feeder import DataFeeder


data_feeder = DataFeeder(
    Path('../Data/Wind Turbine/')  / 'scada_data.csv',
    120,
    'DateTime',
    ['WEC: ava. windspeed', 'WEC: max. windspeed', 'WEC: ava. Rotation']
)

data_feeder.start_feed_data()
