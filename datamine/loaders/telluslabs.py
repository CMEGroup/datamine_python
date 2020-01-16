from . import Loader

import pandas as pd

class TellusLabsLoader(Loader):
    dataset = 'TELLUSLABS'
    fileglob = 'TELLUSLABS_*.csv'
    index = 'metric_date'
    columns = ['crop', 'country_iso', 'geo_level', 'geo_id',
               'geo_display_name', 'metric_date',
               'value', 'measure']
    dtypes = {'category': ('crop', 'country_iso', 'geo_level',
                           'geo_display_name', 'measure'),
              'int64': ('geo_id',),
              'float': ('value',),
              'date:%Y-%m-%d': ('metric_date',)}

    def _load(self, file):
        # Assumption: the header from the value column provides
        # the name of the measure for that CSV file.
        df = pd.read_csv(file, low_memory=False)
        df['measure'] = df.columns[-1]
        return df

tellusLabsLoader = TellusLabsLoader()
