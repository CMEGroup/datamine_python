from . import Loader

import pandas as pd
import os


class OrbitalInsightLoader(Loader):
    dataset = 'ORBITALINSIGHT'
    fileglob = 'ORBITALINSIGHT_*.csv'

    columns = ['date', 'volume_estimate', 'smoothed_estimate',
               'volume_estimate_stderr', 'storage_capacity_estimate',
               'total_available_tanks', 'sampled_tanks', 'truth_value_mb',
               'location']

    dtypes = {'category': ('location',),
              'int64': ('sampled_tanks', 'total_available_tanks'),
              'float': ('smoothed_estimate', 'storage_capacity_estimate',
                        'truth_value_mb', 'volume_estimate', 'volume_estimate_stderr'),
              'date': 'date'}

    def _load(self, file):
        _, location, sublocation, _ = os.path.basename(file).split('_', 3)
        if sublocation != '0':
            location = location + '_' + sublocation
        df = pd.read_csv(file, low_memory=False)
        df['location'] = location
        return df


orbitalInsightLoader = OrbitalInsightLoader()
