from . import Loader

import pandas as pd
import os

class OrbitalInsightLoader(Loader):
    dataset = 'ORBITALINSIGHT'
    fileglob = 'ORBITALINSIGHT_*.csv'
    
    columns = ['storage.capacity.estimate', 'volume.estimate.stderr', 'scaled.estimate.stderr',
                         'total.available.tanks', 'smoothed.estimate', 'sampled.tanks.1w',
                         'sampled.tanks.1d', 'volume.estimate', 'scaled.estimate', 'truth_value_mb',
                         'sampled.tanks', 'date', 'location']

    dtypes = {'category': ('location',),
              'int64': ('sampled.tanks', 'sampled.tanks.1d', 'sampled.tanks.1w', 'total.available.tanks'),
              'float': ('smoothed.estimate', 'storage.capacity.estimate',
                        'truth_value_mb', 'volume.estimate', 'volume.estimate.stderr',
                        'scaled.estimate', 'scaled.estimate.stderr'),
              'date': 'date'}

    def _load(self, file):
        _, location, sublocation, _ = os.path.basename(file).split('_', 3)
        if sublocation != '0':
            location = location + '_' + sublocation
        df = pd.read_csv(file, low_memory=False)
        df['location'] = location
        return df

orbitalInsightLoader = OrbitalInsightLoader()
