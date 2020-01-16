from . import Loader

import pandas as pd

class FXLoader(Loader):
    dataset = 'FX'
    fileglob = '*.gz'

    columns = ['Timestamp', 'Pair', 'Ask', 'Bid']

    dtypes = {'category': ('Pair',),
              'int64': (),
              'float': ('Ask','Bid'),
              'date': ('Timestamp',),
              }

    def _load(self, file):
        df = pd.read_csv(file, skiprows=1, header=None, low_memory=False)
        
        return df

fxLoader = FXLoader()
