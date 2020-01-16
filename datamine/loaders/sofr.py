from . import Loader

import pandas as pd

class SOFROISLoader(Loader):
    dataset = 'SOFR'
    fileglob = 'SOFR_OIS_*.csv'
    columns = ['Trade Date', 'Exchange Code', 'Currency','Commodity Code', 
                'Short Description','Long Description', 'Curve Date', 'Offset', 
                'Discount Factor', 'Forward rate', 'Rate']
    
    
    dtypes = {'category': ('Exchange Code', 'Currency', 'Commodity Code',
                           'Short Description', 'Long Description','Curve Date','Forward rate'),
              'int64': ('Offset',),
              'float': ('Discount Factor','Rate'),
              'date:%Y%m%d': ('Trade Date',)}

    def _load(self, file):
        # Assumption: the header from the value column provides
        # the name of the measure for that CSV file.
        df = pd.read_csv(file, low_memory=False)
        return df

sofroisLoader = SOFROISLoader()
