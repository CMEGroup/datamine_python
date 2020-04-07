from . import Loader

import pandas as pd
from datetime import datetime, timedelta
start = datetime(1970, 1, 1)  # Unix epoch start time

class REPOPXLoader(Loader):
    dataset = 'REPOPX'
    fileglob = 'REPOPX_*.csv'

    # columns = ['Timestamp', 'Record', 'Chain', 'Bid', 'Bid815', 'Bid830', 'Bid845',
    #    'Bid900', 'Bid930', 'Bid1000', 'Bid1200', 'Bid10AM', 'Bid12PM',
    #    'BidYield', 'Change', 'Description', 'MaturityDate', 'Open',
    #    'TreasuryType']
    
    dtypes = {'category': ('Record', 'Chain', 'Description', 'MaturityDate', 'TreasuryType'),
              'int64': (),
              'float': (
                  'Bid', 'Bid815', 'Bid830', 'Bid845', 'Bid900',
                  'Bid930', 'Bid1000', 'Bid1200', 'Bid10AM',
                  'Bid12PM', 'BidYield', 'Change', 'Open',
                  ),
              'date': ('Timestamp')}
    
    def _load(self, file):
        df = pd.read_csv(file, low_memory = False)
        return(df)
        
repopxLoader = REPOPXLoader()
