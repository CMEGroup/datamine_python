from . import Loader

import pandas as pd
import numpy as np

class EODLoader(Loader):
    dataset = 'EOD'
    fileglob = '*.gz'

    columns = ['Trade Date','Exchange Code', 'Asset Class', 'Product Code', 'Clearing Code',
       'Product Description', 'Product Type', 'Underlying Product Code',
       'Put/Call', 'Strike Price', 'Contract Year', 'Contract Month',
       'Contract Day', 'Settlement', 'Settlement Cabinet Indicator',
       'Open Interest', 'Total Volume', 'Globex Volume', 'Floor Volume',
       'PNT Volume', 'Block Volume', 'EFP Volume', 'EOO Volume', 'EFR Volume',
       'EFS Volume', 'EFB Volume', 'EFM Volume', 'SUB Volume', 'OPNT Volume',
       'TAS Volume', 'TAS Block Volume', 'TAM Singapore Volume',
       'TAM Singapore Block Volume', 'TAM London Volume',
       'TAM London Block Volume', 'Globex Open Price',
       'Globex Open Price Bid/Ask Indicator',
       'Globex Open Price Cabinet Indicator', 'Globex High Price',
       'Globex High Price Bid/Ask Indicator',
       'Globex High Price Cabinet Indicator', 'Globex Low Price',
       'Globex Low Price Bid/Ask Indicator',
       'Globex Low Price Cabinet Indicator', 'Globex Close Price',
       'Globex Close Price Bid/Ask Indicator',
       'Globex Close Price Cabinet Indicator', 'Floor Open Price',
       'Floor Open Price Bid/Ask Indicator',
       'Floor Open Price Cabinet Indicator', 'Floor Open Second Price',
       'Floor Open Second Price Bid/Ask Indicator', 'Floor High Price',
       'Floor High Price Bid/Ask Indicator',
       'Floor High Price Cabinet Indicator', 'Floor Low Price',
       'Floor Low Price Bid/Ask Indicator',
       'Floor Low Price Cabinet Indicator', 'Floor Close Price',
       'Floor Close Price Bid/Ask Indicator',
       'Floor Close Price Cabinet Indicator', 'Floor Close Second Price',
       'Floor Close Second Price Bid/Ask Indicator', 'Floor Post-Close Price',
       'Floor Post-Close Price Bid/Ask Indicator',
       'Floor Post-Close Second Price',
       'Floor Post-Close Second Price Bid/Ask Indicator', 'Delta',
       'Implied Volatility', 'Last Trade Date', 'TAM (Trade At Marker)']

    dtypes = {'category': ('Settlement Cabinet Indicator', 'Asset Class', 'Product Code', 'Clearing Code',
                  'Product Description', 'Product Type', 'Underlying Product Code',
                  'Put/Call', 'Strike Price', 'Contract Year', 'Contract Month',
                  'Contract Day','Exchange Code','Globex Open Price Bid/Ask Indicator',
                  'Globex Open Price Cabinet Indicator','Globex High Price Bid/Ask Indicator',
                  'Globex High Price Cabinet Indicator','Globex Close Price Bid/Ask Indicator',
                  'Globex Close Price Cabinet Indicator','Floor Open Price Bid/Ask Indicator',
                  'Floor Open Price Cabinet Indicator','Globex Low Price Bid/Ask Indicator',
                  'Globex Low Price Cabinet Indicator', 'Floor Open Second Price Bid/Ask Indicator',
                  'Floor High Price Bid/Ask Indicator',
                  'Floor High Price Cabinet Indicator','Floor Low Price Bid/Ask Indicator',
                  'Floor Low Price Cabinet Indicator','Floor Close Price Bid/Ask Indicator',
                  'Floor Close Price Cabinet Indicator','Floor Post-Close Price Bid/Ask Indicator',
                  'Floor Post-Close Second Price Bid/Ask Indicator','Floor Close Second Price Bid/Ask Indicator', 
                  ),
            'int64': ('Open Interest', 'Total Volume', 'Globex Volume', 'Floor Volume',
                  'PNT Volume', 'Block Volume', 'EFP Volume', 'EOO Volume', 'EFR Volume',
                  'EFS Volume', 'EFB Volume', 'EFM Volume', 'SUB Volume', 'OPNT Volume',
                  'TAS Volume', 'TAS Block Volume', 'TAM Singapore Volume',
                  'TAM Singapore Block Volume', 'TAM London Volume',
                  'TAM London Block Volume'),
            'float': ('Settlement',
                      'Globex Open Price',
                  'Globex High Price',
                  'Globex Low Price',
                  'Globex Close Price',
                  'Floor Open Price',
                  'Floor Open Second Price',
                  'Floor High Price',
                  'Floor Low Price',
                  'Floor Close Price',
                  'Floor Close Second Price',
                  'Floor Post-Close Price',
                  'Floor Post-Close Second Price',
                  'Delta',
                  'Implied Volatility', 'TAM (Trade At Marker)'),
              'date:%Y%m%d': ('Trade Date','Last Trade Date'),
              }

    def _load(self, file):
        df = pd.read_csv(file, skiprows=1, header=None, low_memory=False)
        if len(df.columns) == 70:
            df.insert(len(df.columns), "TAM (Trade At Marker)", float(np.nan))
        return df

eodLoader = EODLoader()
