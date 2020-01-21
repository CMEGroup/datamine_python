from . import Loader

import pandas as pd

class VOILoader(Loader):
    dataset = 'VOI'
    fileglob = '*.gz'

    columns = ['Trade Date','Exchange Code','Product Code','Product Description',
                'Product Type','Put/Call','Strike Price',
                'Contract Year','Contract Month','Open Interest',
                'Total Volume','Globex Volume','Floor Volume','PNT Volume',
                'Block Volume','DataType']

    dtypes = {'category': ('Exchange Code','Product Code','Product Description',
                'Product Type','Put/Call','Strike Price',
                'Contract Year','Contract Month','DataType'),
              'int64': ('Open Interest',
                'Total Volume','Globex Volume','Floor Volume','PNT Volume',
                'Block Volume'),
              'float': (),
              'date:%Y%m%d:%s': ('Trade Date',),
              }

    def _load(self, file):
        df = pd.read_csv(file, skiprows=1, header=None, low_memory=False)
        
        #Need to extract the timing of the data from the file name.
        if file[-17] == 'p':
            df['DataType'] = 'Preliminary'
        if file[-17] == 'f':
            df['DataType'] = 'Final'
        return df

voiLoader = VOILoader()
