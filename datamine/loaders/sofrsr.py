from . import Loader

import pandas as pd
import gzip
import json

class SOFRStripRatesLoader(Loader):
    dataset = 'SOFRSR'
    fileglob = 'SOFR-Term-Rate-Fixings_*.JSON'

    columns = ['rate','transactionTime','businessDate','productCode', 'securityId','Description']

    dtypes = {'category': ('productCode', 'productDescription', 'securityId',
                           ),
              'int64': (),
              'float': ('rate', 'netPctChg'),
              'date:%m-%d-%Y': ('businessDate',),
              'date': ('transactionTime')}

    def _load(self, filename):
        result = []
        with open(filename, 'rt', encoding='utf-8') as f:
            for line in f:
                line = json.loads(line)
            result = pd.io.json.json_normalize(line['payload'])
        
        return result

SOFRstripratesLoader = SOFRStripRatesLoader()
