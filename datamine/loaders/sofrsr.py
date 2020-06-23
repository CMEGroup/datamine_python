from . import Loader

import pandas as pd
import gzip
import json

class SOFRStripRatesLoader(Loader):
    dataset = 'SOFRSR'
    fileglob = 'SOFRSR_TermRate_Fixings_*.JSON'

    columns = ['rate','transactionTime','businessDate','productCode','securityId','productDescription']

    dtypes = {
        'category': ('productCode', 'productDescription', 'securityId',),
        'float': ('rate',),
        'date:%m-%d-%Y' : ('businessDate'),
        'date:%m-%d-%Y:%H:%M:%S' : ('transactionTime')
             }

    def _load(self, filename):
        result = []
        with open(filename, 'rt', encoding='utf-8') as f:
            for line in f:
                line = json.loads(line)
            result = pd.json_normalize(line['payload'])
            
        return result

SOFRstripratesLoader = SOFRStripRatesLoader()
