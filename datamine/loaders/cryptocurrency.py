from . import Loader

import pandas as pd
import gzip
import json

class CryptocurrencyLoader(Loader):
    dataset = 'CRYPTOCURRENCY'
    fileglob = '*_btcIndexJson.gz'
    index = 'mdEntryDateTime'

    dtypes = {'category': ('mdEntryCode', 'mdEntryType', 'mdUpdateAction',
                           'symbol', 'openCloseSettlFlag'),
              'int64': ('rptSeq',),
              'float': ('netChgPrevDay', 'netPctChg', 'mdEntryPx'),
              'date:%Y%m%d_%H:%M:%S.%f': 'mdEntryDateTime'}

    def _load(self, filename):
        result = []
        with gzip.open(filename, 'rt', encoding='utf-8') as f:
            for line in f:
                line = json.loads(line)
                if 'mdEntries' in line:
                    result.append(line['mdEntries'][0])
        result = pd.DataFrame(result)
        result['mdEntryDateTime'] = result['mdEntryDate'] + '_' + result['mdEntryTime']
        return result.drop(['mdEntryDate', 'mdEntryType'], axis=1)

cryptocurrencyLoader = CryptocurrencyLoader()
