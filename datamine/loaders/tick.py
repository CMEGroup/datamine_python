from . import Loader

import pandas as pd

class TickLoader(Loader):
    dataset = 'TICK'
    fileglob = '*.gz'

    columns = ['trade_date_time', 'trade_date', 'trade_time',
               'trade_sequence_number', 'session_indicator',
               'ticker_symbol', 'future_option_index_indicator', 'contract_delivery_date',
               'trade_quantity', 'strike_price', 'trade_price', 'ask_bid_type',
               'indicative_quote_type', 'market_quote', 'close_open_type',
               'valid_open_exception', 'post_close', 'cancel_code_type',
               'insert_code_type', 'fast_late_indicator', 'cabinet_indicator',
               'book_indicator', 'entry_date', 'exchange_code']

    dtypes = {'category': ('session_indicator', 'ticker_symbol', 'future_option_index_indicator',
                           'close_open_type', 'exchange_code', 'ask_bid_type', 'indicative_quote_type',
                           'valid_open_exception', 'post_close', 'cancel_code_type',
                           'insert_code_type', 'fast_late_indicator', 'cabinet_indicator', 'book_indicator'),
              'int64': ('trade_sequence_number', 'contract_delivery_date', 'trade_quantity'),
              'float': ('strike_price', 'trade_price'),
              'date:%H:%M:%s': ('trade_time'),
              'date:%Y%m%d': ('trade_date', 'entry_date'),
              'date': ('trade_date_time')}

    def _load(self, file):
        df = pd.read_csv(file, header=None, low_memory=False)
        
        # Make trade_date_time the first column
        df.insert(0, -1, df[0].astype(str) + 'T' + df[1].astype(str))
        
        return(df)

tickLoader = TickLoader()
