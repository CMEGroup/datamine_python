from . import Loader

import pandas as pd
from datetime import datetime, timedelta
start = datetime(1970, 1, 1)  # Unix epoch start time

class LiqLoader(Loader):
    dataset = 'LIQTOOL'
    fileglob = 'LIQTOOL_*.csv.gz'
    index = 'tradedate'
    
    dtypes = {'category': ('symbol', 'time_zone'),
              'int64': ('lot_1_size', 'lot_2_size', 'lot_3_size', 'lot_4_size', 'lot_5_size',
                        'lot_6_size', 'lot_7_size', 'lot_8_size', 'lot_9_size', 'lot_10_size',
                        'lot_11_size', 'lot_12_size', 'lot_13_size', 'lot_14_size', 'lot_15_size',
                        'lot_16_size', 'lot_17_size', 'lot_18_size', 'lot_19_size', 'lot_20_size',
                        'lot_21_size', 'lot_22_size', 'lot_23_size', 'lot_24_size', 'lot_25_size', 'frontmonth'),
              'float': ('avg_level_1_spread', 'avg_level_1_midprice', 'avg_level_1_weightedprice', 'avg_level_1_ask_price', 'avg_level_1_bid_price', 'avg_level_1_ask_quantity', 'avg_level_1_bid_quantity', 'avg_level_1_ask_orders', 'avg_level_1_bid_orders',
                        'avg_level_2_spread', 'avg_level_2_midprice', 'avg_level_2_weightedprice', 'avg_level_2_ask_price', 'avg_level_2_bid_price', 'avg_level_2_ask_quantity', 'avg_level_2_bid_quantity', 'avg_level_2_ask_orders', 'avg_level_2_bid_orders',
                        'avg_level_3_spread', 'avg_level_3_midprice', 'avg_level_3_weightedprice', 'avg_level_3_ask_price', 'avg_level_3_bid_price', 'avg_level_3_ask_quantity', 'avg_level_3_bid_quantity', 'avg_level_3_ask_orders', 'avg_level_3_bid_orders',
                        'avg_level_4_spread', 'avg_level_4_midprice', 'avg_level_4_weightedprice', 'avg_level_4_ask_price', 'avg_level_4_bid_price', 'avg_level_4_ask_quantity', 'avg_level_4_bid_quantity', 'avg_level_4_ask_orders', 'avg_level_4_bid_orders',
                        'avg_level_5_spread', 'avg_level_5_midprice', 'avg_level_5_weightedprice', 'avg_level_5_ask_price', 'avg_level_5_bid_price', 'avg_level_5_ask_quantity', 'avg_level_5_bid_quantity', 'avg_level_5_ask_orders', 'avg_level_5_bid_orders',
                        'avg_level_6_spread', 'avg_level_6_midprice', 'avg_level_6_weightedprice', 'avg_level_6_ask_price', 'avg_level_6_bid_price', 'avg_level_6_ask_quantity', 'avg_level_6_bid_quantity', 'avg_level_6_ask_orders', 'avg_level_6_bid_orders',
                        'avg_level_7_spread', 'avg_level_7_midprice', 'avg_level_7_weightedprice', 'avg_level_7_ask_price', 'avg_level_7_bid_price', 'avg_level_7_ask_quantity', 'avg_level_7_bid_quantity', 'avg_level_7_ask_orders', 'avg_level_7_bid_orders',
                        'avg_level_8_spread', 'avg_level_8_midprice', 'avg_level_8_weightedprice', 'avg_level_8_ask_price', 'avg_level_8_bid_price', 'avg_level_8_ask_quantity', 'avg_level_8_bid_quantity', 'avg_level_8_ask_orders', 'avg_level_8_bid_orders',
                        'avg_level_9_spread', 'avg_level_9_midprice', 'avg_level_9_weightedprice', 'avg_level_9_ask_price', 'avg_level_9_bid_price', 'avg_level_9_ask_quantity', 'avg_level_9_bid_quantity', 'avg_level_9_ask_orders', 'avg_level_9_bid_orders',
                        'avg_level_10_spread', 'avg_level_10_midprice', 'avg_level_10_weightedprice', 'avg_level_10_ask_price', 'avg_level_10_bid_price', 'avg_level_10_ask_quantity', 'avg_level_10_bid_quantity', 'avg_level_10_ask_orders', 'avg_level_10_bid_orders',
                        'lot_1_buy_ctt', 'lot_1_sell_ctt', 'lot_1_buy_depth', 'lot_1_sell_depth',
                        'lot_2_buy_ctt', 'lot_2_sell_ctt', 'lot_2_buy_depth', 'lot_2_sell_depth',
                        'lot_3_buy_ctt', 'lot_3_sell_ctt', 'lot_3_buy_depth', 'lot_3_sell_depth',
                        'lot_4_buy_ctt', 'lot_4_sell_ctt', 'lot_4_buy_depth', 'lot_4_sell_depth',
                        'lot_5_buy_ctt', 'lot_5_sell_ctt', 'lot_5_buy_depth', 'lot_5_sell_depth',
                        'lot_6_buy_ctt', 'lot_6_sell_ctt', 'lot_6_buy_depth', 'lot_6_sell_depth',
                        'lot_7_buy_ctt', 'lot_7_sell_ctt', 'lot_7_buy_depth', 'lot_7_sell_depth',
                        'lot_8_buy_ctt', 'lot_8_sell_ctt', 'lot_8_buy_depth', 'lot_8_sell_depth',
                        'lot_9_buy_ctt', 'lot_9_sell_ctt', 'lot_9_buy_depth', 'lot_9_sell_depth',
                        'lot_10_buy_ctt', 'lot_10_sell_ctt', 'lot_10_buy_depth', 'lot_10_sell_depth',
                        'lot_11_buy_ctt', 'lot_11_sell_ctt', 'lot_11_buy_depth', 'lot_11_sell_depth',
                        'lot_12_buy_ctt', 'lot_12_sell_ctt', 'lot_12_buy_depth', 'lot_12_sell_depth',
                        'lot_13_buy_ctt', 'lot_13_sell_ctt', 'lot_13_buy_depth', 'lot_13_sell_depth',
                        'lot_14_buy_ctt', 'lot_14_sell_ctt', 'lot_14_buy_depth', 'lot_14_sell_depth',
                        'lot_15_buy_ctt', 'lot_15_sell_ctt', 'lot_15_buy_depth', 'lot_15_sell_depth',
                        'lot_16_buy_ctt', 'lot_16_sell_ctt', 'lot_16_buy_depth', 'lot_16_sell_depth',
                        'lot_17_buy_ctt', 'lot_17_sell_ctt', 'lot_17_buy_depth', 'lot_17_sell_depth',
                        'lot_18_buy_ctt', 'lot_18_sell_ctt', 'lot_18_buy_depth', 'lot_18_sell_depth',
                        'lot_19_buy_ctt', 'lot_19_sell_ctt', 'lot_19_buy_depth', 'lot_19_sell_depth',
                        'lot_20_buy_ctt', 'lot_20_sell_ctt', 'lot_20_buy_depth', 'lot_20_sell_depth',
                        'lot_21_buy_ctt', 'lot_21_sell_ctt', 'lot_21_buy_depth', 'lot_21_sell_depth',
                        'lot_22_buy_ctt', 'lot_22_sell_ctt', 'lot_22_buy_depth', 'lot_22_sell_depth',
                        'lot_23_buy_ctt', 'lot_23_sell_ctt', 'lot_23_buy_depth', 'lot_23_sell_depth',
                        'lot_24_buy_ctt', 'lot_24_sell_ctt', 'lot_24_buy_depth', 'lot_24_sell_depth',
                        'lot_25_buy_ctt', 'lot_25_sell_ctt', 'lot_25_buy_depth', 'lot_25_sell_depth',),
              'date': ('unixtime',),
              'date:%Y%m%d': ('tradedate',)}
    
    def _load(self, file):
        df = pd.read_csv(file, low_memory = False)
        df['unixtime'] = df['unix_in_sec'].apply(lambda x: start + timedelta(seconds=x))
        df = df.drop(['unix_in_sec'], axis=1)
        return(df)
        
liqLoader = LiqLoader()
