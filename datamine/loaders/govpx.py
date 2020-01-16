from datamine.loaders import Loader

import pandas as pd

class GOVPXLoader(Loader):
        
    dataset = 'GOVPX'
    
    govpx_us_treasury_cols = ['Timestamp','Producer','Record','Ask','AskType','AskYield','Bid','BidType','BidYield','BidYieldChg','CashAskPrice','CashBidPrice','CashMidPrice','Change','Coupon','CUSIP','Description','DollarFlow','High','ICAPVOL','IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice','IndicativeBidYield','IssueDate','Last','LastHitorTake','LastYield','Low','MaturityDate','Mid','MidChg','MidSnapChg','MidYield','MidYldSnapChg','Open','SettlementDate','ShortDescription','TreasuryType','VoiceAskPrice','VoiceAskSize','VoiceAskYield','VoiceBidPrice','VoiceBidSize','VoiceBidYield','VoiceTradeSize','VWAP','VWAP10AM-11AM','VWAP11AM-12PM','VWAP12PM-1PM','VWAP1PM-2PM','VWAP2PM-3PM','VWAP3PM-4PM','VWAP8AM-9AM','VWAP9AM-10AM','VWAY','VWAY10AM-11AM','VWAY11AM-12PM','VWAY12PM-1PM','VWAY1PM-2PM','VWAY2PM-3PM','VWAY3PM-4PM','VWAY8AM-9AM','VWAY9AM-10AM']
    govpx_us_tips_cols = ['Timestamp','Producer','Record','Ask','AskYield','Bid','BidYield','BidYieldChg','BidYieldChg','Coupon','CUSIP','Description','High','ICAPVOL','IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice','IndicativeBidYield','IssueDate','Last','LastHitorTake','LastYield','Low','MaturityDate','Mid','MidChg','MidSnapChg','MidYield','MidYldSnapChg','Open','SettlementDate','ShortDescription','Spread','TreasuryType','VoiceAskPrice','VoiceAskSize','VoiceAskYield','VoiceBidPrice','VoiceBidSize','VoiceBidYield','VoiceTradeSize']
    govpx_us_frn_cols = ['Date','Producer','Record','Ask','AskYield','Bid','BidYield','CashAskPrice','CashBidPrice','CashMidPrice','Coupon','CUSIP','Description','FirstCouponDate','FRNIndexRate','High','IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice','IssueDate','Last','LastHitorTake','LastYield','Low','MaturityDate','Mid','MidSnapChg','MidYield','MidYldSnapChg','ModifiedDuration','Open','PriceDuration','SettlementDate','TreasuryType','VoiceAskPrice','VoiceAskSize','VoiceAskYield','VoiceBidPrice','VoiceBidSize','VoiceBidYield','VoiceTradeSize']
    govpx_us_agencies_cols = ['Timestamp','Producer','Record','AgencySwapSpd','AgencySwapSprdChg','Ask','AskSpread','AskYield','AskYTMSpread','Bid','BidSpread','BidYield','BidYTMSpread','Change','Coupon','CUSIP','Description','IndicativeAskYield','IndicativeAskSpd','IndicativeBidYield','IndicativeBidSpd','IndicativeBidYield','MaturityDate']
            
    govpx_us_treasury_dtypes = {'category': ('Producer','Record','CUSIP','Description','LastHitorTake','ShortDescription',),
              'int64': ('ICAPVOL','TreasuryType','VoiceAskSize','VoiceBidSize',),
              'float': ('Ask','AskType','AskYield',
                        'Bid','BidType','BidYield','BidYieldChg',
                        'CashAskPrice','CashBidPrice','CashMidPrice',
                        'Change','Coupon','DollarFlow','High',
                        'IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice','IndicativeBidYield',
                        'Last', 'LastYield', 'Low',
                        'Mid','MidChg','MidSnapChg','MidYield','MidYldSnapChg',
                        'Open','VoiceAskPrice','VoiceAskYield','VoiceBidPrice','VoiceBidYield','VoiceTradeSize',
                        'VWAP','VWAP10AM-11AM','VWAP11AM-12PM','VWAP12PM-1PM','VWAP1PM-2PM','VWAP2PM-3PM','VWAP3PM-4PM','VWAP8AM-9AM','VWAP9AM-10AM',
                        'VWAY','VWAY10AM-11AM','VWAY11AM-12PM','VWAY12PM-1PM','VWAY1PM-2PM','VWAY2PM-3PM','VWAY3PM-4PM','VWAY8AM-9AM','VWAY9AM-10AM',),
              'date': ('Timestamp','IssueDate','MaturityDate','SettlementDate'),
              }
    
    govpx_us_tips_dtypes = {'category': ('Producer','Record','CUSIP','Description','LastHitorTake','ShortDescription',),
              'int64': ('ICAPVOL','TreasuryType','VoiceAskSize','VoiceBidSize',),
              'float': ('Ask','AskYield',
                        'Bid','BidYield','BidYieldChg',
                        'Coupon','High',
                        'IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice','IndicativeBidYield',
                        'Last', 'LastYield', 'Low',
                        'Mid','MidChg','MidSnapChg','MidYield','MidYldSnapChg',
                        'Open','Spread','VoiceAskPrice','VoiceAskYield','VoiceBidPrice','VoiceBidYield','VoiceTradeSize',),
              'date': ('Timestamp','IssueDate','MaturityDate','SettlementDate'),
              }
    
    govpx_us_frn_dtypes = {'category': ('Producer','Record','CUSIP','Description','LastHitorTake',),
              'int64': ('TreasuryType','VoiceAskSize','VoiceBidSize',),
              'float': ('Ask','AskYield',
                        'Bid','BidYield',
                        'CashAskPrice','CashBidPrice','CashMidPrice',
                        'Coupon','High',
                        'IndicativeAskPrice','IndicativeAskYield','IndicativeBidPrice',
                        'Last', 'LastYield', 'Low',
                        'Mid','MidSnapChg','MidYield','MidYldSnapChg',
                        'Open','VoiceAskPrice','VoiceAskYield','VoiceBidPrice','VoiceBidYield','VoiceTradeSize',
                        'FRNIndexRate','ModifiedDuration','PriceDuration'),
              'date': ('Date', 'IssueDate','MaturityDate','SettlementDate', 'FirstCouponDate'),
              }
    
    govpx_us_agencies_dtypes = {'category': ('Producer','Record','CUSIP','Description',),
              'int64': (),
              'float': ('Ask','AskYield',
                        'Bid','BidYield',
                        'Change','Coupon',
                        'IndicativeAskYield','IndicativeBidYield',
                        'AgencySwapSpd','AgencySwapSprdChg',
                        'AskSpread','AskYTMSpread',
                        'BidSpread','BidYTMSpread',
                        'IndicativeAskSpd','IndicativeBidSpd',),
              'date': ('Timestamp','MaturityDate',),
              }

    if Loader.dataset_args == None:
        print("Specify a dataset for the GovPX loader.")
    else:
        for k, v in Loader.dataset_args.items():
            if k == 'dataset':
                if v == 'treasury':
                    columns = govpx_us_treasury_cols
                    dtypes = govpx_us_treasury_dtypes
                    fileglob = "*_UST_*.csv"
                elif v == 'tips':
                    columns = govpx_us_tips_cols
                    dtypes = govpx_us_tips_dtypes
                    fileglob = "*_TIPS_*.csv"
                elif v == 'frn':
                    columns = govpx_us_frn_cols
                    dtypes = govpx_us_frn_dtypes
                    fileglob = "*_FRN_*.csv"
                elif v == 'agencies':
                    columns = govpx_us_agencies_cols
                    dtypes = govpx_us_agencies_dtypes
                    fileglob = "*_Agencies_*.csv"
            print("Complete reload")
            
    def _load(self, file):
        df = pd.read_csv(file, skiprows=1, header=None, low_memory=False)
        return df

govpxLoader = GOVPXLoader()
