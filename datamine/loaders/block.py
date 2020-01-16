from . import Loader

import pandas as pd
import datetime
import pytz


class BlockLoader(Loader):
    dataset = 'BLOCK'
    fileglob = '*.csv.gz'

    # Column "Product Type 2" has an extra space after the name.
    # columns = ['Trade Datetime', 'Reported Datetime',
    #            'Contract Symbol', 'Product Code', 'Asset Class', 'Market Sector', 'Description ', 'Product Type ', 'Contract Year', 'Contract Month', 'Strike Price', 'Put/Call', 'Exchange Code', 'Trade Price', 'Trade Quantity', 'Trade Source', 'Spread Type', 'Spread Description',
    #            'Contract Symbol 2', 'Product Code 2', 'Asset Class 2', 'Market Sector 2', 'Description 2', 'Product Type 2 ', 'Contract Year 2', 'Contract Month 2', 'Strike Price 2', 'Put/Call 2', 'Exchange Code 2','Trade Price 2', 'Trade Quantity 2',
    #            'Contract Symbol 3', 'Product Code 3', 'Asset Class 3', 'Market Sector 3', 'Description 3', 'Product Type 3 ', 'Contract Year 3', 'Contract Month 3', 'Strike Price 3', 'Put/Call 3', 'Exchange Code 3',
    #            'Contract Symbol 4', 'Product Code 4', 'Asset Class 4', 'Market Sector 4', 'Description 4', 'Product Type 4 ', 'Contract Year 4', 'Contract Month 4', 'Strike Price 4', 'Put/Call 4', 'Exchange Code 4']

    dtypes = {'category': ('Contract Symbol', 'Product Code', 'Asset Class', 'Market Sector', 'Description ', 'Product Type ', 'Put/Call', 'Exchange Code', 'Trade Source', 'Spread Type', 'Spread Description',
                           'Contract Symbol 2', 'Product Code 2', 'Asset Class 2', 'Market Sector 2', 'Description 2', 'Product Type 2 ', 'Put/Call 2', 'Exchange Code 2',
                           'Contract Symbol 3', 'Product Code 3', 'Asset Class 3', 'Market Sector 3', 'Description 3', 'Product Type 3 ', 'Put/Call 3', 'Exchange Code 3',
                           'Contract Symbol 4', 'Product Code 4', 'Asset Class 4', 'Market Sector 4', 'Description 4', 'Product Type 4 ', 'Put/Call 4', 'Exchange Code 4'),
              'int64': ('Contract Year', 'Contract Month',
                        'Contract Year 2', 'Contract Month 2',
                        'Contract Year 3', 'Contract Month 3',
                        'Contract Year 4', 'Contract Month 4',),
              'float': ('Strike Price', 'Trade Price', 'Trade Quantity',
                        'Strike Price 2', 'Trade Price 2', 'Trade Quantity 2',
                        'Strike Price 3',
                        'Strike Price 4'),
              'date': ()}
    
    def _load(self, file):
        df = pd.read_csv(file, low_memory = False)
        
        df['Trade Datetime'] = df['Trade Date'].astype('str') + ' ' + df['Trade Time'].astype('str')
        df['Reported Datetime'] = df['Trade Date'].astype('str') + ' ' + df['Reported Time'].astype('str')
        
        timezone = df['Trade Datetime'].str[-2:]
        if timezone.unique()[0] == "ET":
            sub_string = " ET"
            timezone = pytz.timezone("US/Eastern") 
        elif timezone.unique()[0] == "CT":
            sub_string = " CT"
            timezone = pytz.timezone("US/Central") 
        else:
            pass
            
        df['Trade Datetime'] = df['Trade Datetime'].str.replace(sub_string, "")
        df['Reported Datetime'] = df['Reported Datetime'].str.replace(sub_string, "")
        
        df['Trade Datetime'] = df['Trade Datetime'].apply(datetime.datetime.strptime, args=('%Y%m%d %H:%M:%S',))
        df['Trade Datetime'] = df['Trade Datetime'].apply(timezone.localize)
        df['Reported Datetime'] = df['Reported Datetime'].apply(datetime.datetime.strptime, args=('%Y%m%d %H:%M',))
        df['Reported Datetime'] = df['Reported Datetime'].apply(timezone.localize)

        df = df.drop(['Trade Date', 'Trade Time', 'Reported Time'], axis=1)
        return(df)
        

blockLoader = BlockLoader()
