from . import Loader

import glob
import os

class RSMetricsLoader(Loader):
    dataset = 'RSMETRICS'

    names = ['Order', 'Ticker', 'Type', 'Full.Name', 'Name', 'Location.Type', 'Smelter.Storage',
             'Metal.Shape', 'Metal.Type', 'YearMonthDayUTC', 'Address', 'City', 'State', 'Zip',
             'Country', 'Employee.Cars', 'Containers', 'Trucks', 'Tippers', 'Total.Area.Metal.stocks.m2',
             'Area.Piles.m2', 'Area.Concentrate.Bags.m2', 'Area.Cathodes.m2', 'Area.Anodes.m2',
             'Comments', 'Notes', 'Time_Date', 'Time', 'Month', 'Day', 'Year', 'PrePost', 'DOW',
             'Week.End', 'Region', 'Subregion', 'Latitude', 'Longitude', 'DIRECTORY', 'GMP',
             'Location', 'Metal', 'YearMonth', 'Tot.Area', 'Drop']

    dtypes = {'category': ('Ticker', 'Type', 'Full.Name', 'Name', 'Location.Type',
                           'Smelter.Storage', 'Metal.Shape', 'Metal.Type', 'Country', 'PrePost', 'PrePost',
                           'Location', 'Metal'),
              'int64': ('Employee.Cars', 'Containers', 'Trucks', 'Tippers', 'Total.Area.Metal.stocks.m2',
                        'Area.Piles.m2', 'Area.Concentrate.Bags.m2', 'Area.Cathodes.m2',
                        'Area.Anodes.m2', 'Tot.Area'),
              'date:%Y-%m-%d': ('Notes', ),
              'date:%H:%M %m-%d-%Y': ('Time_Date', )}

    # Return the weekly data first, then the daily
    def _glob(self, path):
        base = os.path.join(path, 'RSMETRICS_*')
        return glob.glob(base + '_WEEKLY_*.csv') + glob.glob(base + '_DAILY_*.csv')

rsMetricsLoader = RSMetricsLoader()
