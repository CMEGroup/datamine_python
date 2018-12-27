"""
Simple Python client for CME Group Datamine

https://datamine.cmegroup.com

.. moduleauthor:: Aaron Walters <aaron.walters@cmegroup.com>

TODO:  Proper Documentation on Functions
TODO: ReadMe Update with examples.
"""

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import pathlib
import gzip
import cgi
import shutil
import os
import json
import multiprocessing
import csv
from pathlib import Path
import tqdm as tqdm

DEFAULT_URL = 'https://datamine.cmegroup.com/cme/api/v1/list'


class RequestError(RuntimeError):
    pass


class DatamineCon(object):
    """
        This class operates with CME Datamine to retrieve your data catalog,
        download specific data onto a specified path, load the data from
        that path, and finally structure data

        Example usage::

        import datamine_io

        datamine = datamine_io.Datamine(user='CHANGE_ME', passwd='CHANGE_ME')
        datamine.getCatalog()
        datamine.loadCrypto()  #for crypto data sets

        datamine.debug = True # turn on debug logging
    """

    debug = False

    def __init__(self, path='./', username=None, password=None,
                 url=DEFAULT_URL, processes=4):
        """creates the variables associated with the class

        :type path: string
        :param path: The URL path where you would files save on your local environment

        :type user: string
        :param user: CME Group Login User Name.  See https://www.cmegroup.com/market-data/datamine-api.html

        :type password: string
        :param password: CME Group APP Password for Datamine Services

        :type url: string
        :param url: The primary URL for CME Datamine API.

        :type url: int
        :param url: The number of threads for downloading files.
        """
        self.url = url
        self.user = username
        self.password = password
        self.path = path
        self.data_catalog = {}
        self.processes = 4

    def _do_call_json(self, url, retry=False):
        """Download catalog helper function from url.

        Returns json response with personal data catalog.

        """
        if self.debug:
            print('do call with url: %s' % (url))

        for attempt in range(1 + bool(retry)):
            try:
                resp = requests.get(self.url, timeout=60,
                                    auth=HTTPBasicAuth(self.user, self.password))
            except Exception:
                if attempt:
                    raise

        if self.debug:
            print('resp code: %s, resp text: %s' % (resp.status_code, resp.text))

        try:
            result = resp.json()
        except Exception as e:
            raise RequestError(
                'Failed to parse JSON response %s. Resp Code: %s. Text: %s' % (e, resp.status_code, resp.text))

        finally:
            resp.connection.close()

        return result

    def _do_call_data(self, package):
        """Download datamine hosted file from url to self.path location

        URL is expected to have a Content-Disposition header telling us what
        filename to use.

        Returns None.

        """


        url = package.get('url')
        path = package.get('path')
        request_param = package.get('params')

        if self.debug:
            print('do GET with url: %s, params: %s' % (url, request_param))

        if path is None:
            path = self.path
        response = requests.get(url, params=request_param, timeout=60,
                                auth=HTTPBasicAuth(self.user, self.password), stream=True)

        if self.debug:
            print('Get Response Return: response code: %s, request params: %s, requested URL: %s,' % (response.status_code, request_param, response.url ))

        if response.status_code != 200:
            raise RequestError('Request Failed: %s. Text: %s' % (response.status_code, response.text))

        #load the returned parameters

        try:
            params = cgi.parse_header(
                response.headers.get('Content-Disposition', ''))[-1]
        except:
            print (
                'No parameter response. Response Code: %s | Response Text: %s | Request URL: %s | Reponse Header/Parameters: %s' %
                (response.status_code, response.text, response.url, response.headers.get('Content-Disposition', '')))


        if 'filename' not in params:
            print ('Could not find a FILENAME in parameter response. Response Code: %s | Response Text: %s | Request URL: %s | Reponse Header/Parameters: %s' %
                               (response.status_code, response.text, response.url, response.headers.get('Content-Disposition', '')))



        try:
            filename = os.path.basename(params['filename'])
            abs_path = os.path.join(path, filename)
            with open(abs_path, 'wb') as target:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, target)
        except Exception as e:
            print (
                'Failed to Download file response %s. Resp Code: %s' % (e, response.status_code))
        finally:
            response.connection.close()

        return None

    def _download_data(self, dataset, fids=[]):
        """Download entire or specific by fids datamine datasets to local directory set by self.path

        :type dataset: string
        :param dataset: The specific CME Datamine dataset name as retreived from catalog.

        :type fids: list of stings
        :param fids: The file ID numbers from self.data_catalog if you want just specific item. Else will pull all avalable.

        Returns None.

        """
        if self.debug:
            print('total Items For Retreval from CME Datamine: %s' % (len(fids)))

        # Create sub directory for files
        pathlib.Path(self.path + dataset).mkdir(parents=True, exist_ok=True)

        # Build List of Fids if not supplied
        if len(fids) == 0:
            try:
                dataCatalogDF = pd.DataFrame.from_dict(self.data_catalog).T
                fids = dataCatalogDF.loc[dataCatalogDF['dataset'] == dataset, 'fid']
                if len(fids) == 0:
                    # No Items to pull
                    print('''No Items for %s in Catalog, Refresh Catalog via .get_catalog()?''')
                    return 1
            except Exception:
                print('Error finding items in catalog. Refresh via .get_catalog()?')
                return 1

        # multithreading
        pool = multiprocessing.Pool(processes=self.processes)

        package = []
        for fid in fids:
            package += [{'url': self.data_catalog[fid]['url'].split('?')[0],
                         'params': {'fid': fid},
                         'path': self.path + dataset}]



        if self.debug:
            print (package)


        pool.map(self._do_call_data, package)


        pool.close()
        pool.join()

        del pool
        return 0

    def _load_files_from_disk(self, file, fileType='CSV', skipHeader=False):
        """Helper function to load files from local disk from self.path
        :type file: string
        :param file: The specific file name

        :type fileType: structureing
        :param fileType: Only support 'CSV' currently

        :type skipHeader: bool
        :type skipHeader: skip the header of the file for processing.

        Returns None.

        """

        if fileType == 'CSV':
            data = []
            # print (Path(file).suffix)
            if (Path(file).suffix) == '.gz':
                with gzip.open(file, "rt", newline="") as f:
                    reader = csv.reader(f)
                    for line in reader:
                        data.append(line)

            if (Path(file).suffix) == '.csv':

                with open(file, 'rt') as csvfile:
                    # print (file)
                    tempData = csv.reader(csvfile,
                                          delimiter=','
                                          )
                    # Workareound for telus lab and Orbital data headers
                    if 'TELLUSLABS' or 'ORBITALINSIGHT' in file:
                        next(tempData, None)
                    for line in tempData:
                        data.append(line)

        return data

    def get_catalog(self, dataset=None, limit=9e10, start=None, refresh=False):
        """Get the list of data files avaliable to you
        This may take time depending upon how many items are currenty
        have avaliable to your login.  Items are retrieved in groups of 1000
        per the standard call support.

        Parameters
        ----------
        :type dataset: string
        :param dataset: The specific dataset items that you would like to retrieve

        :type limit: int64
        :param limit: Limits the amount of catalog items you would like to retrieve.

        :type start: int64
        :param start: The page number to start pulling items from.

        :type refresh: bool
        :param refresh: Set to True if you want to refresh the local copy.

        Creates
        -------
        :creates: python.dictionary self.data_catalog -- containing custom data catalog avaliable.

        eturns
        -------
        Returns None -- dictionary of the data catalog from Datamine
        """

        if refresh:
            try:
                del self._datacatalogresp
                del self.url
                self.url = DEFAULT_URL
            except Exception:
                pass
                self.url = DEFAULT_URL

        try:
            # Test to See if we already have a catalog item to fetch
            if self._datacatalogresp is None:
                pass
        except Exception:
            # this is the case where for first time running of the get catalog
            # support specific data set request
            if dataset is not None:
                self.url = self.url + '?dataset=%s' % dataset
                self._datacatalogresp = self._do_call_json(self.url)

            # no specific dataset set by the user .
            else:
                self._datacatalogresp = self._do_call_json(self.url)
            for item in self._datacatalogresp['files']:
                self.data_catalog[item['fid']] = item

        # Get Paging Items if Greater than 1000 Items
        while self._datacatalogresp['paging']['next'] is not '' and len(self.data_catalog) < limit:
            # print('paging Catalog: %s' % (self._datacatalogresp['paging']['next']))
            print('paging Catalog: %s' % (self._datacatalogresp['paging']['next'].split('&')[-1]))
            self._datacatalogresp = self._do_call_json(self._datacatalogresp['paging']['next'])
            # print (self._datacatalogresp['files'])
            try:
                for item in self._datacatalogresp['files']:
                    self.data_catalog[item['fid']] = item
            # debuging failed responses

            except Exception as e:
                print(e)

                # print (self.datacatalogresp)

                return None
            # todo remove this break when the catalog paging is working again!
            break
        return None

    def crypto_load(self, download=True):
        """This function loads CME Crypto Data -- Bitcoin and Etherium.  This includes
        downloading any data avaliable in your catalog into the
        /cryptocurrency directory of the path variable set upon creating of the
        connection.  It then loads and structures your local data into
        into a pandas DataFrame.
        Data sets avaliable: Bitcoin, Etherium
        See: https://wiki.chicago.cme.com/confluence/display/EPICSANDBOX/24-7+CME+CF+Cryptocurrency+Indices
        Parameters
        ----------
        :param download: Attempt to download any data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.crypto_DF

        Returns
        -------
        :returns:  None
        """

        if download:

            self._download_data('CRYPTOCURRENCY')

            if self.debug:
                print('Download Crypto Data Complete')

        # Load BitCoin Files from Path
        if self.debug:
            print('Loading Cryptro Data')
        crypto_dataDict = []
        for file in tqdm.tqdm(os.listdir(self.path + 'CRYPTOCURRENCY/')):

            if 'btcIndexJson' in file:
                if self.debug:
                    print('loading Cryptocurrency file from: %s' % (self.path + 'CRYPTOCURRENCY/%s' % file))

                with gzip.open(self.path + 'CRYPTOCURRENCY/%s' % file, 'r') as f:
                    for line in f:
                        self.line = line

                        lineDict = json.loads(line.decode('utf-8'))

                        lineDict = lineDict.get('mdEntries', None)
                        if lineDict is not None:
                            lineDict = lineDict[0]
                            crypto_dataDict.append(lineDict)
        del f
        del line

        # Turn into DataFrame
        self.crypto_DF = pd.DataFrame(crypto_dataDict)
        del crypto_dataDict

        # Set Types to save on memory size
        # # Catagoricals
        self.crypto_DF["mdEntryCode"] = self.crypto_DF["mdEntryCode"].astype('category')
        self.crypto_DF["mdEntryType"] = self.crypto_DF["mdEntryType"].astype('category')
        self.crypto_DF["mdUpdateAction"] = self.crypto_DF["mdUpdateAction"].astype('category')
        self.crypto_DF["symbol"] = self.crypto_DF["symbol"].astype('category')
        self.crypto_DF["openCloseSettlFlag"] = self.crypto_DF["openCloseSettlFlag"].astype('category')

        # # Integers
        self.crypto_DF["rptSeq"] = self.crypto_DF["rptSeq"].astype('int64')

        # # floats
        self.crypto_DF["netChgPrevDay"] = self.crypto_DF["netChgPrevDay"].astype('float')
        self.crypto_DF["netPctChg"] = self.crypto_DF["netPctChg"].astype('float')
        self.crypto_DF["mdEntryPx"] = self.crypto_DF["mdEntryPx"].astype('float')

        # Dates and Times from two columns into a single one.  UTC time!
        self.crypto_DF['mdEntryDateTime'] = pd.to_datetime(
            self.crypto_DF['mdEntryDate'] +
            '_' +
            self.crypto_DF['mdEntryTime'],
            format='%Y%m%d_%H:%M:%S.%f',
            utc=True)

        # Set Index
        self.crypto_DF.set_index('mdEntryDateTime', inplace=True)

        del self.crypto_DF['mdEntryDate']
        del self.crypto_DF['mdEntryTime']

        return None

    def MBO_download(self, download=True):
        """This function downloads CME MBO Data.  This
        downloads any data avaliable in your catalog into the
        /MBO directory of the path variable set upon creating of the
        connection.
        No attempt is made to load this data into python given
        the size and unique use case.
        See: https://wiki.chicago.cme.com/confluence/display/EPICSANDBOX/MBO+FIX?focusedCommentId=525291252#comment-525291252
        for specifications and other information about this data set.

        Parameters
        ----------
        :param download: Attempt to download MBO data set.
        :type download: bool.

        Returns
        -------
        :returns:  None

        """
        if download:
            self._download_data('MBO')

        return None

    def tellus_labs_load(self, download=True):
        """This function loads Tellus Labs Data.
        https://telluslabs.com/

        This includes downloading any data avaliable in your catalog into the
        /TELLUSLABS directory of the path variable set upon creating of the
        connection.  It then loads and structures your local data into
        into a pandas DataFrame.
        See: https://www.cmegroup.com/education/articles-and-reports/telluslabs-faq.html
        Parameters
        ----------
        :param download: Attempt to download any data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.tellus_lab_DF

        Returns
        -------
        :returns:  None
        """

        if download:
            self._download_data('TELLUSLABS')

        # Create List of Files on Local Path for loading and structing

        # Load NDVI SATTELNDVI
        files = []
        for file in os.listdir(self.path + 'TELLUSLABS/'):
            if 'TELLUSLABS' and 'NDVI' in file:
                files.append(self.path + 'TELLUSLABS/' + file)

        pool = multiprocessing.Pool(processes=self.processes)
        pool_outputs = pool.map(self._load_files_from_disk, files)
        pool.close()
        pool.join()
        del pool

        names = ['crop', 'country_iso', 'geo_level', 'geo_id',
                 'geo_display_name', 'metric_date', 'SATTELNDVI']

        tellus_labs_NDVI_DF = pd.DataFrame([y for x in pool_outputs for y in x], columns=names)
        tellus_labs_NDVI_DF['measure'] = 'SATTELNDVI'
        tellus_labs_NDVI_DF.rename(columns={'SATTELNDVI': 'value'}, inplace=True)

        # Load CHI TELLUSCHIN
        files = []
        for file in os.listdir(self.path + 'TELLUSLABS/'):
            if 'TELLUSLABS' and 'CHI' in file:
                files.append(self.path + 'TELLUSLABS/' + file)

        pool = multiprocessing.Pool(processes=self.processes)

        pool_outputs = pool.map(self._load_files_from_disk, files)

        pool.close()
        pool.join()
        del pool

        names = ['crop', 'country_iso', 'geo_level', 'geo_id',
                 'geo_display_name', 'metric_date', 'TELLUSCHIN']
        tellus_labs_CHI_DF = pd.DataFrame([y for x in pool_outputs for y in x], columns=names)

        tellus_labs_CHI_DF['measure'] = 'TELLUSCHIN'
        tellus_labs_CHI_DF.rename(columns={'TELLUSCHIN': 'value'}, inplace=True)

        # Join two sets together
        self.tellus_labs_DF = pd.concat([tellus_labs_NDVI_DF, tellus_labs_CHI_DF])

        # Set Types
        # # Catagoricals
        self.tellus_labs_DF["crop"] = self.tellus_labs_DF["crop"].astype('category')
        self.tellus_labs_DF["country_iso"] = self.tellus_labs_DF["country_iso"].astype('category')
        self.tellus_labs_DF["geo_level"] = self.tellus_labs_DF["geo_level"].astype('category')
        self.tellus_labs_DF["geo_display_name"] = self.tellus_labs_DF["geo_display_name"].astype('category')
        self.tellus_labs_DF["measure"] = self.tellus_labs_DF["measure"].astype('category')

        # # Integers
        self.tellus_labs_DF["geo_id"] = self.tellus_labs_DF["geo_id"].astype('int64')

        # # floats
        self.tellus_labs_DF["value"] = self.tellus_labs_DF["value"].astype('float')

        # # Dates
        self.tellus_labs_DF['metric_date'] = pd.to_datetime(
            self.tellus_labs_DF['metric_date'], format='%Y-%m-%d')

        # # Set Index
        self.tellus_labs_DF.set_index('metric_date', inplace=True)

        return None

    def time_sales_load(self, download=True):
        """This function loads time and sales data, often refered to as
         tick data.
        This includes downloading any data avaliable in your catalog into the
        /TICK directory of the path variable set upon creating of the
        connection.  It then loads and structures your local data into
        into a pandas DataFrame.
        Parameters
        ----------
        :param download: Attempt to download any data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.time_sales_DF

        Returns
        -------
        :returns:  None

        """

        if download:
            if self._download_data('TICK') == 0:

                pass
            else:
                print('Error: Downloading Tick Data')
                return 1

        names = ['trade_date', 'trade_time', 'trade_sequence_number', 'session_indicator',
                 'ticker_symbol', 'future_option_index_indicator', 'contract_delivery_date',
                 'trade_quantity', 'strike_price', 'trade_price', 'ask_bid_type',
                 'indicative_quote_type', 'market_quote', 'close_open_type',
                 'valid_open_exception', 'post_close', 'cancel_code_type',
                 'insert_code_type', 'fast_late_indicator', 'cabinet_indicator',
                 'book_indicator', 'entry_date', 'exchange_code']

        files = []
        for file in os.listdir(self.path + 'TICK/'):
            if 'tick' in file:
                files.append(self.path + 'TICK/' + file)

        results = []
        for file in tqdm.tqdm(files):
            results = self._load_files_from_disk(file)

        self.time_sales_DF = pd.DataFrame(results, columns=names)

        del results

        # Set Types per specifications:
        # https://www.cmegroup.com/confluence/display/EPICSANDBOX/Post-Purchase+Information# Post-PurchaseInformation-LayoutGuides
        # # Catagoricals
        self.time_sales_DF["session_indicator"] = self.time_sales_DF["session_indicator"].astype('category')
        self.time_sales_DF["ticker_symbol"] = self.time_sales_DF["ticker_symbol"].astype('category')
        self.time_sales_DF["future_option_index_indicator"] = self.time_sales_DF[
            "future_option_index_indicator"].astype('category')
        self.time_sales_DF["close_open_type"] = self.time_sales_DF["close_open_type"].astype('category')
        self.time_sales_DF["exchange_code"] = self.time_sales_DF["exchange_code"].astype('category')
        self.time_sales_DF["ask_bid_type"] = self.time_sales_DF["ask_bid_type"].astype('category')

        self.time_sales_DF["indicative_quote_type"] = self.time_sales_DF["indicative_quote_type"].astype('category')
        self.time_sales_DF["indicative_quote_type"] = self.time_sales_DF["indicative_quote_type"].astype('category')
        self.time_sales_DF["valid_open_exception"] = self.time_sales_DF["valid_open_exception"].astype('category')
        self.time_sales_DF["post_close"] = self.time_sales_DF["post_close"].astype('category')
        self.time_sales_DF["ask_bid_type"] = self.time_sales_DF["ask_bid_type"].astype('category')

        self.time_sales_DF["cancel_code_type"] = self.time_sales_DF["cancel_code_type"].astype('category')

        self.time_sales_DF["insert_code_type"] = self.time_sales_DF["insert_code_type"].astype('category')
        self.time_sales_DF["fast_late_indicator"] = self.time_sales_DF["fast_late_indicator"].astype('category')
        self.time_sales_DF["cabinet_indicator"] = self.time_sales_DF["cabinet_indicator"].astype('category')
        self.time_sales_DF["book_indicator"] = self.time_sales_DF["book_indicator"].astype('category')

        self.time_sales_DF["book_indicator"] = self.time_sales_DF["book_indicator"].astype('category')
        self.time_sales_DF["book_indicator"] = self.time_sales_DF["book_indicator"].astype('category')

        # # Integers
        self.time_sales_DF["trade_sequence_number"] = self.time_sales_DF["trade_sequence_number"].astype('int64')
        self.time_sales_DF["contract_delivery_date"] = self.time_sales_DF["contract_delivery_date"].astype('int64')
        self.time_sales_DF["trade_quantity"] = self.time_sales_DF["trade_quantity"].astype('int64')
        self.time_sales_DF["entry_date"] = self.time_sales_DF["entry_date"].astype('int64')

        # # floats
        self.time_sales_DF["strike_price"] = self.time_sales_DF["strike_price"].astype('float')
        self.time_sales_DF["trade_price"] = self.time_sales_DF["trade_price"].astype('float')
        self.time_sales_DF["strike_price"] = self.time_sales_DF["strike_price"].astype('float')
        self.time_sales_DF["strike_price"] = self.time_sales_DF["strike_price"].astype('float')

        # Dates and Times from two columns into a single one.
        # UTC time assumption which may not be true for older data sets!

        self.time_sales_DF['trade_date_time'] = pd.to_datetime(
            self.time_sales_DF['trade_date'] +
            '_' +
            self.time_sales_DF['trade_time'],
            format='%Y%m%d_%H:%M:%S',
            utc=True)
        self.time_sales_DF['trade_date'] = pd.to_datetime(
            self.time_sales_DF['trade_date'], format='%Y%m%d')
        # self.time_sales_DF['trade_time'] = pd.to_datetime(
        #        self.time_sales_DF['trade_time'], format='%H:%M:%S')

        return None

    def orbital_insights_load(self, download=True):
        """This function loads Orbital Insights Data.
        https://orbitalinsight.com/

        This includes downloading any data avaliable in your catalog into the
        /ORBITALINSIGHT directory of the path variable set upon creating of the
        connection.  It then loads and structures your local data into
        into a pandas DataFrame.
        SEE: https://www.cmegroup.com/market-data/orbital-insight/faq.html
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.orbital_insights_DF

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('ORBITALINSIGHT') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download Orbital Insights Data Complete')

        files = {}

        # find unique collection of locations
        for file in os.listdir(self.path + 'ORBITALINSIGHT/'):
            if 'ORBITALINSIGHT' in file:
                location = file.split('_')[1]

                sublocation = file.split('_')[2]
                if sublocation == '0':
                    pass
                else:
                    location = location + '_' + sublocation
                files[location] = []

        # collect files into specific location group
        for file in os.listdir(self.path + 'ORBITALINSIGHT/'):
            if 'ORBITALINSIGHT' in file:
                location = file.split('_')[1]
                sublocation = file.split('_')[2]
                if sublocation == '0':
                    pass
                else:
                    location = location + '_' + sublocation
                files[location].append(self.path + 'ORBITALINSIGHT/' + file)

        # Since the file name has the measurement; we need to iterate through
        # these and append the location data accordingly.

        self.results = []
        self.resultsDict = {}

        names = ['date', 'volume_estimate', 'smoothed_estimate',
                 'volume_estimate_stderr', 'storage_capacity_estimate',
                 'total_available_tanks', 'sampled_tanks', 'truth_value_mb']

        self.results = []
        for key in tqdm.tqdm(files):
            pool = multiprocessing.Pool(processes=self.processes)
            pool_outputs = pool.map(self._load_files_from_disk, files[key])

            pool.close()
            pool.join()
            del pool

            tempDF = pd.DataFrame([y for x in pool_outputs for y in x], columns=names)
            tempDF['location'] = key
            tempDF.drop_duplicates(inplace=True)
            self.results.append(tempDF)

        self.orbital_insights_DF = pd.concat(self.results, ignore_index=True)
        # Drop Duplicates
        self.orbital_insights_DF.drop_duplicates(inplace=True)
        self.orbital_insights_DF.reset_index(inplace=True, drop=True)

        # Set Types
        # # Catagoricals
        self.orbital_insights_DF["location"] = self.orbital_insights_DF["location"].astype('category')

        # # Integers
        self.orbital_insights_DF["sampled_tanks"] = self.orbital_insights_DF["sampled_tanks"].astype('int64',
                                                                                                     errors='ignore')
        self.orbital_insights_DF["total_available_tanks"] = self.orbital_insights_DF["total_available_tanks"].astype(
            'int64', errors='ignore')

        # # floats
        self.orbital_insights_DF["smoothed_estimate"] = self.orbital_insights_DF["smoothed_estimate"].astype('float',
                                                                                                             errors='ignore')
        self.orbital_insights_DF["storage_capacity_estimate"] = self.orbital_insights_DF[
            "storage_capacity_estimate"].astype('float', errors='ignore')
        self.orbital_insights_DF["truth_value_mb"] = self.orbital_insights_DF["truth_value_mb"].astype('float',
                                                                                                       errors='ignore')
        self.orbital_insights_DF["volume_estimate"] = self.orbital_insights_DF["volume_estimate"].astype('float',
                                                                                                         errors='ignore')
        self.orbital_insights_DF["volume_estimate_stderr"] = self.orbital_insights_DF["volume_estimate_stderr"].astype(
            'float', errors='ignore')

        # # Dates
        self.orbital_insights_DF['date'] = pd.to_datetime(self.orbital_insights_DF['date'], format='%Y-%m-%d')

        # Set Index
        self.orbital_insights_DF.set_index('date', inplace=True)

        return None

    def eris_load(self, download=True):
        """This function loads Eris Data Sets.

        This includes downloading any data avaliable in your catalog into the
        /ERIS directory of the path variable set upon creating of the
        connection.  It then loads and structures your local data into
        into a pandas DataFrame.
        SEE: https://www.cmegroup.com/confluence/display/EPICSANDBOX/Eris+PAI+Dataset
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.eris_DF

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('ERIS') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download ERIS Data Complete')

        names = ['Symbol', 'FinalSettlementPrice', 'EvaluationDate', 'FirstTradeDate',
                 'ErisPAIDate', 'EffectiveDate', 'CashFlowAlignmentDate', 'MaturityDate', 'NPV (A)',
                 'FixedNPV', 'FloatingNPV', 'Coupon (%)', 'FairCoupon (%)', 'FixedPayment', 'FloatingPayment',
                 'NextFixedPaymentDate', 'NextFixedPaymentAmount', 'PreviousFixingDate', 'PreviousFixingRate',
                 'NextFloatingPaymentDate', 'NextFloatingPaymentAmount', 'NextFixingDate', 'PreviousSettlementDate',
                 'PreviousSettlementPrice', 'PreviousErisPAI', 'FedFundsDate', 'FedFundsRate (%)', 'AccrualDays',
                 'DailyIncrementalErisPAI', 'AccruedCoupons (B)', 'ErisPAI (C)', 'SettlementPrice (100+A+B-C)',
                 'RFQ NPV TickSize ($)', 'Nominal', 'ResetRateDescriptor', 'InterpolationFactor', 'HighTradePrice',
                 'LowTradePrice', 'LastTradePrice', 'DailyContractVolume', 'Tag55(T)', 'Tag65(T)', 'Tag55(T+1)',
                 'Tag65(T+1)', 'LastTradeDate', 'InitialSpeculatorMargin', 'SecondarySpeculatorMargin',
                 'InitialHedgerMargin', 'SecondaryHedgerMargin', 'ExchangeSymbol (EX005)', 'BloombergTicker',
                 'FirstFixingDate', 'Category', 'BenchmarkContractName', 'PV01', 'DV01', 'ShortName',
                 'EffectiveYearMonth']

        # Load Local Files
        files = []
        for file in os.listdir(self.path + 'ERIS/'):
            if 'ERIS' in file:
                files.append(self.path + 'ERIS/' + file)

        results = []
        for file in tqdm.tqdm(files):
            results = self._load_files_from_disk(file)

        self.eris_DF = pd.DataFrame(results, columns=names)

        # Set Types
        # # Catagoricals
        self.eris_DF["Symbol"] = self.eris_DF["Symbol"].astype('category')

        self.eris_DF["ResetRateDescriptor"] = self.eris_DF["ResetRateDescriptor"].astype('category')
        self.eris_DF["ExchangeSymbol (EX005)"] = self.eris_DF["ExchangeSymbol (EX005)"].astype('category')
        self.eris_DF["BloombergTicker"] = self.eris_DF["BloombergTicker"].astype('category')

        # # Integers
        self.eris_DF["AccrualDays"] = self.eris_DF["AccrualDays"].astype('int64', errors='ignore')
        self.eris_DF["EffectiveYearMonth"] = self.eris_DF["EffectiveYearMonth"].astype('int64', errors='ignore')
        self.eris_DF["Nominal"] = self.eris_DF["Nominal"].astype('int64', errors='ignore')

        # # floats
        floatItems = ['FinalSettlementPrice', 'NPV (A)', 'FixedNPV', 'FloatingNPV', 'Coupon (%)',
                      'FairCoupon (%)', 'FixedPayment', 'FloatingPayment', 'NextFixedPaymentAmount',
                      'PreviousFixingRate', 'NextFloatingPaymentAmount', 'PreviousSettlementPrice',
                      'PreviousErisPAI', 'FedFundsRate (%)', 'DailyIncrementalErisPAI', 'AccruedCoupons (B)',
                      'ErisPAI (C)',  'SettlementPrice (100+A+B-C)', 'InterpolationFactor',
                      'HighTradePrice','PV01','DV01'
                      ]
        for floatItem in floatItems:
            self.eris_DF[floatItems] = self.eris_DF[floatItems].astype('float', errors='ignore')

        # # Dates
        dateItems = ['EvaluationDate', 'FirstTradeDate', 'ErisPAIDate',
                     'EffectiveDate', 'CashFlowAlignmentDate', 'MaturityDate', 'NextFixedPaymentDate',
                     'PreviousFixingDate', 'NextFloatingPaymentDate', 'NextFixingDate', 'PreviousSettlementDate',
                     'FedFundsDate','LastTradeDate','FirstFixingDate']
        for dateItem in dateItems:
            self.eris_DF[dateItem] = pd.to_datetime(self.eris_DF[dateItem], format='%m/%d/%Y', errors='ignore')



    def bantix_downloads(self, download=True):
        """This function downloads bantix Data Sets.

        This includes downloading any data avaliable in your catalog into the
        /BANTIX directory of the path variable set upon creating of the
        connection.  Given the various formats; not attempt to load into Pandas is made.
        SEE: https://www.cmegroup.com/market-data/quikstrike-via-bantix-technologies.html
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.bantix_DF

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('BANTIX') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download BANTIX Data Complete')

        return None

    def rsmetrics_load(self, download=True):
        """This function loads RS Metrics metals Data Sets.

            This includes downloading any data avaliable in your catalog into the
            /RSMETRICS directory of the path variable set upon creating of the
            connection.  It then loads and structures your local data into
            into a pandas DataFrame.
            SEE: https://www.cmegroup.com/market-data/rs-metrics/faq-rs-metrics.html
            Parameters
            ----------
            :param download: Attempt to download any
            data avaliable before loading data from local disk.
            :type download: bool.

            Creates
            -------
            :creates: pandas.DataFrame object.rsmetrics_DF

            Returns
            -------
            :returns:  None
            """

        if download:

            if self._download_data('RSMETRICS') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download RSMETRICS Data Complete')

        names = ['Order', 'Ticker', 'Type', 'Full.Name', 'Name', 'Location.Type', 'Smelter.Storage',
                 'Metal.Shape', 'Metal.Type', 'YearMonthDayUTC', 'Address', 'City', 'State', 'Zip',
                 'Country', 'Employee.Cars', 'Containers', 'Trucks', 'Tippers', 'Total.Area.Metal.stocks.m2',
                 'Area.Piles.m2', 'Area.Concentrate.Bags.m2', 'Area.Cathodes.m2', 'Area.Anodes.m2',
                 'Comments', 'Notes', 'Time_Date', 'Time', 'Month', 'Day', 'Year', 'PrePost', 'DOW',
                 'Week.End', 'Region', 'Subregion', 'Latitude', 'Longitude', 'DIRECTORY', 'GMP',
                 'Location', 'Metal', 'YearMonth', 'Tot.Area' ,'Drop']

        # Handle Weekly Subsriptions First
        files = []
        for file in os.listdir(self.path + 'RSMETRICS/'):
            if 'RSMETRICS' and 'WEEKLY' in file:
                files.append(self.path + 'RSMETRICS/' + file)

        results = []
        for file in tqdm.tqdm(files):
            results = self._load_files_from_disk(file)

        self.rsmetrics_DF = pd.DataFrame(results, columns=names)
        del self.rsmetrics_DF['Drop']
        self.rsmetrics_DF['File.Timing'] = 'Weekly'


        # Handle Daily Subsriptions
        files = []
        for file in os.listdir(self.path + 'RSMETRICS/'):
            if 'RSMETRICS' and 'DAILY' in file:
                files.append(self.path + 'RSMETRICS/' + file)

        results = []
        for file in tqdm.tqdm(files):
            results = self._load_files_from_disk(file)
        rsmetrics_df_daily = pd.DataFrame(results, columns=names)
        del rsmetrics_df_daily['Drop']
        rsmetrics_df_daily['File.Timing'] = 'Daily'


        #join daily and weekly
        self.rsmetrics_DF = pd.concat([rsmetrics_df_daily, self.rsmetrics_DF])

        del rsmetrics_df_daily

        # Set Types
        # # Catagoricals
        catagoricalItems = ['Ticker','Type','Full.Name','Name','Location.Type',
                            'Smelter.Storage','Metal.Shape','Metal.Type','Country','PrePost','PrePost',
                            'Location','Metal']
        for catagoricalItem in catagoricalItems:
            self.rsmetrics_DF[catagoricalItem] = self.rsmetrics_DF[catagoricalItem].astype('category')


        # # Integers
        integerItems = ['Employee.Cars','Containers','Trucks','Tippers','Total.Area.Metal.stocks.m2',
                        'Area.Piles.m2','Area.Concentrate.Bags.m2','Area.Cathodes.m2',
                        'Area.Anodes.m2','Tot.Area']
        for integerItem in integerItems:

            self.rsmetrics_DF[integerItem] = self.rsmetrics_DF[integerItem].astype('int64', errors='ignore')

        # # Dates
        self.rsmetrics_DF['Notes'] = pd.to_datetime(self.rsmetrics_DF['Notes'], format='%Y-%m-%d')

        self.rsmetrics_DF['Time_Date'] = pd.to_datetime(self.rsmetrics_DF['Time_Date'], format='%H:%M %m-%d-%Y')

        return None

    def brokertech_tob_download(self, download=True):
        """This function downloads Nex BrokerTech Top of Book Data Sets.

        This includes downloading any data avaliable in your catalog into the
        /NEXBROKERTECTOB, directory of the path variable
        set upon creating of the connection.
        Given the various formats; not attempt to load into Pandas is made.
        SEE: https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: None

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('NEXBROKERTECTOB') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download NEXBROKERTECTOB Data Complete')

        return None


    def brokertech_dob_download(self, download=True):
        """This function downloads Nex BrokerTech Depth of Book Data Sets.

        This includes downloading any data avaliable in your catalog into the
        '/NEXBROKERTECDOB' directory of the path variable
        set upon creating of the connection.
        Given the various formats; not attempt to load into Pandas is made.
        SEE: https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: None

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('NEXBROKERTECDOB') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download NEXBROKERTECDOB Data Complete')

        return None

    def brokertech_fob_download(self, download=True):
        """This function downloads Nex BrokerTech Full Book Data Sets.

        This includes downloading any data avaliable in your catalog into the
        'NEXBROKERTECFOB' directory of the path variable
        set upon creating of the connection.
        Given the various formats; not attempt to load into Pandas is made.
        SEE: https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        Parameters
        ----------
        :param download: Attempt to download any
        data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: None

        Returns
        -------
        :returns:  None
        """

        if download:

            if self._download_data('NEXBROKERTECFOB') == 0:
                pass
            else:
                return 1

            if self.debug:
                print('Download NEXBROKERTECFOB Data Complete')

        return None






