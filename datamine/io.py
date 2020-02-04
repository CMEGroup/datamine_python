"""
Simple Python client for CME Group Datamine

https://datamine.cmegroup.com

.. moduleauthor:: Aaron Walters <aaron.walters@cmegroup.com>

"""

import requests
import urllib3
import cgi
import os
import sys
from datetime import datetime
import logging

# Generate logger
logging.basicConfig(filename='datamine.log', filemode='w', format='%(levelname)s - %(asctime)s - %(message)s', level=logging.ERROR)

from .utils import tqdm_execute_tasks, MAX_WORKERS, logger
from .loaders import Loader

DEFAULT_URL = 'https://datamine.cmegroup.com/cme/api/v1'
NO_LIMIT = sys.maxsize
TIMEOUTS = (3.05, 60)
PAGE_SIZE = 1000
CHUNK_SIZE = 1024


def _url_params(url):
    parts = url.split('?', 1)
    if len(parts) == 1:
        return parts[0], None
    return parts[0], dict(map(lambda x: x.split('=', 1), parts[1].split('&')))

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
                 url=DEFAULT_URL, threads=MAX_WORKERS):
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

        # Leverage basic request/urllib3 functionality as much as possible:
        # Persistent sessions, connection pooling, retry management
        self.session = requests.Session()
        self.session.auth = requests.auth.HTTPBasicAuth(username, password)
        retry = urllib3.util.Retry(read=3, backoff_factor=2, status_forcelist=[400])
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        self.session.mount('', adapter)

        self.path = path
        self.data_catalog = {}
        self._dataset = None
        self._limit = -1
        self.threads = threads

    def _call_api(self, endpoint, params, stream=False):
        url = self.url + '/' + endpoint
        param_str = '&'.join('{}={}'.format(*p) for p in params.items())
        logger.debug('_call_api: {}'.format(param_str))
        return self.session.get(url, timeout=TIMEOUTS, params=params, stream=stream)

    def download_file(self, fid):
        """Download a single file denoted by the given FID.

           :type fid: string
           :param fid: The FID of the file to be retrieved.
        """

        if fid not in self.data_catalog:
            raise RequestError('FID not found in the catalog: {}'.format(fid))
        record = self.data_catalog[fid]
        supplied_url, params = _url_params(record['url'])
        assert supplied_url == self.url + '/download'
        response = self._call_api('download', params, stream=True)
        try:
            # The filename is embedded in the Content-Disposition header
            header = response.headers.get('content-disposition', '')
            try:
                filename = cgi.parse_header(header)[1]['filename']
            except Exception:
                filename = 'error.txt'
                print ('''File Handling Area, looking for Content-Disposition Header and Lacks a 'header'...''')
                print('Expected a "filename" entry in the Content-Disposition header found:\n  {}'.format(header))
                print('See log file for further detail.')
                logging.error(str(record['dataset']) + ' ' + str(supplied_url) + ' ' + ' ' + str(params) + ' ' + ('Expected a "filename" entry in the Content-Disposition header found:\n  {}'.format(header)))
                pass
                             
            dest_path = os.path.join(self.path, record['dataset'])
            if not os.path.exists(dest_path):
                try:
                    os.makedirs(dest_path)
                except:
                    pass
            abs_path = os.path.join(dest_path, os.path.basename(filename))
            with open(abs_path, 'wb') as target:
                try:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            target.write(chunk)
                            target.flush()
                except:
                    pass
        finally:
            # It would be more convenient to use the context manager idiom,
            # but avoiding it allows us to support older versions of requests.
            response.close()

    def download_data(self, dataset=None):
        """Download the entire catalog or a specific dataset to the local directory.

        :type dataset: string, or None
        :param dataset: The specific CME Datamine dataset name as retreived from catalog.
                        If None, the entire catalog is downloaded.
        """

        fids = [fid for fid, record in self.data_catalog.items()
                if dataset is None or record['dataset'] == dataset]
        description = 'downloading {} data'.format(dataset if dataset else 'all datasets')
        tqdm_execute_tasks(self.download_file, fids, description, self.threads, mode='thread')

    def get_catalog(self, dataset=None, limit=None, refresh=False):
        """Get the list of data files avaliable to you
        This may take time depending upon how many items are currenty
        have available to your login.  Items are retrieved in groups of 1000
        per the standard call support.

        Parameters
        ----------
        :type dataset: string
        :param dataset: The specific dataset items that you would like to retrieve.

        :type limit: integer
        :param limit: Limits the amount of catalog items you would like to retrieve.

        :type refresh: bool
        :param refresh: Set to True if you want to force a refresh of the local copy.

        Creates
        -------
        :creates: python.dictionary self.data_catalog -- containing custom data catalog available.

        Returns
        -------
        Returns None -- dictionary of the data catalog from Datamine
        """
        logger.info('get_catalog: retrieving {}, limit {}'.format(dataset if dataset else 'all datasets', limit))

        # No need to download more data if:
        #   -- if the dataset matches, and the new limit is smaller
        #   -- if the previous dataset was None, and there was no limit
        if limit is None:
            limit = NO_LIMIT
        elif not isinstance(limit, int) or limit < 0:
            raise RequestError('Invalid limit value: {!r}'.format(limit))
        is_valid = (self._dataset == dataset and limit <= self._limit or
                    self._dataset is None and self._limit == NO_LIMIT)

        if refresh or not is_valid:
            if self._limit >= 0:
                reason = 'by request' if refresh else 'for new parameters'
                logger.debug('get_catalog: refreshing {}'.format(reason))
            self.data_catalog = {}
            self._dataset = None
            self._limit = 0
            is_valid = False

        if is_valid:
            logger.info('get_catalog: requested data already downloaded')
            return

        params = {}
        duplicates = 0
        nrecs = len(self.data_catalog)
        if dataset:
            params['dataset'] = dataset
        while True:
            params['limit'] = min(PAGE_SIZE, limit - nrecs)
            if params['limit'] <= 0:
                logger.warning('get_catalog: {}-record limit reached'.format(limit))
                break

            resp = self._call_api('list', params)
            if resp.text == '"Could not initiate UNO connection"':
                raise RequestError('Invalid username/password combination.')
            try:
                response = resp.json()
                if response is None:
                    logger.warning('get_catalog: empty record obtained, assuming end of data reached')
                    limit = NO_LIMIT
                    break
                files = response['files']
                next_url = response['paging']['next']
            except (ValueError, TypeError):
                raise RequestError('Invalid JSON data:\n   URL: {}\n  Text: {}\n'.format(resp.url, resp.text))

            self.data_catalog.update((item['fid'], item) for item in files)
            orecs, nrecs = nrecs, len(self.data_catalog)
            duplicates += orecs + len(files) - nrecs

            if not next_url:
                logger.debug('get_catalog: end of data raeached')
                limit = NO_LIMIT
                break
            _, params = _url_params(next_url)

        logger.info('get_catalog: {} records downloaded, {} duplicates, {} saved'.format(nrecs + duplicates, duplicates, nrecs))
        self._limit = max(limit, len(self.data_catalog))
        self._dataset = dataset

    def load_dataset(self, dataset, download=True, limit=None, dataset_args = {}):
        """Load a dataset, optionally downloading files listed in the catalog.
           Parameters
           ----------
           :param download: Attempt to download any data avaliable before loading data from local disk.
           :type download: bool

           :param limit: Limit the number of files loaded to the given number.
           :type limit: integer, or None

           Returns
           -------
           :returns: pandas.DataFrame
        """
        
        if download:
            self.download_data(dataset)

        path = os.path.join(self.path, dataset)
        return Loader.by_name(dataset, dataset_args).load(path, limit=limit)

    '''
    Script consists of "load" and "download" functions.
    "download" functions only download files into local directory
    "load" functions download files into local directory, and then read + structure into a pandas DataFrame

    Design pattern for _download family
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
        Parameters
        ----------
        :param download: Attempt to download any data available before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: None

        Returns
        -------
        :returns:  None
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Design pattern for _load family
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
        Parameters
        ----------
        :param download: Attempt to download any data avaliable before loading data from local disk.
        :type download: bool.

        Creates
        -------
        :creates: pandas.DataFrame object.datasetname_DF

        Returns
        -------
        :returns:  None
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    '''

    def block_load(self, download=True):
        """
        Data Set - Block Trades
        File Path - /BLOCK
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Block+Trades
        """
        self.block_DF = self.load_dataset('BLOCK')
        

    def brokertech_tob_download(self, download=True):
        """
        Data Set - Nex BrokerTech Top of Book Data Sets
        File Path - /NEXBROKERTECTOB
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        """
        if download:
            self.download_data('NEXBROKERTECTOB')

    def brokertech_dob_download(self, download=True):
        """
        Data Set - Nex BrokerTech Depth of Book Data Sets
        File Path - /NEXBROKERTECDOB
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        """
        if download:
            self.download_data('NEXBROKERTECDOB')

    def brokertech_fob_download(self, download=True):
        """
        Data Set - Nex BrokerTech Full Book Data Sets
        File Path - /NEXBROKERTECFOB
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/NEX+-+BrokerTec+Historical+Data
        """
        if download:
            self.download_data('NEXBROKERTECFOB')
        
    def crypto_load(self, download=True):
        """
        Data Set - Crypto Data, Bitcoin & Etherium
        File Path - /cryptocurrency
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Liquidity+Tool+Dataset
        """
        self.crypto_DF = self.load_dataset('CRYPTOCURRENCY')
    
    def eod_load(self, download=True):
        """
        Data Set - End of Day Complete
        File Path - /EOD
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/End+of+Day
        """
        self.eod_DF = self.load_dataset('EOD', download=download)

    def voi_load(self, download=True):
        """
        Data Set - Volume and Open Interest
        File Path - /VOI
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Volume+and+Open+Interest
        """
        self.voi_DF = self.load_dataset('VOI', download=download)

    def eris_load(self, download=True):
        """
        Data Set - Eris PAI
        File Path - /ERIS
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Eris+PAI+Dataset
        """
        self.eris_DF = self.load_dataset('ERIS')

    def fx_load(self, download=True):
        """
        Data Set - FX Premium
        File Path - /FX
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Premium+FX+Feed+Historical+Data
        Warning -- Files are large when uncompressed
        """
        self.fx_DF = self.load_dataset('FX', download=download)
        
#    def govpx_load(self, download=True, dataset_args = {}):
#        """
#        Data Set - GovPX
#        File Path - /GOVPX
#        Function Type - Download & Load
#        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/GovPX+Historical+Data#
#        """
#        self.govpx_DF = self.load_dataset(dataset = 'GOVPX', dataset_args = dataset_args, download=download)

    def govpx_download(self, download=True):
        """
        Data Set - GovPX
        File Path - /GOVPX
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/GovPX+Historical+Data
        """
        if download:
            self.download_data('GOVPX')

    def govpxeod_download(self, download=True):
        """
        Data Set - GovPX End of Day
        File Path - /GOVPXEOD
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/GovPX+End+of+Day+Historical+Data
        """
        if download:
            self.download_data('GOVPXEOD')
            
    def STL_download(self, download=True):
        """
        Data Set - STL INT Settlements
        File Path - /STL
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/STL+INT+Settlements
        """
        if download:
            self.download_data('STL')

    def liqtool_load(self, download=True):
        """
        Data Set - Liquidity Tool
        File Path - /LIQTOOL
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Liquidity+Tool+Dataset
        """
        self.liqtool_DF = self.load_dataset('LIQTOOL')        

    def MD_download(self, download=True):
        """
        Data Set - Market Depth FIX
        File Path - /MD
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Market+Depth
        """
        if download:
            self.download_data('MD')
            
    def RLC_download(self, download=True):
        """
        Data Set - Market Depth RLC
        File Path - /RLC
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Market+Depth
        """
        if download:
            self.download_data('RLC')
            
    def RLCSECDEF_download(self, download=True):
        """
        Data Set - SECDEF RLC
        File Path - /RLCSECDEF
        Function Type - Download Only
        Help URL - 
        """
        if download:
            self.download_data('RLCSECDEF')

    def MBO_download(self, download=True):
        """
        Data Set - MBO FIX
        File Path - /MBO
        Function Type - Download Only
        Help URL - https://wiki.chicago.cme.com/confluence/display/EPICSANDBOX/MBO+FIX
        """
        if download:
            self.download_data('MBO')

    def PCAP_download(self, download=True):
        """
        Data Set - Packet Capture (PCAP)
        File Path - /PCAP
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Packet+Capture+Dataset
        """
        if download:
            self.download_data('PCAP')

    def sofrois_load(self, download=True):
        """
        Data Set - SOFR OIS Index
        File Path - /SOFR
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/market-data/faq-sofr-third-party-data.html
        """
        self.sofrois_DF = self.load_dataset('SOFR', download=download)

    def sofrstriprates_load(self, orient='long', download=True):
        """
        Data Set - SOFR Strip Rates
        File Path - /SOFRSR
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/SOFR+Strip+Rates
        """
        if orient == 'wide':
            
            self.sofrstriprates_DF = self.load_dataset('SOFRSR', 
                                                download=download).pivot(index='businessDate', 
                                                        columns='Description', values='rate').sort_values('businessDate', ascending=False).reset_index()

            self.sofrstriprates_DF['businessDate'] = self.sofrstriprates_DF['businessDate'].dt.date                                          
            self.sofrstriprates_DF.set_index('businessDate', inplace=True)
        elif orient == 'long':
            self.sofrstriprates_DF = self.load_dataset('SOFRSR', download=download)
        else:
            print("Incorrect orientation parameter. Defaulting to long.")
            self.sofrstriprates_DF = self.load_dataset('SOFRSR', download=download)

    def SECDEF_download(self, download=True):
        """
        Data Set - Securities Definition (SECDEF)
        File Path - /SECDEF
        Function Type - Download Only
        Help URL - Not Applicable
        """
        if download:
            self.download_data('SECDEF')

    def time_sales_load(self, download=True):
        """
        Data Set - Time and Sales (TICK)
        File Path - /TICK
        Function Type - Download & Load
        Help URL - 
        """
        self.time_sales_DF = self.load_dataset('TICK')

    def BBO_download(self, download=True):
        """
        Data Set - Top-of-Book (BBO)
        File Path - /BBO
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/confluence/display/EPICSANDBOX/Top+of+Book+-+BBO
        """
        if download:
            self.download_data('BBO')

    def bantix_download(self, download=True):
        """
        Data Set - bantix
        File Path - /BANTIX
        Function Type - Download Only
        Help URL - https://www.cmegroup.com/market-data/quikstrike-via-bantix-technologies.html
        """
        if download:
            self.download_data('BANTIX')

    def JSE_download(self, download=True):
        """
        Data Set - Johannesburg Stock Exchange
        File Path - /JSE
        Function Type - Download Only
        Help URL - 
        """
        if download:
            self.download_data('JSE')

    def orbital_insights_load(self, download=True):
        """
        Data Set - Orbital Insights (https://orbitalinsight.com/)
        File Path - /ORBITALINSIGHT
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/market-data/orbital-insight/faq.html
        """
        self.orbital_insights_DF = self.load_dataset('ORBITALINSIGHT')

    def rsmetrics_load(self, download=True):
        """
        Data Set - RS Metrics
        File Path - /RSMETRICS
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/market-data/rs-metrics/faq-rs-metrics.html
        """
        self.rsmetrics_DF = self.load_dataset('RSMETRICS')

    def tellus_labs_load(self, download=True):
        """
        Data Set - Tellus Labs (https://telluslabs.com)
        File Path - /TELLUSLABS
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/education/articles-and-reports/telluslabs-faq.html
        """
        self.tellus_labs_DF = self.load_dataset('TELLUSLABS')

    def oneqbit_load(self, download=True):
        """
        Data Set - 1Qbit
        File Path - /1QBIT
        Function Type - Download & Load
        Help URL - https://www.cmegroup.com/market-data/faq-1qbit.html
        """
        self.oneqbit_DF = self.load_dataset('1QBIT')
