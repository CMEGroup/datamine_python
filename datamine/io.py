"""
Simple Python client for CME Group Datamine

https://datamine.cmegroup.com

.. moduleauthor:: Aaron Walters <aaron.walters@cmegroup.com>

TODO:  Proper Documentation on Functions
TODO: ReadMe Update with examples.
"""

import requests
import urllib3
import pathlib
import cgi
import os
import sys

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
                raise RequestError('Expected a "filename" entry in the Content-Disposition header found:\n  {}'.format(header))
            dest_path = os.path.join(self.path, record['dataset'])
            pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)
            abs_path = os.path.join(dest_path, os.path.basename(filename))
            with open(abs_path, 'wb') as target:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        target.write(chunk)
                        target.flush()
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
        have avaliable to your login.  Items are retrieved in groups of 1000
        per the standard call support.

        Parameters
        ----------
        :type dataset: string
        :param dataset: The specific dataset items that you would like to retrieve

        :type limit: integer
        :param limit: Limits the amount of catalog items you would like to retrieve.

        :type refresh: bool
        :param refresh: Set to True if you want to force a refresh of the local copy.

        Creates
        -------
        :creates: python.dictionary self.data_catalog -- containing custom data catalog avaliable.

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

    def load_dataset(self, dataset, download=True, limit=None):
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
        return Loader.by_name(dataset).load(path, limit=limit)

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
        self.crypto_DF = self.load_dataset('CRYPTOCURRENCY')

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
            self.download_data('MBO')

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
        self.tellus_labs_DF = self.load_dataset('TELLUSLABS')

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
        self.time_sales_DF = self.load_dataset('TICK')

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
        self.orbital_insights_DF = self.load_dataset('ORBITALINSIGHT')

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
        self.eris_DF = self.load_dataset('ERIS')

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
            self.download_data('BANTIX')

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
        self.rsmetrics_DF = self.load_dataset('RSMETRICS')

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
            self.download_data('NEXBROKERTECTOB')

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
            self.download_data('NEXBROKERTECDOB')

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
            self.download_data('NEXBROKERTECFOB')
