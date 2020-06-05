# CME Datamine
[![Build Status](https://travis-ci.org/CMEGroup/datamine_python.svg?branch=master)](https://travis-ci.org/CMEGroup/datamine_python)
# Overview

CME Datamine is offered via a self-service cloud solution, where you can access more than
450 terabytes of historical data almost instantaneously, using some of the most flexible
data delivery methods available. Extensively back-test strategies using real benchmark
markets that date back as far as the 1970s, to help you gauge profitability and risk.

This python package will support your rapid analysis by supplying a basic framework for
direct iteration with CME Datamine cloud system to accomplish the following tasks.

1. Load your data item catalog which you have subscribed
2. Download your data items to your local machines from the cloud
3. Specific data items automatically structured into a Pandas dataframe from
your local copy.  This includes correct typing and other generic routines to support
your analysis needs.
4. Examples of working with this data in Pandas via a collection of Ipyhon Notebook files.

# Installation

## Conda

The easiest way to install this package is to do so in a
Python environment created with [Anaconda](https://www.anaconda.com/distribution/)
or its minimalist alternative [Miniconda](https://docs.conda.io/en/latest/miniconda.html).
Once this environment is installed and activated, simply run this command:
```
conda install -c cme_group datamine
```

## PyPi

Installation from [PyPi](https://pypi.org/project/datamine/)
```
pip install datamine
```

## From source

To install from source, clone this repository and execute
```
pip install .
```
If you wish to install the package in writable mode for development, do
```
pip install -e .
```

# Example usage

The following sections quickly outline some of the simple methods to access
CME Datamine data. For interactive use, we recommend the use of a
[Jupyter](https://jupyter.org) notebook or the
[JupyterLab](https://jupyterlab.readthedocs.io/en/latest) platform.

## Load My Data Catalog Items

```buildoutcfg
myDatamine = dm.DatamineCon(username='YOUR_CME_APP_ID', password='YOUR_CME_APP_PASSWORD', path='./data/')
#Get My Datamine Data Catalog
myDatamine.get_catalog(limit=1000)
# Review one of the data catalog items as supplied in dict format.  
myDatamine.data_catalog.popitem()


```

## Download Specific Data Products
You can request specific data products.  Current data products supported are as follows.
When requesting your data, you must specify the _dataset_ tag or leave it blank will request
all items in your catalog.  

### CME Data Products

|  Data Set Name                | Data Type     | _dataset_ Tag  |
|---                            |---            |---|
|  CME Time and Sales           | Price         | TICK     |
|  CME  Market Depth MBO        | Price         | MBO  |
|  CME CF Crypto Currency       | Index         | CRYPTOCURRENCY  |
|  BrokerTec Top of Book       | Price         | NEXBROKERTECTOB  |
|  BrokerTec Depth of Book     | Price         | NEXBROKERTECDOB  |
|  BrokerTec Full Book         | Price         | NEXBROKERTECFOB  |
|  Eris PAI                     | Market Analytics | ERIS  |
|  SSTL INT Settlements         | Price         | STL  |



### Third Party Data

|  Data Set Name                | Data Type     | _dataset_ Tag  |
|---                            |---            |---|
|  TellusLabs                   | Alternative - Ags             | TELLUSLABS  |
|  Orbital Insight              | Alternative - Energy          | ORBITALINSIGHT  |
|  Bantix Technologies          | Market Analytics - Options    | BANTIX  |
|  RS Metrics                   | Alternative - Metals          | RSMETRICS  |
|  1 Qbit                    | Market Analytics           | 1QBIT  |


A complete list of data products can be reviewed on [CME Datamine]([https://datamine.cmegroup.com/#t=p&p=cme.dataHome)


Example request for specific Data Sets using the _dataset_ tag.
```buildoutcfg
myDatamine.get_catalog(dataset='CRYPTOCURRENCY', limit=1000)
myDatamine.get_catalog(dataset='TICK', limit=1000)
myDatamine.get_catalog(dataset='TELLUSLABS', limit=1000)
myDatamine.get_catalog(dataset='RSMETRICS', limit=1000)
```

## Use Bitcoin Information in Analysis
The following example can be found in the [Load Datamine Data Locally Example Notebook](https://github.com/CMEGroup/datamine_python/blob/master/examples/Load%20Datamine%20Data%20Locally%20Example.ipynb)
```buildoutcfg
myDatamine.get_catalog(dataset='CRYPTOCURRENCY', limit=1000)
myDatamine.crypto_load()

#plot second interval index values for Bitcoin
indexValue = myDatamine.crypto_DF.loc[myDatamine.crypto_DF['symbol'] =='BRTI','mdEntryPx'].plot(figsize=[15,5]);
plt.title('Historical Bitcoin Intraday Reference Rate')
plt.xlabel('Date')
plt.ylabel('USD/BTC')
plt.style.use('fivethirtyeight')
plt.show()

```
![Bitcoin RT Index Plot Example](https://github.com/CMEGroup/datamine_python/blob/master/examples/images/BitcoinRTIndexValue.png "Bitcoin Logo")


## Questions and Comments?
Please use the Issues feature.


## Notice
The information herein has been complied by CME Group for general informational and education purposes only and does not constitute trading advice or the solicitation of purchases or sale of futures, options, or swaps. The views in this video reflect solely those of the author and not necessarily those of CME Group or its affiliated institutions. All examples discussed are hypothetical situations, used for explanation purposes only, and should not be considered investment advice of the results of actual market experience. Although every attempt has been made to ensure the accuracy of the information herein, CME Group and its affiliates assume no responsibility for any errors or omissions. All data is sourced by CME Group unless otherwise stated. All matters pertaining to rules and specification herein are made subject to and are superseded by official CME, CBOT, NYMEX, and COMEX rules. Current rules should be consulted in all cases concerning contact specifications.

CME Group, the Globe Logo, CME, Globex, E-Mini, CME Direct, CME Datamine and Chicago Mercantile Exchange are trademarks of Chicago Mercantile Exchange Inc.  CBOT is a trademark of the Board of Trade of the City of Chicago, Inc.  NYMEX is a trademark of New York Mercantile Exchange, Inc.  COMEX is a trademark of Commodity Exchange, Inc. All other trademarks are the property of their respective owners.
