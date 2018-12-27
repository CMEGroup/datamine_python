# CME Datamine

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



# How to Use This Package

The following will clone this repo, including an environment.yml file that will create the 
proper Anaconda environment with all the dependencies and launch the juptyer lab environment.


```
git clone https://github.com/CMEGroup/datamine.git
cd datamine
conda env create
source activate  datamine
juptyer notebook
```

# Example Usages


The following quickly outline some of the simple 


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
|  BrokerTech Top of Book       | Price         | NEXBROKERTECTOB  | 
|  BrokerTech Depth of Book     | Price         | NEXBROKERTECDOB  | 
|  BrokerTech Full Book         | Price         | NEXBROKERTECFOB  | 
|  Eris PAI                     | Market Analytics | ERIS  | 

  

### Third Party Data

|  Data Set Name                | Data Type     | _dataset_ Tag  |
|---                            |---            |---|
|  TellusLabs                   | Alternative - Ags             | TELLUSLABS  | 
|  Orbital Insight              | Alternative - Energy          | ORBITALINSIGHT  | 
|  Bantix Technologies          | Market Analytics - Options    | BANTIX  | 
|  RS Metrics                   | Alternative - Metals          | RSMETRICS  | 


A complete list of data products can be reviewed on [CME Datamine]([https://datamine.cmegroup.com/#t=p&p=cme.dataHome)
 
 
Example request for specific Data Sets using the _dataset_ tag.
```buildoutcfg
myDatamine.get_catalog(dataset='CRYPTOCURRENCY', limit=1000)
myDatamine.get_catalog(dataset='TICK', limit=1000)
myDatamine.get_catalog(dataset='TELLUSLABS', limit=1000)
myDatamine.get_catalog(dataset='RSMETRICS', limit=1000)
```

## Use Bitcoin information in anlaysis
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

