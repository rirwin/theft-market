# Introduction to Theft Market
============

*Documentation is a work in progress*

Table of Contents:

1. Introduction
2. REST API
3. Operation 
4. Directory descriptions

## Introduction

More visibility into and organization of historical real estate listing data.  I leverage Trulia's API to gather their historic data.  See Trulia's [developer page](http://developer.trulia.com/docs/read/Home) for an overview of their API.

![alt text](img/high_level.png "High level overview of Theft Market")

To bootstrap the data pipeline, Theft Market repeatedly calls Trulia's API to get the list of states, cities, and zipcodes in the US.  It parses the XML responses and puts this information into its Meta Store, a MySQL database.  The [TruliaInfoFetcher](/trulia-fetcher/TruliaInfoFetcher.py) calls getStates, getCitiesInStates, and getZipCodesInState to populate the Meta Store; see Trulia's [information library page](http://developer.trulia.com/docs/read/LocationInfo) for more about these calls.

With information about different geographic areas, Theft Market repeatedly calls Trulia's API to get real estate data about each of the areas, which takes approximately 50,000 API calls.  The [TruliaDataFetcher](/trulia-fetcher/TruliaDataFetcher.py) functions used are getStateStats, getCityStats, and getZipCodeStats; see Trulia's [stats library page](http://developer.trulia.com/docs/read/TruliaStats) for more about these calls. The results of these calls are then split in the pipeline.

![alt text](img/pipeline_details.png "Pipeline details")

The [TruliaDataFetcher](/trulia-fetcher/TruliaDataFetcher.py) uses [HappyBase](http://happybase.readthedocs.org/en/latest/) to put data directly into HBase when it finishes parsing a stats response.  Also, the stats are sent to HDFS using [FluentD](http://www.fluentd.org/) to the WebHDFS port on the HDFS NameNode. FluentD appends each record to a file of records in HDFS, and files are partitioned hourly as currently configured.  Each line of these record file includes a JSON object for each record allowing flexibility in what was parsed out of the XML.

Subsequently, these large files in HDFS are processed by Hadoop Streaming with these Python [map-reduce jobs](https://github.com/rirwin/theft-market/tree/master/map-reduce/python).  This translates the JSON objects to structured, tab-separated files that are used as Hive external tables.  To create the Hive tables, use the create table script; see external table [creation query](hive/city/create_ext_table_city.q) for an example to create a city table. One could also write Pig, Cascading, etc. scripts based on the file structure from nice structure of the Hadoop Streaming processing mentioned above.  Following that, there are a handful of other ad hoc queries in the [hive directory](hive/city/).  This concludes progress on the batch processing, deep-dive analytics layer of Theft Market.

![alt text](img/web_server.png "Web server details")
The user is exposed to a simple REST API for getting statistics about particular geographic areas.  

## REST API

The format is straighforward, the caller passes a dictionary (described below) to a base url corresponding to the query interest; here are the following urls supported:

- /data/city/volume 
- /data/city/average
- /data/zipcode/volume
- /data/zipcode/average

where volume is the aggregation of listings over the time period and average is the average listing price over the time period.  The dictionary passes the remainder of the search parameters.  All dictionaries contains the following key-value pairs:

- start_date (YYYY-MM-DD)
- end_date (YYYY-MM-DD)
- num_bedrooms (positive integer)

For city queries provide:

- state_code (XX)
- city (<city name>) (some browsers may need a %20 inserted for spaces in the city name) 

For zipcode queries provide:

- zipcode (XXXXX)

A full example 


## Operation

1. get data in zip file, unzip
2. run database manager to reset database tables
3. hbase shell to create tables (todo do this in HBaseManager)
2. start "info" crawler to get US metadata
3. start fluentd
4. start thrift server
5. run script to read in unzipped data
6. run mapreduce job to put in nice format
7. Create hive external tables (todo, check zip, state)
7. Hive script to setup external tables

Step 5 enables web api
Step 6 enables hive queries


## Directory description

* conf - Configuration
* trulia-fetcher
