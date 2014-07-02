# Introduction to Theft Market
============

*Documentation is a work in progress*

Table of Contents:

1. Introduction
2. Operation 
3. Directory descriptions


## Introduction

More visibility into and organization of historical real estate listing data.  I leverage Trulia's API to gather their historic data.  See Trulia's [developer page](http://developer.trulia.com/docs/read/Home) for an overview of their API.

![alt text](img/high_level.png "High level overview of Theft Market")

To bootstrap the data pipeline, Theft Market repeatedly calls Trulia's API to get the list of states, cities, and zipcodes in the US.  It parses the XML responses and puts this information into its Meta Store, a MySQL database.  The [TruliaInfoFetcher](/trulia-fetcher/TruliaInfoFetcher.py) calls getStates, getCitiesInStates, and getZipCodesInState to populate the Meta Store; see Trulia's [information library page](http://developer.trulia.com/docs/read/LocationInfo) for more about these "info" calls.

With information about different geographic areas, Theft Market repeatedly calls Trulia's API to get real estate data about each of the areas, which takes around 50000 API calls.  The [TruliaDataFetcher]((/trulia-fetcher/TruliaDataFetcher.py)) functions used are getStateStats, getCityStats, and getZipCodeStats; see Trulia's [stats library page](http://developer.trulia.com/docs/read/TruliaStats) for more about these "data" calls. 

![alt text](img/pipeline_details.png "Pipeline details")

## Operation

1. get data in zip file, unzip
2. start "info" crawler to get US metadata
3. start fluentd
4. start thrift server
5. run script to read in unzipped data
6. run mapreduce job to put in nice format
7. Hive script to setup external tables

Step 5 enables web api
Step 6 enables hive queries


## Directory description

* conf - Configuration
* trulia-fetcher
