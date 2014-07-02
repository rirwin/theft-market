# Introduction to Theft Market
============

*Documentation is a work in progress*

Table of Contents:

1. Introduction
2. Operation 
3. Directory descriptions


## Introduction

More visibility into and organization of historical real estate listing data

![alt text](https://github.com/rirwin/theft-market/blob/master/img/high_level.png "Logo Title Text 1")


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
