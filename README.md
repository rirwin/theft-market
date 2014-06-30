theft-market
============

Insight Data Engineering


INSTALL

Scripted Install


Manual Install

(1) apt-get/yum packages (apache2, ruby)
(2) pip packages (flask)
(3) extern packages (happybase, td-agent)
(4) MySQL config
(5) Flask/apache configuration


OPERATION

(1) get data in zip file, unzip
(2) start "info" crawler to get US metadata
(3) start fluentd
(4) start thrift server
(5) run script to read in unzipped data
(6) run mapreduce job to put in nice format
(7) Hive script to setup external tables

Step (5) enables web api
Step (6) enables hive queries


