## Schemas

#### XML

Theft Market's raw XML listing report from Trulia is fetched with both the [TruliaInfoFetcher](trulia-fetcher/TruliaInfoFetcher.py) and the [TruliaDataFetcher](trulia-fetcher/TruliaDataFetcher.py).  
The geographic region info has the format:


```xml
     <LocationInfo>
      <state>CA</state>
      <city>
        <id>00212</id>
        <name>San Francisco</name>
        <longitude>-122.448635</longitude>
        <latitude>37.764744</latitude>
      </city>
      ...       
    </LocationInfo>
```

The listing data has the format:

```xml
       <listingStats>
        <listingStat>
         <weekEndingDate>2007-02-10</weekEndingDate>
         <avgListingValue>
          <subcategory>
           <type>1 Bedroom Properties</type>
           <averageListingPrice>730219</averageListingPrice>
           <numberOfProperties>1095</numberOfProperties>
          </subcategory>
          ...
         </avgListingValue>
       </listingsStat>
       ...
      </listingStats>
```

#### FluentD -> HDFS

The XML about listing data is parsed and sent HDFS through FluentD and the WebHDFS port via this call in the [TruliaDataFetcher (L342)](trulia-fetcher/TruliaDataFetcher.py#L342).  The match rules for FluentD are in [this](conf/fluentd/td-agent.conf) file (note that on installation, this is stored in the '/etc/td-agent' directory).  The resulting schema in HDFS has the format:


```json
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797734,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-08-20","avg_list":329000,"num_list":1}
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797735,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-08-27","avg_list":329000,"num_list":2}
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797735,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-09-03","avg_list":329000,"num_list":1} 
```

#### MySQL 

The XML city meta-data (i.e., the city name, GPS lat/lon) is parsed and put into the MySQL database in the following format.  The [DatabaseManager](common/DatabaseManager.py#L89) retreives the information about the city info for fetching the listing data and response in the [RestCallHandler](server/RestCallHandler.py).

     +------+--------------+------------+------------------+-------------------+
     | id   | city         | state_code | latitude         | longitude         |
     +------+--------------+------------+------------------+-------------------+
     |  212 | Acton        | CA         | 34.4841944990312 | -118.177722496673 |
     |  296 | Adelanto     | CA         | 34.5849837366879 | -117.335851499633 |
     |  394 | Agoura Hills | CA         | 34.1503125002187 | -118.754038000766 |
     +------+--------------+------------+------------------+-------------------+


#### HBase

The XML is parsed and sent to HBase through HappyBase and the Thrift server.  There is are four tables in HBase: state, county, city, and zipcode. Each table has a single column family, 'cf', for the listing data which has columns keyed by the date of the listing stats.  The stats about single row in HBase are aggregated together and sent to HBase in the [TruliaDataFetcher](trulia-fetcher/TruliaDataFetcher.py).  The format of part of a row in HBase is shown below.  The [HBaseManager](common/HBaseManager.py) handles retrieval of data from HBase and formats the response for the [RestCallHandler](server/RestCallHandler.py).

The row's key for the city table:

     3|ca_san_francisco

The row's columns:

     ...
     cf:2014-05-24             timestamp=1403686456225, value={'a': u'1338274', 'n': u'190'}            
     cf:2014-05-31             timestamp=1403686456239, value={'a': u'1348144', 'n': u'185'}            
     cf:2014-06-07             timestamp=1403686456253, value={'a': u'1366076', 'n': u'197'}            
     cf:2014-06-14             timestamp=1403686456268, value={'a': u'1389901', 'n': u'198'}   


#### Redis

Schema version 1

As part of an evaluation of Redis on a single node vs HBase as part of a cluster (not a completely fair comparison), I tried using Redis to see if I could store all the listing data in memory.  Since Redis has no concept of tables, there is an abbreviation in the key prepended to the key above.  Also, having a variable number of columns is possible (i.e., using more complex hashs in Redis), but I wanted to try having the week be part of the key instead to keep it simple.

The key for cities:

     > keys 'CI|3|ca-san_francisco*'
      1) CI|3|ca-san_francisco|2010-07-31
      2) CI|3|ca-san_francisco|2011-01-15
      ...
     
The value represents a single listing (as inputed) from python:

     {'a': 1338274, 'n': 190} 


Schema version 2

I found that the above schema had too high of a footprint.  The new schema leverages Redis hash data structure to reduce the footprint almost by an order of magnitude.

The key for cities:

     > keys 'CI|3|ca-san_francisco'
      1) "CI|3|ca-san_francisco"
     
The value is a hash that is keyed by the listing week
      
     > hkeys 'CI|3|ca-san_francisco'
      1) "2010-07-31"
      2) "2011-01-15"
     ...
     
The value of value with the average and number of listings for that geographic location for that week:

     > hget 'CI|3|ca-san_francisco' '2010-07-31'
      "{'a':1187840,'n':490}"


The aggregation of having similar keys resulted in a significantly lower footprint.


#### Hive 

The files in HDFS are tab-separated.  These files are create from the JSON schema in 2a. via a simple mapreduce job (i.e., [city_cols_mapper.py](map-reduce/python/city_cols_mapper.py)).  The format below enables hive external tables.  The columns shown below are (state code, city, number of bedrooms, week ending date, average price, number of listings).  Example Hive city queries are in the hive [city](hive/city) directory.


        wy	afton	5	2009-08-08	216000	1
        wy	afton	4	2009-08-08	249475	4
        wy	afton	3	2009-08-08	282671	7
        wy	afton	2	2009-08-08	150000	1
        wy	afton	1	2009-08-08	165429	2
     

