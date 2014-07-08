## Schemas

1. The XML listing report from Trulia has the format:

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

2a. The XML is parsed and sent HDFS through FluentD and the WebHDFS port.  The resulting schema in HDFS has the format:


```json
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797734,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-08-20","avg_list":329000,"num_list":1}
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797735,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-08-27","avg_list":329000,"num_list":2}
    2014-07-01T04:59:57Z  hdfs.zipcode.all_list_stats   {"state_code":"MA","ts":1404190797735,"med_list":329000,"zipcode":"01260","num_beds":3,"week_ending_date":"2011-09-03","avg_list":329000,"num_list":1} 
```

2b. The XML is parsed and sent to HBase through HappyBase and the Thrift server.  There is a single column family, 'cf', for the listing data.  The 'cf' column family has its columns keyed by the date of the listing stats.

     ...
     cf:2014-05-24             timestamp=1403686456225, value={'a': u'1338274', 'n': u'190'}            
     cf:2014-05-31             timestamp=1403686456239, value={'a': u'1348144', 'n': u'185'}            
     cf:2014-06-07             timestamp=1403686456253, value={'a': u'1366076', 'n': u'197'}            
     cf:2014-06-14             timestamp=1403686456268, value={'a': u'1389901', 'n': u'198'}   


2c. The XML city meta-data (i.e., the city name, GPS lat/lon) is parsed and put into the MySQL database in the following format:

     +------+--------------+------------+------------------+-------------------+
     | id   | city         | state_code | latitude         | longitude         |
     +------+--------------+------------+------------------+-------------------+
     |  212 | Acton        | CA         | 34.4841944990312 | -118.177722496673 |
     |  296 | Adelanto     | CA         | 34.5849837366879 | -117.335851499633 |
     |  394 | Agoura Hills | CA         | 34.1503125002187 | -118.754038000766 |
     +------+--------------+------------+------------------+-------------------+


3. The files in HDFS are tab-separated.  These files are create from the JSON schema in 2a. via a simple mapreduce job (i.e., [city_cols_mapper.py](map-reduce/python/city_cols_mapper.py)).  The format below enables hive external tables.  The columns are (state code, city, number of bedrooms, week ending date, average price, number of listings):


        wy	afton	5	2009-08-08	216000	1
        wy	afton	4	2009-08-08	249475	4
        wy	afton	3	2009-08-08	282671	7
        wy	afton	2	2009-08-08	150000	1
        wy	afton	1	2009-08-08	165429	2
     

