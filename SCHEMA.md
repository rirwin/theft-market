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

2b. The XML is parsed and sent to HBase through HappyBase and the Thrift server.
