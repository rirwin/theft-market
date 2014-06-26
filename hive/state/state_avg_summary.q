CREATE TABLE simple_state_avg_2010_3bd AS 
SELECT state_code, round(avg(price)) as average 
FROM state_avg_list
WHERE num_bed=3 and week_ending_date BETWEEN '2010-01-01' and '2010-12-31'
GROUP BY state_code
ORDER BY average DESC;

CREATE EXTERNAL TABLE hive_test_simple_avg_2010_3bd ( field1 string, field2 string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, a:col1") TBLPROPERTIES ("hbase.table.name" = "hbase_test_simple_avg_2010_3bd");