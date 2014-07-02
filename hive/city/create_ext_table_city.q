CREATE EXTERNAL TABLE city_avg_list (
   state_code STRING,
   city STRING,
   num_bed INT,
   week_ending_date DATE,
   price INT,
   num_listings INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as textfile
LOCATION '/data/city/clean/avg_list/';
