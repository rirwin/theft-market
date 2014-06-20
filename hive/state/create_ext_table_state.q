CREATE EXTERNAL TABLE state_avg_list (
   state_code STRING,
   num_bed INT,
   week_ending_date DATE,
   price INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as textfile
LOCATION '/user/ubuntu/state_clean_mapper';