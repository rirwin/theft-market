create table zipcode_stats (zipcode varchar(5), num_bed int4, week_ending_date DATE, num_list int4, avg_list int4);

load data local infile './avg_zipcode_listings.tsv' into table state_stats fields terminated by '\t' lines terminated by '\n' (zipcode, num_bed, week_ending_date, num_list, avg_list);
