create table city_stats (state_code varchar(2), city varchar(64), num_bed int4, week_ending_date DATE, avg_list int4, num_list int4);

load data local infile './avg_city_listings.tsv' into table city_stats fields terminated by '\t' lines terminated by '\n' (state_code, city, num_bed, week_ending_date, avg_list, num_list);
