create table county_stats (state_code varchar(2), county varchar(64), num_bed int4, week_ending_date DATE, avg_list int4, num_list int4);

load data local infile './avg_county_listings.tsv' into table county_stats fields terminated by '\t' lines terminated by '\n' (state_code, county, num_bed, week_ending_date, avg_list, num_list);
