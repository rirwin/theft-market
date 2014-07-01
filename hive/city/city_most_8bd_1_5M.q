SELECT city, state_code, sum(num_listings) as sum_num_listed, round(avg(price)) as average 
FROM city_avg_list
WHERE num_bed=8 and week_ending_date BETWEEN '2010-01-01' and '2014-12-31' and price < 1500000
GROUP BY city, state_code
ORDER BY sum_num_listed DESC
LIMIT 10;