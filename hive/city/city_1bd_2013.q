SELECT city, state_code, round(avg(price)) as average 
FROM city_avg_list
WHERE num_bed=1 and week_ending_date BETWEEN '2013-01-01' and '2013-12-31' and num_listings > 10
GROUP BY city, state_code
ORDER BY average ASC
LIMIT 10;