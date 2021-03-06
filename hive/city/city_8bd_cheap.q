SELECT city, state_code, round(avg(price)) as average 
FROM city_avg_list
WHERE num_bed=8 and week_ending_date BETWEEN '2010-01-01' and '2014-12-31'
GROUP BY city, state_code
ORDER BY average DESC
LIMIT 10;