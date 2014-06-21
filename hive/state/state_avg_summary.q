SELECT state_code, round(avg(price)) as average 
FROM state_avg_list
WHERE num_bed=3 and week_ending_date BETWEEN '2010-01-01' and '2010-12-31'
GROUP BY state_code
ORDER BY average DESC;