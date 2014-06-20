SELECT state_code, round(avg(price)) as average 
FROM state_avg_list
WHERE num_bed=3
GROUP BY state_code
ORDER BY average DESC;