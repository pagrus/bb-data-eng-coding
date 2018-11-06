.header on
.mode csv

SELECT pa.item_name, 
CAST((neg_a_sum - neg_b_sum) AS REAL)/(SELECT COUNT(*) FROM temp_deltas WHERE diff = 2) AS "Avg change in sales when colder",
CAST((pos_a_sum - pos_b_sum) AS REAL)/(SELECT COUNT(*) FROM temp_deltas WHERE diff = 2) AS "Avg change in sales when warmer"
FROM pos_a_totals pa 
JOIN pos_b_totals pb ON pa.item_name = pb.item_name
JOIN neg_a_totals na ON pa.item_name = na.item_name
JOIN neg_b_totals nb ON pa.item_name = nb.item_name;
