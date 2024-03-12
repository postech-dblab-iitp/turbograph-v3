MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(part: PART)
WHERE part.P_BRAND = 'Brand#15' AND part.P_CONTAINER = 'LG CASE'
WITH lineitem._id AS LID, lineitem.L_QUANTITY AS LQUAN, lineitem.L_EXTENDEDPRICE AS LEXT, part
MATCH (item2: LINEITEM)-[:COMPOSED_BY]->(part)
WITH LID, LQUAN, LEXT, avg(item2.L_QUANTITY) as prev_avg_quantity
WITH LQUAN, LEXT, 0.2 * prev_avg_quantity AS avg_quantity
WHERE avg_quantity > LQUAN
WITH sum(LEXT) as SUM_LEXT
RETURN SUM_LEXT / 7.0 as avg_yearly;