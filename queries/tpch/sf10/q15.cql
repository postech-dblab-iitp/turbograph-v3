MATCH (li:LINEITEM)
WHERE li.L_SHIPDATE >= date('1994-11-01')
	AND li.L_SHIPDATE < date('1995-02-01')
WITH li.L_SUPPKEY as suppkey, sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as total_revenue
WITH max(total_revenue) as max_total_revenue
MATCH (li2:LINEITEM)-[:SUPPLIED_BY]->(s2:SUPPLIER)
WHERE li2.L_SHIPDATE >= date('1994-11-01')
	AND li2.L_SHIPDATE < date('1995-02-01')
WITH
	s2.S_SUPPKEY AS S_SUPPKEY,
	s2.S_NAME AS S_NAME,
	s2.S_ADDRESS AS S_ADDRESS,
	s2.S_PHONE AS S_PHONE,
	max_total_revenue,
	sum(li2.L_EXTENDEDPRICE * (1-li2.L_DISCOUNT)) as total_revenue2
WHERE total_revenue2 = max_total_revenue
RETURN
	S_SUPPKEY,
	S_NAME,
	S_ADDRESS,
	S_PHONE,
	total_revenue2,
	max_total_revenue
ORDER BY S_SUPPKEY;