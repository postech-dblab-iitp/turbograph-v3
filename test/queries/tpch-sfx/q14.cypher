MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(:PARTSUPP)-[:COMPOSED_BY]->(part: PART)
WHERE date(item.L_SHIPDATE) >= date('1995-09-01')
	AND date(item.L_SHIPDATE) < date(date('1995-09-01') + duration('P1M'))
RETURN
	100 * SUM(CASE
		WHEN part.P_TYPE =~ '.*PROMO.*'
		THEN item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)
		ELSE 0 END) / SUM(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS PROMO_REVENUE;