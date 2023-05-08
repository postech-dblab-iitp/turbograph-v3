MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(:PARTSUPP)-[:COMPOSED_BY]->(part: PART)
WHERE date(lineitem.L_SHIPDATE) >= date('1995-09-01')
	AND date(lineitem.L_SHIPDATE) < date(date('1995-09-01') + duration('P1M'))
RETURN
	100 * SUM(CASE
		WHEN part.P_TYPE =~ '.*PROMO.*'
		THEN lineitem.L_EXTENDEDPRICE*(1 - lineitem.L_DISCOUNT)
		ELSE 0 END) / SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS PROMO_REVENUE;