MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(part: PART)
WHERE (part.P_BRAND = 'Brand#12'
		and part.P_CONTAINER in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
		and lineitem.L_QUANTITY >= 1 and lineitem.L_QUANTITY <= 1 + 10
		and part.P_SIZE > 1 and part.P_SIZE < 5
		and lineitem.L_SHIPMODE in ['AIR', 'AIR REG']
		and lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (part.P_BRAND = 'Brand#23'
		and part.P_CONTAINER in ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
		and lineitem.L_QUANTITY >= 10 and lineitem.L_QUANTITY <= 10 + 10
		and part.P_SIZE > 1 and part.P_SIZE < 10
		and lineitem.L_SHIPMODE in ['AIR', 'AIR REG']
		and lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (part.P_BRAND = 'Brand#34'
		and part.P_CONTAINER in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
		and lineitem.L_QUANTITY >= 20 and lineitem.L_QUANTITY <= 20 + 10
		and part.P_SIZE > 1 and part.P_SIZE < 15
		and lineitem.L_SHIPMODE in ['AIR', 'AIR REG']
		and lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
RETURN
	SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS revenue;