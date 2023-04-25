MATCH (s:SUPPLIER) WHERE s.S_COMMENT =~ '.*Customer.*.*Complaints.*' WITH s, s.id as p_id
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
WHERE p.P_BRAND <> 'Brand#45'
	and NOT (p.P_TYPE =~ '.MEDIUM POLISHED.*')
	and p.P_SIZE in [49,14,23,45,19,3,36,9]
	and NOT ps.PS_SUPPKEY IN p_id
RETURN
	p.P_BRAND,
	p.P_TYPE,
	p.P_SIZE,
	count(distinct ps.PS_SUPPKEY) as supplier_cnt
order by supplier_cnt DESC, p.P_BRAND, p.P_TYPE, p.P_SIZE;