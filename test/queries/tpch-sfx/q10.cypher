MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n:NATION)
where date(o.O_ORDERDATE) >= date('1993-10-01')
	and date(o.O_ORDERDATE) < date('1994-01-01')
	and li.L_RETURNFLAG = 'R'
return
	c.id,c.C_NAME,
	c.C_ACCTBAL,
	n.N_NAME,
	c.C_ADDRESS,
	c.C_PHONE,
	c.C_COMMENT,
	sum(tofloat(li.L_EXTENDEDPRICE) * (1-tofloat(li.L_DISCOUNT))) as revenue
order by revenue desc
LIMIT 20;