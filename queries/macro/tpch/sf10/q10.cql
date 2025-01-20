MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n:NATION)
WHERE o.O_ORDERDATE >= date('1994-11-01')
    AND o.O_ORDERDATE < date('1995-02-01')
    AND li.L_RETURNFLAG = 'R'
RETURN
    c.C_CUSTKEY,
    c.C_NAME,
    c.C_ACCTBAL,
    n.N_NAME,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT,
    sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as revenue
ORDER BY 
    revenue desc
LIMIT 20;