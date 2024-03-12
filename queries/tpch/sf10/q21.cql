MATCH 
    (l1:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER), 
    (l1)-[:IS_PART_OF]->(o:ORDERS), 
    (s)-[:SUPP_BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'MOROCCO' AND
    l1.L_RECEIPTDATE > l1.L_COMMITDATE AND
    o.O_ORDERSTATUS = 'F'
    AND EXISTS {
        MATCH 
        (l2:LINEITEM)-[:IS_PART_OF]->(o)
        WHERE s.S_SUPPKEY <> l2.L_SUPPKEY
    }
    AND NOT EXISTS {
        MATCH 
        (l3:LINEITEM)-[:IS_PART_OF]->(o)
        WHERE 
        s.S_SUPPKEY <> l3.L_SUPPKEY and
        l3.L_RECEIPTDATE > l3.L_COMMITDATE
    }
RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
ORDER BY numwait desc, S_NAME
LIMIT 100;