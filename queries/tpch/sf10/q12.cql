MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
WHERE (li.L_SHIPMODE = 'RAIL' OR li.L_SHIPMODE = 'SHIP')
    AND li.L_COMMITDATE < li.L_RECEIPTDATE
    AND li.L_SHIPDATE < li.L_COMMITDATE
    AND li.L_RECEIPTDATE >= date('1995-01-01')
    AND li.L_RECEIPTDATE < date('1996-01-01')
RETURN 
    li.L_SHIPMODE AS l_shipmode,
    sum(CASE WHEN o.O_ORDERPRIORITY = '1-URGENT' OR o.O_ORDERPRIORITY = '2-HIGH'
        THEN 1
        ELSE 0 END) as high_line_count,
    sum(CASE WHEN o.O_ORDERPRIORITY <> '1-URGENT' AND o.O_ORDERPRIORITY <> '2-HIGH'
        THEN 1
        ELSE 0 END) as low_line_count
ORDER BY l_shipmode;