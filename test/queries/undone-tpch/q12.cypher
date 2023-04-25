MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)
WHERE toUpper(li.L_SHIPMODE) in ['MAIL','SHIP']
	and date(li.L_COMMITDATE) < date(li.L_RECEIPTDATE)
	and date(li.L_SHIPDATE) < date(li.L_COMMITDATE)
	and date(li.L_RECEIPTDATE) >= date('1994-01-01')
	and date(li.L_RECEIPTDATE) < date('1995-01-01')
RETURN 
	li.L_SHIPMODE,
    sum(CASE WHEN o.O_ORDERPRIORITY IN ['1-URGENT','2-HIGH']
		THEN 1
		ELSE 0 END) as high_line_count,
    sum(CASE WHEN o.O_ORDERPRIORITY IN ['3-MEDIUM','4-NOT SPECIFIED','5-LOW']
		THEN 1
		ELSE 0 END) as low_line_count
order by li.L_SHIPMODE;