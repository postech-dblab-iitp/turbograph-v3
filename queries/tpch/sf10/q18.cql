MATCH (lineitem: LINEITEM)
		WITH lineitem.L_ORDERKEY as l_orderkey, sum(lineitem.L_QUANTITY) AS sum_lquantity
		WHERE sum_lquantity > 312
		MATCH (item:LINEITEM)-[:IS_PART_OF]->(ord: ORDERS)-[:MADE_BY]->(customer: CUSTOMER)
		WHERE item.L_ORDERKEY = l_orderkey
		RETURN
			customer.C_NAME,
			customer.C_CUSTKEY,
			ord.O_ORDERKEY,
			ord.O_ORDERDATE AS orderdate,
			ord.O_TOTALPRICE AS totalprice,
			SUM(item.L_QUANTITY)
		ORDER BY
			totalprice DESC,
			orderdate
		LIMIT 100;