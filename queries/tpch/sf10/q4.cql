MATCH (l: LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)
		WHERE l.L_COMMITDATE < l.L_RECEIPTDATE 
			AND ord.O_ORDERDATE >= date('1995-07-01')
			AND ord.O_ORDERDATE < date('1995-10-01')
		WITH distinct ord._id AS ord_id, ord.O_ORDERPRIORITY AS ord_pr
		RETURN
			ord_pr,
			COUNT(*) AS ORDER_COUNT
        	ORDER BY
           	ord_pr;