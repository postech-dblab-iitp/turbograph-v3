MATCH (item:LINEITEM)
		WHERE item.L_SHIPDATE <= date('1998-08-25')
		RETURN
			item.L_RETURNFLAG AS ret_flag,
			item.L_LINESTATUS AS line_stat,
			sum(item.L_QUANTITY) AS sum_qty,
			sum(item.L_EXTENDEDPRICE) AS sum_base_price,
			sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,
			sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge,
			avg(item.L_QUANTITY) AS avg_qty,
			avg(item.L_EXTENDEDPRICE) AS avg_price,
			avg(item.L_DISCOUNT) AS avg_disc,
			COUNT(*) AS count_order
		ORDER BY
			ret_flag,
			line_stat;