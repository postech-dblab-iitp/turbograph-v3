MATCH (item:LINEITEM)
WHERE date(item.L_SHIPDATE) <= date(date('1998-12-01') - duration('P90D'))
RETURN
	item.L_RETURNFLAG,
	item.L_LINESTATUS,
	sum(item.L_QUANTITY) AS sum_qty,
	sum(item.L_EXTENDEDPRICE) AS sum_base_price,
	sum(item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)) AS sum_disc_price,
	sum(item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)*(1+item.L_TAX)) AS sum_charge,
	avg(item.L_QUANTITY) AS avg_qty,
	avg(item.L_EXTENDEDPRICE) AS avg_price,
	avg(item.L_DISCOUNT) AS avg_disc,
	COUNT(*) AS count_order
ORDER BY item.L_RETURNFLAG, item.L_LINESTATUS
