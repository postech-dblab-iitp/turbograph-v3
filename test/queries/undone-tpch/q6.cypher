MATCH (li:LINEITEM)
WHERE date(li.L_SHIPDATE) >= date('1994-01-01')
	AND date(li.L_SHIPDATE) < date('1995-01-01')
 	AND tofloat(li.L_DISCOUNT) > 0.06 - 0.01
 	AND tofloat(li.L_DISCOUNT) < 0.06 + 0.01
 	AND li.L_QUANTITY < 24
RETURN
	sum(li.L_EXTENDEDPRICE * tofloat(li.L_DISCOUNT)) AS revenue;