MATCH (s:SUPPLIER)<-[:PARTSUPP]-(p:PART)
		WHERE NOT REGEXP_MATCHES(s.S_COMMENT,'.*Customer.*Complaints.*')
			AND p.P_BRAND <> 'Brand#21'
			AND NOT (p.P_TYPE CONTAINS 'PROMO BURNISHED')
			AND ((p.P_SIZE = 1)
				OR (p.P_SIZE = 19)
				OR (p.P_SIZE = 23)
				OR (p.P_SIZE = 50)
				OR (p.P_SIZE = 10)
				OR (p.P_SIZE = 20)
				OR (p.P_SIZE = 34)
				OR (p.P_SIZE = 42))
		RETURN
			p.P_BRAND as P_BRAND,
			p.P_TYPE as P_TYPE,
			p.P_SIZE as P_SIZE,
			count(s.S_SUPPKEY) as supplier_cnt
		ORDER BY
			supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE;