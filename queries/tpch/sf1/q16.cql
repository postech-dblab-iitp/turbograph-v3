MATCH (s:SUPPLIER)<-[:PARTSUPP]-(p:PART)
		WHERE NOT REGEXP_MATCHES(s.S_COMMENT,'.*Customer.*Complaints.*')
			AND p.P_BRAND <> 'Brand#51'
			AND NOT (p.P_TYPE CONTAINS 'PROMO PLATED')
			AND ((p.P_SIZE = 11)
				OR (p.P_SIZE = 44)
				OR (p.P_SIZE = 42)
				OR (p.P_SIZE = 8)
				OR (p.P_SIZE = 45)
				OR (p.P_SIZE = 14)
				OR (p.P_SIZE = 40)
				OR (p.P_SIZE = 46))
		RETURN
			p.P_BRAND as P_BRAND,
			p.P_TYPE as P_TYPE,
			p.P_SIZE as P_SIZE,
			count(s.S_SUPPKEY) as supplier_cnt
		ORDER BY
			supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE;