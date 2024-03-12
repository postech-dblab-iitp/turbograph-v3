MATCH (lineitem:LINEITEM)-[:COMPOSED_BY]->(part:PART)
WHERE (part.P_BRAND = 'Brand#25'
    AND (part.P_CONTAINER = 'SM CASE' OR part.P_CONTAINER = 'SM BOX' OR part.P_CONTAINER = 'SM PACK' OR part.P_CONTAINER = 'SM PKG')
    AND lineitem.L_QUANTITY >= 8 AND lineitem.L_QUANTITY <= 8 + 10
    AND part.P_SIZE >= 1 AND part.P_SIZE <= 5
    AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
    AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (part.P_BRAND = 'Brand#51'
    AND (part.P_CONTAINER = 'MED BAG' OR part.P_CONTAINER = 'MED BOX' OR part.P_CONTAINER = 'MED PACK' OR part.P_CONTAINER = 'MED PKG')
    AND lineitem.L_QUANTITY >= 19 AND lineitem.L_QUANTITY <= 19 + 10
    AND part.P_SIZE >= 1 AND part.P_SIZE <= 10
    AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
    AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (part.P_BRAND = 'Brand#51'
    AND (part.P_CONTAINER = 'LG CASE' OR part.P_CONTAINER = 'LG BOX' OR part.P_CONTAINER = 'LG PACK' OR part.P_CONTAINER = 'LG PKG')
    AND lineitem.L_QUANTITY >= 29 AND lineitem.L_QUANTITY <= 29 + 10
    AND part.P_SIZE >= 1 AND part.P_SIZE <= 15
    AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
    AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
RETURN
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS revenue;