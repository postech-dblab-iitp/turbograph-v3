MATCH (c1:CUSTOMER)
WHERE c1.C_ACCTBAL > 0.00
    AND (substring(c1.C_PHONE,1,2) = '12'
        OR substring(c1.C_PHONE,1,2) = '32'
        OR substring(c1.C_PHONE,1,2) = '16'
        OR substring(c1.C_PHONE,1,2) = '29'
        OR substring(c1.C_PHONE,1,2) = '11'
        OR substring(c1.C_PHONE,1,2) = '14'
        OR substring(c1.C_PHONE,1,2) = '27')
WITH avg(c1.C_ACCTBAL) as avg_bal
MATCH (c2:CUSTOMER)
WHERE NOT EXISTS {
        MATCH (o:ORDERS)-[:MADE_BY]->(c2)
    }
    AND (substring(c2.C_PHONE,1,2) = '12'
        OR substring(c2.C_PHONE,1,2) = '32'
        OR substring(c2.C_PHONE,1,2) = '16'
        OR substring(c2.C_PHONE,1,2) = '29'
        OR substring(c2.C_PHONE,1,2) = '11'
        OR substring(c2.C_PHONE,1,2) = '14'
        OR substring(c2.C_PHONE,1,2) = '27')
    AND c2.C_ACCTBAL > avg_bal
WITH c2.C_ACCTBAL AS acctbal, substring(c2.C_PHONE,1,2) AS cntrycode
RETURN
    cntrycode,
    COUNT(*) as numcust,
    sum(acctbal) as totacctbal
ORDER BY cntrycode;