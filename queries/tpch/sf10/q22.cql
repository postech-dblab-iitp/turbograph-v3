MATCH (c1:CUSTOMER)
WHERE c1.C_ACCTBAL > 0.00
    AND (substring(c1.C_PHONE,1,2) = '22'
        OR substring(c1.C_PHONE,1,2) = '34'
        OR substring(c1.C_PHONE,1,2) = '13'
        OR substring(c1.C_PHONE,1,2) = '25'
        OR substring(c1.C_PHONE,1,2) = '29'
        OR substring(c1.C_PHONE,1,2) = '11'
        OR substring(c1.C_PHONE,1,2) = '10')
WITH avg(c1.C_ACCTBAL) as avg_bal
MATCH (c2:CUSTOMER)
WHERE NOT EXISTS {
        MATCH (o:ORDERS)
        WHERE o.O_CUSTKEY = c2.C_CUSTKEY
    }
    AND (substring(c2.C_PHONE,1,2) = '22'
        OR substring(c2.C_PHONE,1,2) = '34'
        OR substring(c2.C_PHONE,1,2) = '13'
        OR substring(c2.C_PHONE,1,2) = '25'
        OR substring(c2.C_PHONE,1,2) = '29'
        OR substring(c2.C_PHONE,1,2) = '11'
        OR substring(c2.C_PHONE,1,2) = '10')
    AND c2.C_ACCTBAL > avg_bal
WITH c2.C_ACCTBAL AS acctbal, substring(c2.C_PHONE,1,2) AS cntrycode
RETURN
    cntrycode,
    COUNT(*) as numcust,
    sum(acctbal) as totacctbal
ORDER BY cntrycode;