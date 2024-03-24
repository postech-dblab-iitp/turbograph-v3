MATCH (c1:CUSTOMER)
WHERE c1.C_ACCTBAL > 0.00
    AND (substring(c1.C_PHONE,1,2) = '34'
        OR substring(c1.C_PHONE,1,2) = '14'
        OR substring(c1.C_PHONE,1,2) = '19'
        OR substring(c1.C_PHONE,1,2) = '26'
        OR substring(c1.C_PHONE,1,2) = '30'
        OR substring(c1.C_PHONE,1,2) = '23'
        OR substring(c1.C_PHONE,1,2) = '31')
WITH avg(c1.C_ACCTBAL) as avg_bal
MATCH (c2:CUSTOMER)
WHERE NOT EXISTS {
        MATCH (o:ORDERS)
        WHERE o.O_CUSTKEY = c2.C_CUSTKEY
    }
    AND (substring(c2.C_PHONE,1,2) = '34'
        OR substring(c2.C_PHONE,1,2) = '14'
        OR substring(c2.C_PHONE,1,2) = '19'
        OR substring(c2.C_PHONE,1,2) = '26'
        OR substring(c2.C_PHONE,1,2) = '30'
        OR substring(c2.C_PHONE,1,2) = '23'
        OR substring(c2.C_PHONE,1,2) = '31')
    AND c2.C_ACCTBAL > avg_bal
WITH c2.C_ACCTBAL AS acctbal, substring(c2.C_PHONE,1,2) AS cntrycode
RETURN
    cntrycode,
    COUNT(*) as numcust,
    sum(acctbal) as totacctbal
ORDER BY cntrycode;