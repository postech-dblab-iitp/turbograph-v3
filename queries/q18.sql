/*
- 2.4.18 대량 고객 쿼리(Q18)
    - 대용량 고객 쿼리는 대량 주문을 한 고객을 기준으로 고객의 순위를 지정합니다. 대량 주문은 총 수량이 일정 수준 이상인 주문으로 정의됩니다.
- 2.4.18.1 비즈니스 질문
    - 대용량 고객 쿼리는 대량 주문을 한 적이 있는 상위 100명의 고객 목록을 찾습니다.
    - 쿼리는 고객 이름, 고객 키, 주문 키, 날짜 및 총 가격, 주문 수량을 나열합니다.
*/
select
    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, //essential information
    sum(l_quantity) //Total orders
from
    customer, orders, lineitem
where
    o_orderkey in ( //IN subquery with grouping operation
        select
            l_orderkey
        from
            lineitem
        group by 
            l_orderkey 
    having
            sum(l_quantity) > [QUANTITY] // QUANTITY is any value between 312 and 315
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey 
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate;
