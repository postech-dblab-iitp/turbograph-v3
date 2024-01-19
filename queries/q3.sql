/*
- 2.4.3 배송 우선 순위 쿼리(Q3)
    - 이 쿼리는 값이 가장 높은 10개의 배송되지 않은 주문을 검색합니다.
- 2.4.3.1 비즈니스 질문
    - 배송 우선 순위 쿼리는 주어진 날짜에 배송되지 않은 주문 중 가장 큰 수익을 올린 주문의 배송 우선 순위와 잠재적 수익을 l_extendedprice * (1-l_discount)의 합계로 정의합니다.
    - 주문은 수익의 내림차순으로 나열됩니다.
    - 배송되지 않은 주문이 10개 이상 있는 경우 수익이 가장 큰 주문 10개만 나열됩니다.
*/
select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue, //Potential revenue, aggregation operations
    o_orderdate,
    o_shippriority
from
    customer, orders, lineitem //Three meter connection
where
    c_mktsegment = '[SEGMENT]' //Randomly selected within the range specified by TPC-H standard
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '[DATE]' //Specify the date period and select it randomly in [1995-03-01, 1995-03-31]
    and l_shipdate > date '[DATE]'
group by //Grouping operation
    l_orderkey, //Order ID
    o_orderdate, //Order date
    o_shippriority //Transportation priority
order by //Sort operation
    revenue desc, //In descending order, the potential maximum income is listed first
    o_orderdate;
