/*
- 2.4.12 배송 모드 및 주문 우선 순위 쿼리(Q12)
    - 이 쿼리는 더 저렴한 배송 모드를 선택하면 약정 날짜 이후에 고객이 더 많은 부품을 수령하게 되어 중요 우선순위 주문에 부정적인 영향을 미치는지 여부를 결정합니다.
- 2.4.12.1 비즈니스 질문
    - 배송 모드 및 주문 우선 순위 쿼리는 지정된 연도에 고객이 실제로 받은 라인 항목에 대해 배송 모드별로 l_receiptdate가 지정된 두 가지 배송 모드에 대해 l_commitdate를 초과하는 주문에 속하는 라인 항목 수를 계산합니다.
    - l_commitdate 이전에 실제로 배송된 항목만 고려됩니다.
    - 늦은 광고 항목은 우선순위가 URGENT 또는 HIGH인 항목과 URGENT 또는 HIGH가 아닌 우선순위가 있는 항목의 두 그룹으로 분할됩니다.
*/

select
    l_shipmode,
    sum(//Aggregate operation
        case when 
      o_orderpriority ='1-URGENT' //OR operation, one of the two can be satisfied, and the original OR HIGH can be selected
            or o_orderpriority ='2-HIGH'
        then 1
    else 0
        end) as high_line_count,
    sum(
    case when 
        o_orderpriority <> '1-URGENT' //AND operation, both of which are not satisfied. It is not URGENT AND not HIGH
            and o_orderpriority <> '2-HIGH'
        then 1
        else 0
        end) as low_line_count
from
    orders,lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]') 
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '[DATE]' //January 1st of any year from 1993 to 1997
    and l_receiptdate < date '[DATE]' + interval '1' year //Within 1 year
group by //Grouping operation
    l_shipmode
order by //Sort operation
    l_shipmode;
