/*
- 2.4.1 가격 요약 보고서 쿼리(Q1)
    - 이 쿼리는 청구, 배송 및 반환된 비즈니스 금액을 보고합니다.
- 2.4.1.1 비즈니스 질문
    - 가격 책정 요약 보고서 쿼리는 지정된 날짜에 배송된 모든 라인 항목에 대한 요약 가격 책정 보고서를 제공합니다.
    - 날짜는 데이터베이스에 포함된 가장 큰 배송 날짜로부터 60 - 120일 이내입니다.
    - 쿼리는 확장 가격, 할인 확장 가격, 할인 확장 가격 + 세금, 평균 수량, 평균 확장가격 및 평균 할인에 대한 합계를 나열합니다.
    - 이러한 집계는 RETURNFLAG 및 LINESTATUS별로 그룹화되고 RETURNFLAG 및 LINESTATUS의 오름차순으로 나열됩니다.
    - 각 그룹의 광고 항목 수가 포함됩니다.
*/

select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price, 
  sum(l_extendedprice*(1-l_discount)) as sum_disc_price, 
  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)  //DELTA is a value within 60 ~ 120
group by 
    l_returnflag, l_linestatus 
order by
    l_returnflag, l_linestatus;
