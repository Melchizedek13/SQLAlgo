/*
    Find three consecutive numbers in a sequence
*/

-- First algo -> https://dbfiddle.uk/Qx8m6KyZ
with
t(val) as (values
   (1), (2), (2), (3), (3), (3), (2), (4)
), t_rn as (
   select row_number() over() rn, val from t
), t_grp as (
   select val, rn,
          rn - row_number() over(partition by val order by rn) grp_id
     from t_rn
)
select val
  from t_grp
 group by val, grp_id
having count(*) = 3
;

-- Second algo -> https://dbfiddle.uk/LJOQ2cq2
with
t(val) as (values
   (1), (2), (2), (3), (3), (3), (2), (4)
),
t_rn as (
   select val, row_number() over() rn from t
),
cons_1 as (
   select *,
          lag(val) over (order by rn) as prev_val
     from t_rn
),
cons_2 as (
   select *,
          case when val != prev_val then 1 else 0 end as expr_1
     from cons_1
),
cons_3 as (
   select *,
          sum(expr_1) over (order by rn) as grp_1
     from cons_2
)
select val
  from cons_3
 group by val, grp_1
having count(1) = 3
;
