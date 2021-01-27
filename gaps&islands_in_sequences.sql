/*
    Find three consecutive numbers in a sequence
*/

with t(id) as (
    values (1), (2), (2), (3), (3), (3), (2), (4)
), t_rn as (
    select id, row_number() over() rn from t
), t_grp as (
    select id, rn,
           rn - row_number() over(partition by id order by rn) grp_id
      from t_rn
)
 select id
   from t_grp
  group by id, grp_id
  having count(*) = 3
;
