-- Postgres way with analytics
with item_sales(item_id, price, dt) as (
	values(1, 100, date'2020-03-01'),
	      (1, 300, date'2020-03-04'),
	      (1, 200, date'2020-03-07'),
	      (2, 150, date'2020-03-06')
)
select item_id, price, dt
  from (
    select *, row_number() over (partition by item_id order by dt desc) rd
      from item_sales
  ) rs
where rs.rd = 1;

-- Common sql through self-join
select t1.item_id, t1.price, t1.dt
  from item_sales           t1
       left join item_sales t2
              on t1.item_id = t2.item_id
             and t1.dt < t2.dt
where t2.item_id is null;

-- Oracle way
with item_sales(item_id, price, dt) as (
  select 1, 100, date'2020-03-01' from dual union all
  select 1, 300, date'2020-03-04' from dual union all
  select 1, 200, date'2020-03-07' from dual union all
  select 2, 150, date'2020-03-06' from dual
)
select item_id,
       min(price) keep (dense_rank first order by dt desc) price,
       max(dt) dt
  from item_sales
 group by item_id
;
